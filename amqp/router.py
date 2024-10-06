import asyncio
from typing import Callable, Dict
from collections import defaultdict
import gzip
import time
import traceback
import uuid
from inspect import isawaitable

import orjson as json
import aio_pika as pk

from pdtlite_abc.msg import G_MSG_MAP, G_RPC_MAP, RpcErrorMsg
from pdtlite_abc.abc import IMsg, IReqMsg, IRspMsg
from pdtlite_abc.typedef import QName, ContentEncoding, ContentType
from pdtlite_abc.abc import IMsgRouter
from pdtlite_abc.error import InternalError
from pdtlite_abc import __version__


class RabbitmqMsgRouter(IMsgRouter):
    # TODO reconnect

    def __init__(self, mq_addr: str, q_name: QName = None, **q_params) -> None:
        """
        Args:
            mq_addr (str): "amqp://user:password@ip:port/"
            q_name (QName, optional): 
            durable (bool):
                Durability (queue survive broker restart)
            exclusive (bool):
                Makes this queue exclusive. Exclusive queues may only be accessed 
                by the current connection, and are deleted when that connection closes.
                Passive declaration of an exclusive queue by other connections are not allowed.
            passive (bool): 
                Do not fail when entity was declared previously but has another params.
                Raises aio_pika.exceptions.ChannelClosed when queue doesn’t exist.
            auto_delete (bool): 
                Delete queue when channel will be closed.
            timeout (bool):
                execution timeout
        """
        # mq connection
        self.conn = None
        # mq对象实例
        self.mq: pk.Channel = None
        # router归属当前router的queue的名称
        self.q_name = q_name or str(uuid.uuid1())
        # router归属当前router的queue, 用于接收消息
        self.q: pk.Queue = None
        self.q_params: dict = q_params
        # mq的地址
        self.mq_addr = mq_addr
        # 订阅的channel的回调函数
        self.cb_map = defaultdict(list)
        # 保存rpc请求期间的的异步Future对象
        self.fu_map: Dict[str, asyncio.Future] = {}
        self.mq_exchange: Dict[str, pk.Exchange] = {}

        if q_name is None:
            self.q_params["auto_delete"] = True

    async def connect(self):
        self.conn = await pk.connect(self.mq_addr)
        self.mq = await self.conn.channel()
        self.q = await self.mq.declare_queue(self.q_name, **self.q_params)
        self.q_name  = self.q.name
        # await self.q.consume(self._on_msg, no_ack=True)

    def dumps(self, msg: IMsg, content_type: ContentType, content_encoding: ContentEncoding = None) -> bytes:
        ret = msg.to_dict()
        ret = {"header": {"msg_type": msg.type_name()}, "body": ret, "version": __version__}
        if content_type == ContentType.JSON:
            ret = json.dumps(ret)
        else:
            raise ValueError()

        if content_encoding == ContentEncoding.GZIP:
            ret = gzip.compress(ret)
        return ret

    def loads(self, msg: bytes, content_type: ContentType, content_encoding: ContentEncoding = None) -> IMsg:
        if content_encoding == ContentEncoding.GZIP:
            msg = gzip.decompress(msg)
        if content_type == ContentType.JSON:
            msg = json.loads(msg)
        else:
            raise ValueError()
        msg_type = msg["header"]["msg_type"]
        try:
            return G_MSG_MAP[msg_type](**msg["body"])
        except KeyError:
            raise ValueError(f"不支持的消息类型: {msg_type}")

    async def call(self, q_name: str, msg: IReqMsg, timeout: float = 5, encoding: str = ContentEncoding.GZIP) -> IRspMsg:
        cor_id = str(uuid.uuid4())
        fu = asyncio.Future()
        self.fu_map[cor_id] = fu
        pk_msg = pk.Message(
            self.dumps(msg, ContentType.JSON, encoding),
            correlation_id=cor_id, reply_to=self.q.name,
            content_type=ContentType.JSON, content_encoding=encoding
        )
        try:
            await self.mq.default_exchange.publish(pk_msg, routing_key=q_name)
            ret = await asyncio.wait_for(fu, timeout=timeout)
            if isinstance(ret, RpcErrorMsg):
                raise InternalError(ret.code, ret.msg)
            return ret
        finally:
            self.fu_map.pop(cor_id, None)

    async def subscribe(self, channel: str, callback: Callable) -> None:
        self.cb_map[channel].append(callback)
        ex = await self.get_exchange(channel)
        await self.q.bind(ex)

    async def publish(self, channel: str, msg: IMsg, encoding: ContentEncoding = None) -> None: 
        ex = await self.get_exchange(channel)
        msg_bytes = self.dumps(msg, ContentType.JSON, encoding)
        pk_msg = pk.Message(msg_bytes, content_type=ContentType.JSON, content_encoding=encoding)
        await ex.publish(pk_msg, routing_key="")
    
    async def get_exchange(self, channel: str):
        ex = self.mq_exchange.get(channel)
        if not ex:
            ex = await self.mq.declare_exchange(channel, type=pk.ExchangeType.FANOUT)
            self.mq_exchange[channel] = ex
        return ex
    
    async def _on_msg(self, pk_msg: pk.IncomingMessage):
        if pk_msg.correlation_id:
            await self._handle_rpc_msg(pk_msg)
        else:
            await self._handle_msg(pk_msg)

    async def _handle_rpc_msg(self, pk_msg: pk.IncomingMessage):
        msg = self.loads(pk_msg.body, content_type=pk_msg.content_type, content_encoding=pk_msg.content_encoding)
        if isinstance(msg, IReqMsg):
            cb = G_RPC_MAP.get(msg.type_name())
            if not cb:
                raise ValueError(f"收到不支持的rpc调用: {msg.type_name()}")
            try:
                rsp = cb(msg)
                if isawaitable(rsp):
                    rsp = await rsp
            except:
                traceback.print_exc()
                return

            assert isinstance(rsp, IRspMsg)
            pk_rsp_msg = pk.Message(
                self.dumps(rsp, content_type=pk_msg.content_type, content_encoding=pk_msg.content_encoding),
                content_type=pk_msg.content_type, content_encoding=pk_msg.content_encoding,
                correlation_id=pk_msg.correlation_id,
            )
            await self.mq.default_exchange.publish(pk_rsp_msg, routing_key=pk_msg.reply_to)
        else:
            fu = self.fu_map.pop(pk_msg.correlation_id)
            if fu:
                fu.set_result(msg)
            else:
                print("warning: 收到未知的response...", msg)

    async def _handle_msg(self, pk_msg: pk.IncomingMessage):
        msg = self.loads(pk_msg.body, pk_msg.content_type, pk_msg.content_encoding)
        for cb in self.cb_map[pk_msg.exchange]:
            try:
                r = cb(msg)
                if isawaitable(r):
                    asyncio.ensure_future(r)
            except:
                traceback.print_exc()

    async def close(self):
        await self.mq.close()
        await self.conn.close()
        
    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.close()
