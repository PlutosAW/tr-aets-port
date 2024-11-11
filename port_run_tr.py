# -*- coding: utf-8 -*-
"""
Created on Thu Apr 15 19:57:23 2021

@author: adamw
"""

import json, os , time, gzip
import asyncio, aio_pika
from contextlib import suppress

from fe.algo import (
    Algo,
    RespPosUpdate,
    RespTargetDone,
    RespUpdateTarget,
    SessionRespHandler,
    ShmClient,
    UpdateTarget,
)
from alphaless.secmaster import SecMaster, SecMasterLoader
from alphaless import Exchange, ProductType, SymbolType, Date
# loader = SecMasterLoader()
# # loader.load_http("https://test-openapi-mcp.timeresearch.biz", Exchange.BINANCE)
# loader.load_http("https://prod-openapi-mcp.timeresearch.biz", Exchange.BINANCE)
# Sec_Master = SecMaster.instance

# fe shared memory directory
Shm_Dir = "/home/tmp/shm"
# Shm_Dir = "/opt/data/ts_01/kungfu/algo_shm"

from amqp.amqp_base import BaseConsumer 
from amqp.amqp_base import BasePublisher
from db_resources.db_client import CVDB
from cap_allocate.cap_handler import CapHandler

# prepare databse
live_db = CVDB()

#############

# load mq 
with open('config_port.json', 'r') as f:
    config = json.load(f)
ACC_MQ = config['ACC_MQINFO']

Table_Cass_Weights, Table_L1_Weights, Table_VO_Weights, Table_AO_Weights = 'Optimal_Weights', 'L1_Weights', 'VO_Weights', 'AO_Weights'
Table_Tasks, Table_Tasks_VO, Table_Tasks_AO = 'Table_Tasks', 'Table_Tasks_VO', 'Table_Tasks_AO'
Table_States, Table_States_VO, Table_States_AO = 'Current_States', 'Current_States_VO', 'Current_States_AO'

# # get cap allo weight
AO_WEIGHTS = live_db.load_data(Table_AO_Weights)
# CASS_WEIGHTS = live_db.load_data(Table_Cass_Weights)
# # components
# CASS_TICKERS = [s[4:] + 'USDT' for s in CASS_WEIGHTS]
# AO_TICKERS = [s[4:] + 'USDT' for s in AO_WEIGHTS]
AO_TICKERS = ['BTCUSDT', 'ETHUSDT', 'DYDXUSDT', 'GMXUSDT',
                'ORDIUSDT', 'PENDLEUSDT', 'ENAUSDT', 'ICPUSDT', 'BNBUSDT']
NOT_TICKERS = ['RNDRUSDT']
# Ticker_Sid = { ticker: Sec_Master.find_sid( exch=Exchange.BINANCE, product_type=ProductType.LINEAR_SWAP, exch_ticker=ticker)
#                 for ticker in AO_TICKERS}
# print (Ticker_Sid)
# Ticker_Sid = {'BTCUSDT': 16410, 'ETHUSDT': 16411, 'DYDXUSDT': 16539, 'GMXUSDT': 101422, 'ORDIUSDT': 217108, 'PENDLEUSDT': 178812, 'ENAUSDT': 308748, 'ICPUSDT': 16520, 'BNBUSDT': 16425}
Ticker_Sid = live_db.load_data('Ticker_Sid')

## get mq instance
MQ = {
      'MQZK': {'pub': BasePublisher,
               'con': BaseConsumer} ,
      }
BCON = MQ['MQZK']['con'](QUEUE = 'ao03_signal', durable=True)

## load ams token and initialize caphandler
AMS_ACC = {'tr_aw': {'platform': 'tr_future',
                    'account': 'TR_AW',
                     'token_id': '', 
                     'base_curr': 'USDT'},            
            }
# Handler = CapHandler(AMS_ACC, Table_Tasks, table_states = Table_States)
Handler = CapHandler(AMS_ACC, Table_Tasks_AO, table_states = Table_States_AO)

## portfolio weights
PORT_WEIGHTS = { 'tr_aw': {'Sect_AO': 2 ,
                        'Cash': 0.3   },
               }

#############
class TR_Handler(SessionRespHandler):

    def on_resp_pos_update(self, resp: RespPosUpdate):
        print("on_resp_pos_update", resp.ts, resp.acct_id, resp.sid, resp.pos)

    def on_resp_update_target(self, resp: RespUpdateTarget):
        # response to update_target, ec=ErrorCode.NONE means success
        print("on_resp_update_target", resp.ts, resp.update_id, resp.acct_id, resp.sid, resp.ec)

    def on_resp_target_done(self, resp: RespTargetDone):
        # target done
        # pos is the final position
        # canceled=True means the target is canceled

        print(
            "on_resp_target_done",
            resp.ts,
            resp.update_id,
            resp.acct_id,
            resp.sid,
            resp.target_pos,
            resp.pos,
            resp.canceled,
        )
        self.job_complete = True

    def on_session_login(self, session_id: int):
        print("on_session_login", session_id)
        # logged in, can now send requests
        self.job_complete = False


    def on_session_logout(self, session_id: int):
        print("on_session_logout", session_id)
        # logged out, timeout or server is down
        # ShmClient will try to reconnect automatically

    def on_session_not_logged_in_error(self, session_id: int):
        print("on_session_not_logged_in_error", session_id)
        # requests sent when not logged in
        # ShmClient will try to reconnect automatically

## output    
def get_sid(ticker):
    loader = SecMasterLoader()
    # loader.load_http("https://test-openapi-mcp.timeresearch.biz", Exchange.BINANCE)
    loader.load_http("https://prod-openapi-mcp.timeresearch.biz", Exchange.BINANCE)
    Sec_Master = SecMaster.instance
    sid = Sec_Master.find_sid( exch=Exchange.BINANCE, product_type=ProductType.LINEAR_SWAP, exch_ticker=ticker)
    return sid

async def pub_pstn_tr(positions):
    # process target positions
    print ("target positons: ", positions)

    session_id = 1
    shm_client = ShmClient()
    tr_handler = TR_Handler()
    shm_client.initialize(Shm_Dir, session_id, tr_handler)
    
    # Wait for login before sending any requests
    while not shm_client.logged_in:
        shm_client.poll()
        time.sleep(0.1)
    
    # subscribe to pos updates via on_resp_pos_update, will receive current pos snapshot first
    # shm_client.sub_pos_update(3320, 16410)
    # # to unsubscribe, call shm_client.unsub_pos_update(385, 16410)
    
    target = UpdateTarget()
    target.update_id = int(time.time())
    target.acct_id = 3320  # 486942321 385

    prc_limit, time_limit = 0.05, 9 * 60 
    for ticker in positions:
        # get sid and positions
        sid =  Ticker_Sid[ticker]     # get_sid(ticker)
        print (ticker, ' sid: ',  sid)
        target.sid = sid
        # BTCUSDT 16410 , 14044 is USDT
        target.pos = positions[ticker]['quote_size']
        target.algo = Algo.TWAP
        # execution start and end time, hard coded 9 minutes
        target.start_ts = int(time.time() * 1e9)
        target.end_ts_soft = int(target.start_ts + time_limit * 1e9)
        # Optional, if this is set, continue sending orders to get more fill after
        # soft end time if not fully filled yet.
        target.end_ts_hard = target.end_ts_soft
        
        # Set the ratio of qty sent as maker orders. Note that this affects the ratio of orders sent at
        # scheduling points and does not guarantee that, say, 80% of the volume will be maker volume.
        # E.g. if we sent one maker order of 8 qty and one taker order of 2 qty, and the taker order
        # was filled while the maker order was not, at the next update time the unfilled 8 qty would be
        # distributed according to the ratio again. The reason for this is to improve fill rate as maker
        # orders are harder to fill.
        # OPTIONAL. Default is 0 (taker only).
        target.maker_ratio = 0.8
        # maker order start / end time, outside of this time range only taker orders will be sent, optional
        target.maker_start_ts = target.start_ts
        target.maker_end_ts = target.end_ts_hard
        # # taker order start / end time, optional
        # target.taker_start_ts = target.start_ts
        # target.taker_end_ts = target.end_ts_hard
        
        # trading volume / participation rate limit. E.g. Not to trade more than 5% of the total
        # market volume.
        # OPTIONAL. Default (0) is no limit.
        target.volume_limit = 0.2
        # Max price to buy or min price to sell. Default value (0) is nil.
        prc_limit = prc_limit if positions[ticker]['side'] == 'Buy' else (- prc_limit)
        target.limit_price = positions[ticker]['arrival_prc'] * (1 + prc_limit)
        # always use limit price for orders.
        target.strict_limit = False
        # taker: always use limit price.
        # maker: use limit price if it doesn't cross the opposite side.
        target.prefer_limit = False
        
        # use update_target to send new target
        shm_client.update_target(target)
        for i in range(5):
            shm_client.poll()
            time.sleep(0.1)

    # while not tr_handler.job_complete:
    #     shm_client.poll()
    #     time.sleep(1)
    # enum class FE_API ErrorCode : int32_t {
    #     NONE = 0,
    #     INTERNAL_ERROR = 1,
    #     INVALID_START_TS = 3,
    #     INVALID_END_TS = 4,
    #     INVALID_SIDE = 5,
    #     INVALID_QTY = 6,
    #     INVALID_LIMIT_PRICE = 7,
    #     INVALID_ACCOUNT = 8,
    #     INVALID_SYMBOL = 9,
    #     INVALID_ALGO = 10,
    #     INVALID_END_TS_SOFT = 11,
    #     INVALID_END_TS_HARD = 12,
    #     EXPIRED_START_TS = 13,
    #     EXPIRED_END_TS_HARD = 14,
    #     INVALID_MAKER_RATIO = 15,
    #     INVALID_MAKER_START_TS = 16,
    #     INVALID_MAKER_END_TS = 17,
    #     INVALID_TAKER_START_TS = 18,
    #     INVALID_TAKER_END_TS = 19,
    #     INVALID_VOLUME_LIMIT = 20,
    #     DUPLICATE_ORDER_ID = 21,
    #     INVALID_ORDER_ID = 22,
    #     ORDER_CLOSED = 23,
    #     EXPIRED_END_TS = 24,
    #     TOO_MANY_ORDERS = 25,
    #     ORDER_STALE = 26,
    #     TIMEOUT = 100,
    #     TRADING_PAUSED = 101,
    #     };


async def update_db(orders, table=Table_Tasks_AO):
    # add acc name in the key
    # jobs = {order['q'][1] + '_' + order['d']['data']['request_id'] + '_' + order['d']['data']['order_action']: 
    #         order  for order in orders}     # distinguish create and cancel
    jobs = { order['d']['data']['request_id'] + '_' + order['d']['data']['order_action']: 
            order  for order in orders}     # distinguish create and cancel
        
    count = 0
    try:
        live_db.push_data( jobs, table = table)
    except Exception as e:
        print (e)
        if count < 1:
            live_db.update_data( jobs, table = table)
        count += 1
        pass

    return

async def pub_order_zk(orders_zk):            
    # initialize trader
    for order in orders_zk:
        platform, subaccount = order['q']
        ## before port use perp, after port use future
        psub = subaccount if platform in subaccount else platform + '_' + subaccount
        TPUB = BasePublisher(
                     EXCHANGE_TYPE = 'direct',
                     EXCHANGE = ACC_MQ[psub]['exchange'], 
                     QUEUES = [ACC_MQ[psub]['queue']],
                     ROUTING_KEY = ACC_MQ[psub]['routing_key'],
                 )  # exec_orders
        trader, conn_p = await TPUB.setProducer(loop = None)   
    
        msg_out = gzip.compress(json.dumps(order ).encode())
        msg_out = aio_pika.Message(  body= msg_out)           
        await trader.publish( msg_out, routing_key = ACC_MQ[psub]['routing_key'] )
        await conn_p.close()

    return  orders_zk

## cal                             
async def strat_cass(ticker, strat_signal, accs = ['aq_aw']):
    # bals, ptns = await Handler.get_port_info_h()
    # cap_alloc, alloc_current = await Handler.get_alloc(bals, ptns )
    # bals_acc, ptns_acc = await Handler.get_port_info_h_acc()
    bals_acc, ptns_acc = await Handler.get_port_info_h_db()
    for acc in accs:
        if 'Sect_Cass' in PORT_WEIGHTS[acc]:
            acc_weights = {k: w * PORT_WEIGHTS[acc]['Sect_Cass'] for k, w in CASS_WEIGHTS.items() }
            cap_alloc, alloc_current = await Handler.get_alloc(bals_acc[acc], ptns_acc[acc], weights = acc_weights )
            orders_all = await Handler.gen_orders(strat_signal, cap_alloc, alloc_current, 
                                            bals_acc[acc], ptns_acc[acc], acc, 
                                            table_states = Table_States )
            orders_zk = orders_all[1]
            if len(orders_zk) > 0:
                # receipt = await pub_order(orders)
                # print (f"signal {ticker} is sent to xxx: ", receipt)  
                receipt1 = await pub_order_zk(orders_zk)
                print (f"signal {ticker} is sent to zk: ", receipt1)               
                await update_db(orders_zk)                                   
    return
        
async def strat_l1(ticker, strat_signal, accs = ['dv_aw']):
    bals_acc, ptns_acc = await Handler.get_port_info_h_db()
    for acc in accs:
        if 'Sect_L1' in PORT_WEIGHTS[acc]:
            acc_weights = {k: w * PORT_WEIGHTS[acc]['Sect_L1'] for k, w in L1_WEIGHTS.items() }
            cap_alloc, alloc_current = await Handler.get_alloc(bals_acc[acc], ptns_acc[acc], weights = acc_weights )
            orders_all = await Handler.gen_orders(strat_signal, cap_alloc, alloc_current, 
                                            bals_acc[acc], ptns_acc[acc], acc,
                                            table_states = Table_States )
            orders_zk = orders_all[1]
            if len(orders_zk) > 0:
                # receipt = await pub_order(orders)
                # print (f"signal {ticker} is sent to xxx: ", receipt)  
                receipt1 = await pub_order_zk(orders_zk)
                print (f"signal {ticker} is sent to zk: ", receipt1)               
                await update_db(orders_zk)                                   
    return
        
async def strat_ao(ticker, strat_signal, accs = ['aq_aw']):
    bals_acc, ptns_acc = await Handler.get_port_info_h_db()
    for acc in accs:
        if 'Sect_AO' in PORT_WEIGHTS[acc]:
            acc_weights = {k: w * PORT_WEIGHTS[acc]['Sect_AO'] for k, w in AO_WEIGHTS.items() }
            cap_alloc, alloc_current = await Handler.get_alloc(bals_acc[acc], ptns_acc[acc], weights = acc_weights )
            orders_all = await Handler.gen_orders(strat_signal, cap_alloc, alloc_current, 
                                                bals_acc[acc], ptns_acc[acc], acc,
                                                table_states = Table_States_AO)
            orders_zk = orders_all[1]
            if len(orders_zk) > 0:
                receipt1 = await pub_order_zk(orders_zk)
                print (f"signal {ticker} is sent to zk: ", receipt1)               
                await update_db(orders_zk)                                   
    return

async def strat_ao_p(ticker, strat_signal, accs = ['aq_aw']):
    bals_acc, ptns_acc = await Handler.get_port_info_h_db()
    for acc in accs:
        if 'Sect_AO' in PORT_WEIGHTS[acc]:
            acc_weights = {k: w * PORT_WEIGHTS[acc]['Sect_AO'] for k, w in AO_WEIGHTS.items() }
            cap_alloc, alloc_current = await Handler.get_alloc(bals_acc[acc], ptns_acc[acc], weights = acc_weights )
            # keep in records
            orders_all = await Handler.gen_orders(strat_signal, cap_alloc, alloc_current, 
                                                bals_acc[acc], ptns_acc[acc], acc,
                                                table_states = Table_States_AO)
            orders_zk = orders_all[1]
            if len(orders_zk) > 0:
                # receipt1 = await pub_order_zk(orders_zk)
                # print (f"signal {ticker} is sent to zk: ", receipt1)               
                await update_db(orders_zk)    

            # place target position orders
            prc = strat_signal[ticker][0][-1][-1][-1]
            s = strat_signal[ticker][0][0]
            a = s.split('Sys9')[1]
            st = float(strat_signal[ticker][0][1].split(":")[0])
            target_position = cap_alloc[s] * st / prc
            positions = {ticker: {
                'quote_size': target_position,
                'arrival_prc': prc,
                'st': st,
                'side': 'Buy' if alloc_current.get(a, 0.0) < target_position else 'Sell',
                } }
            await pub_pstn_tr(positions)

    return

######    
### loop jobs
async def cass_job( message):
    '''
    Parameters
    ----------
    message: strat_signal
    {"ORDIUSDT": [["Sys9ORDI", "-0.06206669: Increase Short", [-0.06206669, 1730757906, "2024-11-04T22:05:06+00:00", [1774036.9, 0.84064069, -1.06177673, -0.84912406, 0.34139394, -9.91, 29.66]]]]}
    '''
    # get accounts
    accs = [k for k in AMS_ACC]
    
    async with message.process(requeue = True):
        # print (message.body.decode() )
        # msg_in = gzip.decompress(message.body).decode()
        msg_in = message.body.decode()
        strat_signal = json.loads(msg_in )       
        print ("received message", msg_in)
        for ticker in  strat_signal:
            model = strat_signal[ticker][0][0][:4]
            print (ticker)
            # if ticker in CASS_TICKERS and model == 'Sys6' and ticker not in NOT_TICKERS:
            #     # submit new orders
            #     await strat_cass(ticker, strat_signal, accs = accs)
            # if ticker in L1_TICKERS and model == 'Sys6'  and ticker not in NOT_TICKERS:
            #     # submit new orders
            #     await strat_l1(ticker, strat_signal, accs = accs)
            # if ticker in VO_TICKERS and model == 'Sys8' and ticker not in NOT_TICKERS:        
            #     await strat_vo(ticker, strat_signal, accs = accs)   
            if ticker in AO_TICKERS and model == 'Sys9' and ticker not in NOT_TICKERS:        
                await strat_ao_p(ticker, strat_signal, accs = accs)   
        return

def _check_connection(loop, conn_c):
    if conn_c.is_closed:
        loop.run_until_complete(runJobs(
            loop ) )

async def runJobs(loop):
    # take global variable cross_exs and handler
    consumer, conn_c = await BCON.setConsumer(loop)
    
    print ('ampq are loaded')        
    # cback = functools.partial(process_message, consumer )                 
    await consumer.consume(cass_job)
    # check connection
    loop.call_later(60, _check_connection, loop, conn_c)

async def kill_tasks():
    pending = asyncio.all_tasks()
    for task in pending:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task 
            

########################################  

    

################################################
if __name__ == "__main__":
    
    try: 
        loop = asyncio.get_event_loop()
        loop.run_until_complete(runJobs(
            loop ) )
        loop.run_forever()
    except KeyboardInterrupt:
        loop.close()
        # asyncio.run(kill_tasks())
        print ('user canceled jobs')
        
    # start = time.time()  
    # asyncio.run(process_message(Handler))    
    # print ('process one signal takes: ', time.time() - start)
    
