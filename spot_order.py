import time

from alphaless import Side
from alphaless.ts import PositionMode, PositionOffset

from fe.algo import (
    Algo,
    AlgoUrgency,
    InsertOrder,
    QryOrders,
    RespCancelOrder,
    RespOrderUpdate,
    RespQryOrders,
    SessionRespHandler,
    ShmClient,
)

# Shm_Dir = "/opt/data/ts_01/kungfu/algo_shm"
Shm_Dir = "/opt/data/ts_01/kungfu/order_algo_shm"

class Handler(SessionRespHandler):
    def __init__(self, acct_id, sid):
        SessionRespHandler.__init__(self)
        self.acct_id = acct_id
        self.sid = sid
        self.pos = 0.0
        self.order_coid = None
        self.stopped = False

    def on_resp_cancel_order(self, resp: RespCancelOrder):
        print(f"on_resp_cancel_order coid:{resp.coid}, ec:{resp.ec}")

    def on_resp_order_update(self, resp: RespOrderUpdate):
        print(
            f"on_resp_order_update e:{resp.event}, coid:{resp.coid}, "
            f"order_status:{resp.order_status}, side:{resp.side}, qty:{resp.qty}, "
            f"remain_qty:{resp.remain_qty}, cum_exec_qty:{resp.cum_exec_qty}, "
            f"avg_exec_price:{resp.avg_exec_price}, exec_qty:{resp.exec_qty}, "
            f"exec_price:{resp.exec_price}, fee:{resp.fee}, fee_sid:{resp.fee_sid}, "
            f"lqd_type:{resp.lqd_type}, ts:{resp.ts}, ec:{resp.ec}"
        )
        if resp.exec_qty > 0:
            if resp.side == Side.BUY:
                self.pos += resp.exec_qty
            else:
                self.pos -= resp.exec_qty

        # order is closed
        if resp.remain_qty == 0 and resp.coid == self.order_coid:
            self.order_coid = None
            # self.send_order()

    def on_resp_qry_positions(self, resp):
        # uint32_t logic_acct_id;
        # uint32_t sid;
        # PositionSide position_side;
        # MarginMode margin_mode;
        # double pos_size;
        # double avg_entry_price;
        # double leverage;
        # double liq_price;
        # double unrealised_pnl;
        # bool is_last;
        # uint16_t err_code;
        # double cum_realised_pnl;
        # double position_mm;
        if resp.sid == self.sid and not resp.is_last and resp.err_code == 0:
            print(f"on_resp_qry_positions sid:{resp.sid}, pos:{resp.pos_size}")
            self.pos = resp.pos_size
            # self.send_order()
        elif resp.is_last and self.pos is None:
            print("on_resp_qry_positions no position")
            self.pos = 0
            # self.send_order()

    def on_resp_qry_balances(self, resp):
        # uint32_t logic_acct_id;
        # uint32_t sid;   // optional
        # char coin[16];  // ETH/BTC/USDT
        # double frozen;
        # double available;  // available = total-frozen
        # double total;
        # bool is_last;
        # double unrealised_pnl;
        # double realised_pnl;
        # uint16_t err_code;
        # double borrowed;
        # double interest;
        pass

    def on_resp_qry_orders(self, resp: RespQryOrders):
        # Nanoseconds ts;
        # int32_t acct_id;
        # int32_t sid;
        # int64_t req_id;
        # ErrorCode ec;
        # int16_t num_pages;
        # int16_t page_idx;
        # int16_t num_orders;
        print(
            f"on_resp_qry_orders req_id:{resp.req_id}, ec:{resp.ec}, num_pages:{resp.num_pages}, "
            f"page_idx:{resp.page_idx}, num_orders:{resp.num_orders}"
        )
        for i in range(resp.num_orders):
            order = resp.order(i)
            print(
                f"order {i}: coid:{order.coid}, sid:{order.sid}, side:{order.side}, "
                f"qty:{order.qty}, algo:{order.algo} remain_qty:{order.remain_qty}, "
                f"start_ts:{order.start_ts}, end_ts:{order.end_ts}, urgency:{order.urgency}, "
                f"cum_exec_qty:{order.cum_exec_qty}, avg_exec_price:{order.avg_exec_price}, "
                f"insert_ts:{order.insert_ts}"
            )

    # position update push should be enabled in trade server
    def on_position_update(self, resp):
        # uint32_t logic_acct_id;
        # uint32_t sid;
        # PositionSide position_side;
        # MarginMode margin_mode;
        # double pos_size;
        # double avg_entry_price;
        # double leverage;
        # double liq_price;
        # double unrealised_pnl;
        # double cum_realised_pnl;
        # double position_mm;
        # int64_t exch_update_time_ns;
        # double mark_price;
        # double available_pos_size;
        pass

    # balance update push should be enabled in trade server
    def on_balance_update(self, resp):
        # uint32_t logic_acct_id;
        # uint32_t sid;
        # char coin[16];  // ETH/BTC/USDT
        # int64_t exch_update_time_ns;
        # double frozen;
        # double available;  // available = total-frozen
        # double total;
        # double unrealised_pnl;
        # double realised_pnl;
        # uint16_t err_code;
        # double borrowed;
        # double interest;
        pass

    def on_session_login(self, session_id: int):
        print("on_session_login", session_id)
        # logged in, can now send requests

        # # cancel all open orders, sid is optional
        # self.shm_client.cancel_all(self.acct_id, self.sid)

        # # qry positions, sid is optional
        # self.shm_client.qry_positions(self.acct_id, self.sid)

        # for spot, use qry_balances
        # self.shm_client.qry_balances(self.acct_id, self.sid)

        # query orders
        ts = time.time()
        req = QryOrders()
        req.acct_id = self.acct_id
        req.sid = self.sid  # optional
        req.req_id = int(ts)
        req.start_ts = int((ts - 300) * 1e9)
        req.end_ts = int(ts * 1e9)
        req.include_closed = True
        # self.shm_client.qry_orders(req)

    def on_session_logout(self, session_id: int):
        print("on_session_logout", session_id)
        # logged out, timeout or server is down
        # ShmClient will try to reconnect automatically

    def on_session_not_logged_in_error(self, session_id: int):
        print("on_session_not_logged_in_error", session_id)
        # requests sent when not logged in
        # ShmClient will try to reconnect automatically


# TODO: change these to your own values
session_id = 1
acct_id = 3321 # 3321 spot, 3320 perp
sid = 14511

##
def send_order(size, period=60):
    order = InsertOrder()
    # unique order id across all accounts
    order.coid = int(time.time() * 1e6)
    order.sid = sid
    order.acct_id = acct_id
    order.algo = Algo.TWAP
    order.qty = size
    order.pos_mode = PositionMode.NET
    order.pos_offset = PositionOffset.OPEN
    if order.qty < 0:
        order.side = Side.SELL
    else:
        order.side = Side.BUY
    order.limit_price = 0

    # in nanoseconds
    order.start_ts = int(time.time() * 1e9)
    order.end_ts = order.start_ts + int(period * 1e9)

    # urgency to follow the schedule.
    # low: may fall more behind the schedule but higher maker ratio.
    # mid: balance between maker ratio and schedule.
    # high: stay closer to the schedule but higher taker ratio.
    order.urgency = AlgoUrgency.LOW

    # trading volume / participation rate limit. E.g. Not to trade more than 5% of the total
    # market volume. 0 means no limit.
    order.volume_limit = 0

    # minimum maker ratio (maker fill qty / total fill qty) to achieve. 0 means no limit.
    order.min_maker_ratio = 0

    shm_client.insert_order(order)
    print(f"sent order {order.coid}, {order.side}, {order.qty}")

def stop(self):
    self.stopped = True
    self.cancel_order()

def cancel_order(self):
    if self.order_coid:
        self.shm_client.cancel_order(self.acct_id, self.order_coid)



shm_client = ShmClient()
handler = Handler(acct_id, sid)
shm_client.initialize(Shm_Dir, session_id, handler)

while not shm_client.logged_in:
    shm_client.poll()
    time.sleep(0.1)

send_order(2.0)
# use polling to recevie responses and maintain heartbeat to keep connection alive
try:
    while True:
        shm_client.poll()
        time.sleep(0.5)
except KeyboardInterrupt:
    print("canceling order...")
    handler.stop()
    while True:
        shm_client.poll()
        time.sleep(0.1)
        if handler.order_coid is None:
            break
