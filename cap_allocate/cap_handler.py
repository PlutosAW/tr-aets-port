#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 22 12:20:21 2022
input: cap_alloc, acc bals ptns, strat_signal
output: cap_alloc = {} ,    orders [{}] to xts or ex

@author: zk
"""
import json, time, os
import numpy as np
import datetime as dt
import requests

from db_resources.db_client import CVDB

live_db = CVDB()
Table_Assets = 'Assets_Current'		
Table_Positions = 'Positions_Current'

############
# load config 
with open('config_port.json', 'r') as f:
    config = json.load(f)
EX_TK = config['EX_TICKER']
TRADE_PARAMS = config['TRADE_PARAMS']

PRIO_SEQ = ['existing', 'USDT', 'USD']
MIN_NOTIONAL, LEV_MAX = 200, 4.0

###############################
class CapHandler():
    def __init__(self, AMS_ACC, Table_Tasks, acc_types= ['spot', 'perp'],
                 table_states = 'Current_States'):
        self.AMS_ACC = AMS_ACC
        self.Table_Tasks = Table_Tasks
        self.acc_types = acc_types
        self.Table_States = table_states
        self.binance_url_v3 =  'https://api.binance.com/api/v3'
        
    ## get current port info
    async def _add_dicts(self, port_old, port_new):
       for a in port_new:
            b1 = port_old.get(a, {'total': 0.0 , 'notional': 0.0})
            b2 = port_new[a]
            x = {k: sum(filter(None, [b2[k],  b1[k]])) for k in b2 }
            port_old.update({a: x})
       return port_old

    async def get_port_info_h_db(self):
        # get hourly port info ; 
        assets = live_db.load_data(Table_Assets, item_list = list(self.AMS_ACC.keys()) )
        positions = live_db.load_data(Table_Positions, item_list = list(self.AMS_ACC.keys()) )
        bals, ptns = {k: {} for k in self.AMS_ACC}, {k: {} for k in self.AMS_ACC}
        
        # select accs only in self.AMS_ACC
        for typ in self.acc_types:
            if 'spot' in typ:
                for acc in assets:
                    bal = {item['asset']: {'total': item['quantity'],
                                           'notional': item['usdValue']} for item in assets[acc]['spot']}
                    bals[acc][typ] = bal
            elif 'perp' in typ:
                for acc in assets:
                    bal = {item['asset']: {'total': item['quantity'],
                                           'notional': item['usdValue']} for item in assets[acc]['perp']}
                    bals[acc][typ] = bal
                for acc in positions:
                    pos = {item['position']: {'total': float(item['quantity']),
                                           'notional': float(item['usdValue'])} for item in positions[acc]}
                    ptns[acc][typ] = pos

        return bals, ptns
    
    ## get ongoing orders
    async def _get_ongoing_orders(self, ticker, model):
        tasks = live_db.load_data(self.Table_Tasks)
        ongoing_orders = {}
        if len(tasks) > 0 :
            # select right orders only for current book accounts
            accs = [ self.AMS_ACC[k]['account'] for k in self.AMS_ACC]
            zk_orders = {k: v for k, v in tasks.items() if( (v['n'] == 'EVENT_EXEC') 
                                        and (v['d']['data'].get('subaccount') in self.AMS_ACC) )}
            if len(zk_orders) > 0:
                ongoing_orders['EVENT_EXEC'] = {k: v for k, v in zk_orders.items() if (v['d']['data'].get('time_limit', '2000-01-01') 
                                                                                       > dt.datetime.utcnow().isoformat()
                                                    and v['d']['data']['symbol'].split('USD')[0].split('_')[0] in ticker
                                                    and v['d']['data'].get('strategy_id') == model  
                                                    and v['d']['job'] == "create_task")                                                   
                                                }
        return ongoing_orders


    ## prepare symbol
    async def _ticker_to_symbol(self, ticker, market_type, quote_curr, platform):
        if platform == 'ftx':
            symbol = ticker + '-PERP'
        else:
            symbol = ticker[:-4] + quote_curr.upper()
        return symbol
        
    async def _system_to_asset(self, system):
        a = system[4:].upper()
        return a
    
    async def _a_to_ptn(self, a, acc):
        if self.AMS_ACC[acc]['platform'] in ['dv', 'cy']:
            p_cross = a + self.AMS_ACC[acc]['base_curr']
        else:
            p_cross = a + '/USD'
        return p_cross
    
    async def _system_to_st(self, system, ticker):
        if 'Sys6' in system:
            st = 'VM' + ticker
        elif 'Sys7' in system:
            st = 'VH' + ticker
        elif 'Sys8' in system:
            st = 'VO' + ticker
        else:
            print ("unknown system")
            st = None
        return st
    async def _st_to_system(self, st_id):
        if st_id[:2] == 'VM':
            system = 'Sys6' + st_id[2:-4] if 'USDT' in st_id else 'Sys6' + st_id[2:-3]
        elif st_id[:2] == 'VH':
            system = 'Sys7' + st_id[2:-4] if 'USDT' in st_id else 'Sys7' + st_id[2:-3]
        elif st_id[:2] == 'VO':
            system = 'Sys8' + st_id[2:-4] if 'USDT' in st_id else 'Sys8' + st_id[2:-3]
        else:
            print ("unknown st_id")
            system = None
        return system
    
    async def _cross_to_asset(self, cross):
        if '_' in cross:
            a = cross.split('_')[0].upper()
        else:
            a = cross.split('USD')[0].upper()
        return a
    
    ## get ex
    async def _get_ex(self, ticker):
        ex_lst = []
        for ex in EX_TK:
            if ticker in EX_TK[ex]:
                ex_lst.append(ex)
        return ex_lst
    
## get prices
    def get_histrates(self, cross, granularity = '1m', 
                      start=None, end=None, limit=100):  
        url = self.binance_url_v3 + '/klines'
        # datestr to unix timestamp
        if start is not None:
            start_ms = int(dt.datetime.fromisoformat(start).timestamp() * 1000)
        else:
            start_ms = start
        if end is not None:
            end_ms = int(dt.datetime.fromisoformat(end).timestamp() * 1000)
        else:
            end_ms = end
        
        params = {'symbol': cross,
                  'interval': granularity,
                  'startTime': start_ms,
                  'endTime': end_ms,
                  'limit': limit,
            }
        data = requests.get(url, params).json()
        # put most recent data on top
        data = data[::-1]
        return data

    async def _get_price(self, cross):
        # states  = live_db.load_data( self.Table_States )    
        # state = [ states[item] for item in states if cross in item][0]
        # prc = state[3][-1] 
        data = self.get_histrates(cross, granularity = '1m', limit=2)[0]
        prc = float(data[4])
        return prc

    async def _get_price_btc(self):
        data = self.get_histrates('BTCUSDT', granularity = '1m', limit=2)[0]
        prc_btc = float(data[4])
        return prc_btc
        
    async def _get_price_usd(self, prc_ticker_btc):
        data = self.get_histrates('BTCUSDT', granularity = '1m', limit=2)[0]
        prc_btc = float(data[4])
        prc_ticker_usd = prc_ticker_btc * prc_btc
        return prc_ticker_usd
 

    ##################
    # port logic: 
    #     1. one subacc only support mutually exclusive strategy tickers;
    #     2. current strat states needs to be used if one subacc trade the same tickers in different strats;
    ### cal cap alloc 
    async def get_alloc(self, bals, ptns, weights):
        # bals['binance_future'] = {'eth': {'total': 5.7e-05, 'notional': 0.09969756},}
        # count balance only as ptns contribution is taken by unrealizedpnl
        total_bal = sum(filter(None,  [ item[k]['notional'] for typ, item in bals.items() for k in item ]  ))
        cap_alloc = { k: total_bal * v for k, v in weights.items() }
        
        # current alloc; exclude the assets as the collateral in perp
        alloc_current = {}
        for typ, item in bals.items():
            if 'perp' not in typ:
                for k in item:
                    if k in alloc_current:
                        alloc_current[k] += item[k]['notional']
                    elif item[k]['notional']: 
                        alloc_current[k] = item[k]['notional']  
        for typ, item in ptns.items():
            for k in item:
                a = await self._cross_to_asset(k)            
                if a in alloc_current:
                    alloc_current[a] += item[k]['notional']
                elif item[k]['notional']: 
                    alloc_current[a] = item[k]['notional']  
                    
        return  cap_alloc,   alloc_current 
            
        # for acc in Acc_Types:       
        #     if 'spot' in acc:
        #         acc_bal = sum(filter(None, [bals[acc][k]['notional'] for k in bals[acc]] ) 

    async def _cal_delta(self, ticker, signal, cap_alloc, alloc_current,
	                         table_states = None):
        if not table_states:
            table_states = self.Table_States
        
        s = signal[0][0]
        a = await self._system_to_asset(s)
        
        # st_id = await self._system_to_st(s, ticker)
        # st_states = live_db.load_data(Table_States)
        # # update states
        # st_states[st_id][0] = int(signal[0][1].split(': ')[0])
        cap_a = 0
        # for sn in st_states:
        #     if st_id == sn:
        #     # if a.upper() in sn[:-3]:    
        #         alias = await self._st_to_system(sn)        
        #         cap_a += cap_alloc[alias] * st_states[sn][0]
        #         # print (sn, cap_alloc[alias])
        alias = signal[0][0]
        # revise to handle fraction st
        st =  float(signal[0][1].split(': ')[0])
        cap_a += cap_alloc[alias] * st
        print (st, cap_alloc[alias])
        
        delta_size = cap_a - alloc_current.get(a, 0.0)
        
        return delta_size
    
    async def _get_margin_avai(self, prio_asset, bals, acc_ptn, lev_max = 1.0):
        total_bal = sum(filter(None,  [ item[k]['notional'] for typ, item in bals.items() for k in item ]  ))
        total_ptn = sum(filter(None,  [ item['notional'] for _, item in acc_ptn.items() ] ) )
        # long/short cancel each other 
        margin_avai = total_bal * lev_max - abs(total_ptn)
        
        # # this is unnecessary for perp
        # # available in specific asset 
        # bal = acc_bal.get(prio_asset, None)
        # bal_tradable = bal['notional'] * lev_max if bal else 0.0
    
        # size_tradable = min(margin_avai, bal_tradable)  
        # return size_tradable
        return margin_avai
    
    ###################
    ### gen orders
    async def gen_orders(self, strat_signal, cap_alloc, alloc_current, bals, ptns,
                         acc = 'dv_aw', table_states = None):
        if not table_states:
             table_states = self.Table_States
        for ticker, item in strat_signal.items():
            s = strat_signal[ticker][0][0]
            a = await self._system_to_asset(s)
            platform = self.AMS_ACC[acc]['platform']
            p_cross = await self._a_to_ptn(a, acc)
            
            # cal delta
            delta_size = await self._cal_delta(ticker, strat_signal[ticker], 
                                    cap_alloc, alloc_current,
                                    table_states = table_states)
            action = 'Buy' if delta_size > 0 else 'Sell'
            # stop ongoing orders the same ticker & model
            ongoing_orders = await self._get_ongoing_orders(ticker, s[:4] )
            cancel_orders = {}
            for venue, orders in ongoing_orders.items():
                cancel_orders[venue] = []
                for k, order in orders.items():
                    # print (order)
                    # cancel current orders of acc only
                    if (order['d']['data'].get('subaccount') == self.AMS_ACC[acc]['account'] ) and (order['d']['data']['side'] != action):
                        order['d']['job'] = "cancel_task"
                        cancel_orders[venue].append(order)                 
    
            # account priority; binance spot first, perp; 
            # get ex from ticker account config
            orders, orders_zk, orders_cy = cancel_orders.get('EVENT_EXEC', []).copy(), cancel_orders.get('EVENT_EXEC', []).copy(), cancel_orders.get('EVENT_EXEC', []).copy()
            order_count = len(orders)
            # ex_lst = await self._get_ex(ticker)
            p = 0
            ## close existing position first
            while abs(delta_size) > MIN_NOTIONAL and p < len(PRIO_SEQ):
                prio_asset = PRIO_SEQ[p]
                i = 0
                while abs(delta_size) > MIN_NOTIONAL  and i < len(self.acc_types):
                    acc_typ = self.acc_types[i]
                    acc_bal = bals.get(acc_typ, {})
                    if prio_asset == 'existing' :
                        # clean existing spot and perp 
                        if 'spot' in acc_typ:                         
                            bal = acc_bal.get(a, None)
                        else:
                            acc_ptn = ptns.get(acc_typ, {})
                            bal = acc_ptn.get(a, None)    # in positions, asset is the symbol                
                        # prc = await self._get_price((a + 'usdt').upper())    # update notional value
                        # bal_notional = bal['total'] * prc if bal else 0.0
                        bal_notional = bal['notional'] if bal else 0.0
                        size_notional = min(abs(bal_notional), abs(delta_size)) if (delta_size * bal_notional ) < 0 else 0.0
                        quote_curr =  self.AMS_ACC[acc]['base_curr'] 
                    else:
                        bal = acc_bal.get(prio_asset, None)
                        bal = bal['notional'] if bal else 0.0
                        #################
                        # spot account couldn't short sell
                        if 'spot' in acc_typ: 
                            if platform == 'dv':
                                # for dv spot can short sell
                                size_notional = min( bal,  abs(delta_size)) 
                            else:
                                size_notional = min( bal,  max(delta_size, 0.0)) 
                        else:
                            acc_ptn = ptns.get(acc_typ, {})
                            # available margin count the asset balance in both spot and perp accounts 
                            pos_avai = await self._get_margin_avai(prio_asset, bals, acc_ptn, lev_max = LEV_MAX)
                            size_notional = min(pos_avai, abs(delta_size))                        
                        quote_curr = prio_asset
    
                    if size_notional > MIN_NOTIONAL:
                        market_type = 'future' if 'perp' in acc_typ else 'spot'
                        subaccount = acc   # add subacc info                                         
                        # for exectutor zk
                        order_zk = await self._format_order_zk(ticker, strat_signal[ticker], 
                                           size_notional, action, 
                                           market_type = market_type,
                                           platform = platform,
                                           subaccount = subaccount,
                                           quote_curr = quote_curr,
                                           order_count = order_count
                                           )
                        orders_zk.append(order_zk)
                        # for general purpose 
                        orders.append(order_zk)

                        # for exectutor cy
                        order_cy = await self._format_order_cy(ticker, strat_signal[ticker], 
                                           size_notional, action, 
                                           market_type = market_type,
                                           platform = platform,
                                           subaccount = subaccount,
                                           quote_curr = quote_curr,
                                           order_count = order_count
                                           )
                        orders_cy.append(order_cy)
                        
                        ## update delta and i
                        order_count += 1
                        delta_size = delta_size - np.sign(delta_size) * size_notional
                    i += 1
                p += 1
        return orders, orders_zk, orders_cy
                                      
    async def _format_order_zk(self, ticker, signal, 
                        size_notional, action, 
                         subaccount = 'neon', 
                         market_type = 'spot',
                         platform = 'binance_spot',
                         trade_params = TRADE_PARAMS,
                         quote_curr = 'usdt', 
                         order_count = 0
                         ): 
        
        
        pct_limit = trade_params[platform]['pct_limit']
        hours = trade_params[platform]['hours']
        cpr = trade_params[platform]['cpr']
        taker = trade_params[platform]['taker']
    
        symbol = await self._ticker_to_symbol(ticker, market_type, quote_curr, platform)
        
        # price limit percentage    
        if action == 'Sell':
            pct_limit = - pct_limit
    
        curr_price  =  signal[0][2][3][-1]   
        prc_quo = 1.0
        if quote_curr == 'btc':
            prc_quo = await self._get_price('BTCUSDT')
        elif quote_curr == 'eth':
            prc_quo = await self._get_price('ETHUSDT')        
        size_notional =  size_notional / prc_quo
        curr_price  =  curr_price / prc_quo
                
        # size = size_notional / (curr_price * (1 + pct_limit))
        size = size_notional / curr_price 
        
        task = {'symbol': symbol, 
                'request_id': self.AMS_ACC[subaccount]['account'] + '_' + symbol + str(signal[0][2][1]) + '_' + str(order_count) ,
                'platform':  platform.replace('perp', 'future') if 'perp' in platform else platform, 
                'total_size': size,
                'order_action': action,
                'prc_limit': curr_price * (1 + pct_limit),
                'time_limit': (dt.datetime.fromtimestamp(signal[0][2][1]) + dt.timedelta(hours = hours )).isoformat(),
                'cpr': cpr,
                'taker' : taker,
                'market_type' : market_type,
                'subaccount' : subaccount,            
            }
        
        order = {'n': 'MQZK',
                 'q': (platform, subaccount), 
                 'd': {'job': 'create_task',
                        'data': task,}
                              }
    
        return order

    async def _format_order_cy(self, ticker, signal, 
                        size_notional, action, 
                         subaccount = 'neon', 
                         market_type = 'spot',
                         platform = 'binance_spot',
                         trade_params = TRADE_PARAMS,
                         quote_curr = 'usdt', 
                         order_count = 0
                         ): 
        
        pct_limit = trade_params[platform]['pct_limit']
        hours = trade_params[platform]['hours']
        cpr = trade_params[platform]['cpr']
        taker = trade_params[platform]['taker']
    
        symbol = await self._ticker_to_symbol(ticker, market_type, quote_curr, platform)
        
        # price limit percentage    
        if action == 'Sell':
            pct_limit = - pct_limit
    
        curr_price  =  signal[0][2][3][-1]   
        prc_quo = 1.0
        if quote_curr == 'btc':
            prc_quo = await self._get_price('BTCUSDT')
        elif quote_curr == 'eth':
            prc_quo = await self._get_price('ETHUSDT')        
        size_notional =  size_notional / prc_quo
        curr_price  =  curr_price / prc_quo
                
        # size = size_notional / (curr_price * (1 + pct_limit))
        size = size_notional / curr_price 
        
        task = {
        "id": signal[0][2][1] ,
        "method": "run",
        "procedure": "smartOrderExec",
        "args": {
            "exchange": "binance" if market_type == 'spot' else  "binance-futures",
            "symbol": symbol, 
            "amount": size if action.lower() == 'buy' else - size,
            "wave_duration_s": 5,
            "limit_order_duration_s": 3,
            "duration_s": hours * 3600 ,
            "limit_orders_post_only": False if taker else True,
            # 'subaccount' : subaccount,            
            }
        }
    
        order = {'n': 'MQZK',
                 'q': (platform, subaccount), 
                 'd': {'job': 'create_task',
                       'data': task,}
                }
    
        return order
    
### main func


