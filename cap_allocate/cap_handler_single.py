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

from db_resources.db_client import CVDB
from db_resources.db_client_mg import get_mongo


############
# load ams token
try: 
    AMS_TOKEN_ID = os.environ['AMS_TOKEN_ID']
except: 
    AMS_TOKEN_ID = input("Enter AMS_TOKEN_ID: ")
# load db    
DB_NAME = "pdtlite"
Acc_Types = {"binance_perp": "perp_position",
             "binance_spot": "spot_balance",     }
AMS_ACC = {'neon': {'account': "Cassandra",
                    'token_id': AMS_TOKEN_ID,   },
           'prop': {'account': "binance_trading123",
                    'token_id': AMS_TOKEN_ID,   },
           }

Table_States = 'Current_States'
Table_Tasks = 'Table_Tasks'
Table_Opt_Weights = 'Optimal_Weights'

live_db = CVDB()
# get cap allo weight
CASS_WEIGHTS = live_db.load_data(Table_Opt_Weights)
# ST_STATES = live_db.load_data(Table_States)

# load config 
with open('config_port.json', 'r') as f:
    config = json.load(f)
ACC_MQ = config['ACC_MQINFO']
EX_TK = config['EX_TICKER']
TRADE_PARAMS = config['TRADE_PARAMS']
LEV_MAX = 3.0

PRIO_SEQ = ['existing', 'usdt', 'btc']

###############################
## get current port info
async def _add_dicts(port_old, port_new):
   for a in port_new:
        b1 = port_old.get(a, {'total': 0.0 , 'notional': 0.0})
        b2 = port_new[a]
        x = {k: sum(filter(None, [b2[k],  b1[k]])) for k in b2 }
        port_old.update({a: x})
   return port_old

async def get_port_info_h(typ_map = Acc_Types):
    # get hourly port info ; 
    mongo_db = (await get_mongo())[DB_NAME]
    bals, ptns = {}, {}
    for typ in typ_map:
        bals[typ], ptns[typ] = {}, {}
        db_table = getattr(mongo_db, typ_map[typ])
        async for doc in db_table.find().sort('timestamp', -1).limit(len(AMS_ACC)):
            # print (doc)
            if 'spot' in typ:
                bal = doc['balances']
                bals[typ] = await _add_dicts(bals[typ], bal)
                
            elif 'perp' in typ:
                pos = doc['positions']
                pos = {cross: pos[cross] for cross in pos if abs(pos[cross]['notional']) > 0}
                # pos={'btc_usdt': {'total': 1, 'notional': 22778.6},}
                ptns[typ] = await _add_dicts(ptns[typ], pos)
                
                # bal = doc['margin_remaining'] + doc['margin_reserved'] 
                # support U@M only. TODO add notition to margin assets
                bal = {'usdt': {'total': doc['equity'], 'notional': doc['equity']}}   
                bals[typ] = await _add_dicts(bals[typ], bal)
                                
    # bals['binance_perp'].update( {'btc': {'total': 0.00000381638, 'notional': 0.0681638}})
    return bals, ptns

async def get_port_info_h_acc(typ_map = Acc_Types):
    # get hourly port info ; 
    mongo_db = (await get_mongo())[DB_NAME]
    bals, ptns = {k: {} for k in AMS_ACC}, {k: {} for k in AMS_ACC}
    for typ in typ_map:
        db_table = getattr(mongo_db, typ_map[typ])
        # async for doc in db_table.find().sort('timestamp', -1).limit(len(AMS_ACC)):
        #     # print (doc)
        #     acc = [k  for k in AMS_ACC if AMS_ACC[k]['account']==doc['account_id']][0]
        for acc in AMS_ACC:
            async for doc in db_table.find({'account_id': AMS_ACC[acc]['account']}
                                           ).sort('timestamp', -1).limit(1):
                if 'spot' in typ:
                    bal = doc['balances']
                    bals[acc][typ] = bal
                elif 'perp' in typ:
                    pos = doc['positions']
                    pos = {cross: pos[cross] for cross in pos if abs(pos[cross]['notional']) > 0}
                    # pos={'btc_usdt': {'total': 1, 'notional': 22778.6},}
                    ptns[acc][typ] = pos
                    
                    # bal = doc['margin_remaining'] + doc['margin_reserved'] 
                    # TODO add notition to margin assets
                    bal = {'usdt': {'total': doc['equity'], 'notional': doc['equity']}}                
                    bals[acc][typ] = bal                
                
    # bals['binance_perp'].update( {'btc': {'total': 0.00000381638, 'notional': 0.0681638}})
    return bals, ptns

## get ongoing orders
async def _get_ongoing_orders(ticker, model):
    tasks = live_db.load_data(Table_Tasks)
    ongoing_orders = {}
    if len(tasks) > 0 :
        xts_orders = {k: v for k, v in tasks.items() if (v['n'] == 'XTS')  }
        if len(xts_orders) > 0:
            ongoing_orders = {k: v for k, v in xts_orders.items() if (v['d']['data']['end_time'] > time.time_ns() 
                                                           and v['d']['data']['symbol'].split('_')[0] in ticker
                                                           and v['d']['data']['strategy_id'] == model  
                                                           and v['d']['job'] == "create_task")                                                   
                        }
    return ongoing_orders

## prepare symbol
async def _ticker_to_symbol(ticker, market_type, quote_curr, platform):
    if market_type == 'spot': 
        symbol = ticker[:-4] + quote_curr.upper()
    elif market_type == 'future': 
        symbol = ticker[:-4] + quote_curr.upper()
        if platform == 'ftx':
            symbol = symbol + '-PERP'
    return symbol

async def _system_to_asset(system):
    a = system[4:].lower()
    return a
async def _a_to_ptn(a):
    p_cross = a + '_usdt'
    return p_cross

async def _system_to_st(system, ticker):
    if 'Sys6' in system:
        st = 'VM' + ticker
    elif 'Sys7' in system:
        st = 'VF' + ticker
    elif 'Sys8' in system:
        st = 'FC' + ticker
    else:
        print ("unknown system")
        st = None
    return st
async def _st_to_system(st_id):
    if st_id[:2] == 'VM':
        system = 'Sys6' + st_id[2:-4] if 'USDT' in st_id else 'Sys6' + st_id[2:-3]
    elif st_id[:2] == 'VF':
        system = 'Sys7' + st_id[2:-4] if 'USDT' in st_id else 'Sys7' + st_id[2:-3]
    elif st_id[:2] == 'FC':
        system = 'Sys8' + st_id[2:-4] if 'USDT' in st_id else 'Sys8' + st_id[2:-3]
    else:
        print ("unknown st_id")
        system = None
    return system

async def _cross_to_asset(cross):
    a = cross.split('_')[0].lower()
    return a

## get ex
async def _get_ex(ticker):
    ex_lst = []
    for ex in EX_TK:
        if ticker in EX_TK[ex]:
            ex_lst.append(ex)
    return ex_lst

## get prices
async def _get_price(cross):
    # TODO use router to get price from price engine of cass_rept
    states  = live_db.load_data( Table_States )    
    state = states[f'VM{cross}']
    prc = state[3][-1]        
    return prc

async def _get_price_btc():
    # TODO use router to get price from price engine of cass_rept
    states  = live_db.load_data( Table_States )    
    state_btc = states['VMBTCUSDT']
    prc_btc = state_btc[3][-1]        
    return prc_btc

async def _get_price_usd(prc_ticker_btc):
    # TODO use router to get price from price engine of cass_rept
    states  = live_db.load_data( Table_States )    
    state_btc = states['VMBTCUSDT']
    prc_btc = state_btc[3][-1]    
    prc_ticker_usd = prc_ticker_btc * prc_btc
    return prc_ticker_usd

##################
# port logic: 
#     1. one subacc only support mutually exclusive strategy tickers;
#     2. current strat states needs to be used if one subacc trade the same tickers in different strats;
### cal cap alloc 
async def get_alloc(bals, ptns, weights = CASS_WEIGHTS ):
    # bals['binance_future'] = {'eth': {'total': 5.7e-05, 'notional': 0.09969756},}
    total_bal = sum(filter(None,  [ item[k]['notional'] for acc, item in bals.items() for k in item ] ) )
    cap_alloc = { k: total_bal * v for k, v in weights.items() }
    
    # current alloc
    alloc_current = {}
    for acc, item in bals.items():
        for k in item:
            if k in alloc_current:
                alloc_current[k] += item[k]['notional']
            elif item[k]['notional']: 
                alloc_current[k] = item[k]['notional']  
    for acc, item in ptns.items():
        for k in item:
            a = await _cross_to_asset(k)            
            if a in alloc_current:
                alloc_current[a] += item[k]['notional']
            elif item[k]['notional']: 
                alloc_current[a] = item[k]['notional']  
                
    return  cap_alloc,   alloc_current 
        
    # for acc in Acc_Types:       
    #     if 'spot' in acc:
    #         acc_bal = sum(filter(None, [bals[acc][k]['notional'] for k in bals[acc]] ) 

async def _cal_delta(ticker, signal, cap_alloc, alloc_current):
    
    s = signal[0][0]
    a = await _system_to_asset(s)
    st_id = await _system_to_st(s, ticker)
    st_states = live_db.load_data(Table_States)
    # update states
    st_states[st_id][0] = int(signal[0][1].split(': ')[0])
    
    cap_a = 0
    for sn in st_states:
        if a.upper() in sn[:-3]:    
            alias = await _st_to_system(sn)        
            cap_a += cap_alloc[alias] * st_states[sn][0]
            # print (sn, cap_alloc[alias])
    
    delta_size = cap_a - alloc_current.get(a, 0.0)
    
    return delta_size
    
async def _get_margin_avai(prio_asset, acc_bal, acc_ptn, lev_max = 1.0):
    total_bal = sum(filter(None,  [ item['notional'] for _, item in acc_bal.items() ] ) )
    total_ptn = sum(filter(None,  [ item['notional'] for _, item in acc_ptn.items() ] ) )
    # long/short cancel each other 
    margin_avai = total_bal * lev_max - abs(total_ptn)
    
    # available in specific asset 
    bal = acc_bal.get(prio_asset, None)
    bal_tradable = bal['notional'] * lev_max if bal else 0.0

    size_tradable = min(margin_avai, bal_tradable)  
    return size_tradable

###################
### gen orders
async def gen_orders(strat_signal, cap_alloc, alloc_current, bals, ptns, acc = 'neon'):
    for ticker, item in strat_signal.items():
        s = strat_signal[ticker][0][0]
        a = await _system_to_asset(s)
        p_cross = await _a_to_ptn(a)
        
        # cal delta
        delta_size = await _cal_delta(ticker, strat_signal[ticker], cap_alloc, alloc_current)
        action = 'Buy' if delta_size > 0 else 'Sell'
        # stop ongoing orders the same ticker & model
        ongoing_orders = await _get_ongoing_orders(ticker, s[:4] )
        cancel_orders = []
        for _, order in ongoing_orders.items():
            # cancel current orders of acc only
            if (order['d']['data']['account'] == acc ) and (order['d']['data']['side'] != action):
                order['d']['job'] = "cancel_task"
                order['d']['data']['order_action'] = "Stop"
                cancel_orders.append(order)                 

        # account priority; binance spot first, perp; 
        # get ex from ticker account config
        orders, orders_zk = cancel_orders.copy(), cancel_orders.copy()
        order_count = len(orders)
        ex_lst = await _get_ex(ticker)
        p = 0
        ## close existing position first
        while abs(delta_size) > 10.0 and p < len(PRIO_SEQ):
            prio_asset = PRIO_SEQ[p]
            i = 0
            while abs(delta_size) > 10.0 and i < len(ex_lst):
                ex = ex_lst[i]
                acc_bal = bals.get(ex, {})
                if prio_asset == 'existing' :
                    # clean existing spot and perp 
                    if 'spot' in ex:                         
                        bal = acc_bal.get(a, None)
                    else:
                        acc_ptn = ptns.get(ex, {})
                        bal = acc_ptn.get(p_cross, None)                    
                    prc = await _get_price((a + 'usdt').upper())    # update notional value
                    bal_notional = bal['total'] * prc if bal else 0.0
                    size_notional = min(abs(bal_notional), abs(delta_size)) if (delta_size * bal_notional ) < 0 else 0.0
                    quote_curr = 'usdt'
                else:
                    bal = acc_bal.get(prio_asset, None)
                    bal = bal['notional'] if bal else 0.0
                    # spot account couldn't short sell
                    if 'spot' in ex:                        
                        size_notional = min( bal,  max(delta_size, 0.0)) 
                    else:
                        acc_ptn = ptns.get(ex, {})
                        pos_avai = await _get_margin_avai(prio_asset, acc_bal, acc_ptn, lev_max = LEV_MAX)
                        size_notional = min(pos_avai, abs(delta_size))                        
                    quote_curr = prio_asset

                if size_notional > 0:
                    market_type = 'future' if 'perp' in ex else 'spot'
                    platform = ex
                    subaccount = acc   # add subacc info                                         
                    # for xts
                    order = await _format_order(ticker, strat_signal[ticker], 
                                       size_notional, action, 
                                       market_type = market_type,
                                       platform = platform,
                                       subaccount = subaccount,
                                       quote_curr = quote_curr, 
                                       order_count = order_count
                                       )
                    orders.append(order)
                    
                    # for exectutor zk
                    order_zk = await _format_order_zk(ticker, strat_signal[ticker], 
                                       size_notional, action, 
                                       market_type = market_type,
                                       platform = platform,
                                       subaccount = subaccount,
                                       quote_curr = quote_curr
                                       )
                    orders_zk.append(order_zk)
                    
                    ## update delta and i
                    order_count += 1
                    delta_size = delta_size - np.sign(delta_size) * size_notional
                i += 1
            p += 1
    return orders, orders_zk
           
async def _format_order( ticker, signal, 
                        size_notional, action, 
                     subaccount = 'neon', 
                     market_type = 'spot',
                     platform = 'binance_spot',
                     trade_params = TRADE_PARAMS,
                     quote_curr = 'USDT', 
                     order_count = 0
                     ): 
       
    pct_limit = trade_params[platform]['pct_limit']
    hours = trade_params[platform]['hours']
    cpr = trade_params[platform]['cpr']
    taker = trade_params[platform]['taker']
    begin_time = int(signal[0][2][1] * 1e+9)
    end_time = int(begin_time + hours * 3600 * 1e+9)
    
    symbol = await _ticker_to_symbol(ticker, market_type, quote_curr, platform)
            
    # price limit percentage    
    if action == 'Sell':
        pct_limit = - pct_limit

    curr_price  =  signal[0][2][3][-1]   
    prc_quo = 1.0
    if quote_curr == 'btc':
        prc_quo = await _get_price('BTCUSDT')
    elif quote_curr == 'eth':
        prc_quo = await _get_price('ETHUSDT')        
    size_notional =  size_notional / prc_quo
    curr_price  =  curr_price / prc_quo
            
    # size = size_notional / (curr_price * (1 + pct_limit))
    size = size_notional / curr_price 
    
    task = {'counter': "hyperbola",
            'account': AMS_ACC[subaccount]['account'],
            'token_id': AMS_ACC[subaccount]['token_id'],
            'trader_id': 'zkwang', 
            'strategy_id': signal[0][0][:4],
            # 'action_type': "Create", 
            'order_action': "Create", 
            'exec_type': "VWAP", 
            'max_prate': 0.25,
            
            'request_id': symbol + str(signal[0][2][1]) + '_' + str(order_count) ,
            'symbol': symbol.split(quote_curr.upper())[0] + '_' + quote_curr.upper() + '..' + platform.upper().replace('_', '..') , 
            'side': action,
            'target_volume': size, 
            'begin_time': begin_time,
            'end_time': end_time,
            'limit_price': curr_price * (1 + pct_limit),
            'target_prate': cpr,
            'urgency_type': "HighUrgency" if taker == 'Yes' else "MediumUrgency",
                            
            'json_param': json.dumps({
                                'platform': platform,
                                'market_type' : market_type,
                                'subaccount' : subaccount,   
                                'quanity': size,
                                } )
        }
    
    order = {'n': 'XTS',
             'q': (platform, subaccount), 
             'd': {'job': 'create_task',
                  'data': task,}
            }
    return order
    
                                      
async def _format_order_zk(ticker, signal, 
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

    symbol = await _ticker_to_symbol(ticker, market_type, quote_curr, platform)
    
    # price limit percentage    
    if action == 'Sell':
        pct_limit = - pct_limit

    curr_price  =  signal[0][2][3][-1]   
    prc_quo = 1.0
    if quote_curr == 'btc':
        prc_quo = await _get_price('BTCUSDT')
    elif quote_curr == 'eth':
        prc_quo = await _get_price('ETHUSDT')        
    size_notional =  size_notional / prc_quo
    curr_price  =  curr_price / prc_quo
            
    # size = size_notional / (curr_price * (1 + pct_limit))
    size = size_notional / curr_price 
    
    task = {'symbol': symbol, 
            'request_id': symbol + str(signal[0][2][1]) + '_' + str(order_count) ,
            'platform': platform,
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

### main func


