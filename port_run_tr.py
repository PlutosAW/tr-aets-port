# -*- coding: utf-8 -*-
"""
Created on Thu Apr 15 19:57:23 2021

@author: adamw
"""

import json, os , time, gzip
import asyncio, aio_pika
from contextlib import suppress


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
AO_TICKERS = [s[4:] + 'USDT' for s in AO_WEIGHTS]
NOT_TICKERS = ['RNDRUSDT']

## get mq instance
MQ = {
      'MQZK': {'pub': BasePublisher,
               'con': BaseConsumer} ,
      }
BCON = MQ['MQZK']['con'](QUEUE = 'mf02_signal', durable=True)

## load ams token and initialize caphandler
AMS_ACC = {'ec_aw': {'platform': 'binance_perp',
                    'account': 'Ec_Aw',
                     'token_id': '', 
                     'base_curr': 'USDT'},            
            }
# Handler = CapHandler(AMS_ACC, Table_Tasks, table_states = Table_States)
Handler = CapHandler(AMS_ACC, Table_Tasks_AO, table_states = Table_States_AO)

## portfolio weights
PORT_WEIGHTS = { 'ec_aw': {'Sect_AO': 2 ,
                        'Cash': 0.3   },
               }

#############
## output    
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

######    
### loop jobs
async def cass_job( message):
    '''
    Parameters
    ----------
    message: strat_signal
    {"HBARBTC": [["Sys6HBAR", "1: Open Long", [1, 1630623607, "2021-09-02T23:00:07+00:00", [5.38e-06, 5.29e-06, 8e-08, 8e-08, 1e-08, 2.58, 5.5e-06]]]]}

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
                await strat_ao(ticker, strat_signal, accs = accs)   
        return
    
async def runJobs(loop):
    # take global variable cross_exs and handler
    
    consumer, conn_c = await BCON.setConsumer(loop)
    
    print ('ampq are loaded')        
    # cback = functools.partial(process_message, consumer )
                 
    await consumer.consume(cass_job)


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
    # asyncio.run(process_message(the_handler))    
    # print ('process one signal takes: ', time.time() - start)
    
