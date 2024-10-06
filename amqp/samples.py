# -*- coding: utf-8 -*-
"""
Created on Sun Apr  4 16:04:06 2021

@author: adamw
"""
import nest_asyncio
nest_asyncio.apply()

import asyncio
import aio_pika

import random
import json

####################################################
## sample code for consumer
from amqp_base import BaseConsumer

BCON = BaseConsumer(QUEUE = 'hello')

# live_strat_data_hourly
async def consumer(BCON):
    async with BCON.consumer.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                print(message.body)
                if  BCON.consumer.name in message.body.decode():
                    break
async def consumer_single(BCON):
    message = await BCON.consumer.get()
    async with message.process():
        print (message.body.decode() )
        
async def _close(_obj):
    await _obj.channel.close()
    await _obj.connection.close()
    
asyncio.run(  consumer(BCON) )  
asyncio.run(  consumer_single(BCON) )  
   
asyncio.run(_close(BCON))

############################################
## sample publisher
from amqp_base import BasePublisher


routing_key = "hello"
BPUB = BasePublisher(EXCHANGE = 'test', 
                 EXCHANGE_TYPE = 'direct',
                 QUEUES = ['hello'],
                 ROUTING_KEY = routing_key,
                 )  # live_strat_data_hourly

tickers = {
    'MXSE.EQBR.LKOH': (1933, 1940),
    'MXSE.EQBR.MSNG': (1.35, 1.45),
    'MXSE.EQBR.SBER': (90, 92),
    'MXSE.EQNE.GAZP': (156, 162),
    'MXSE.EQNE.PLZL': (1025, 1040),
    'MXSE.EQNL.VTBR': (0.05, 0.06)
}

def getticker():
    return list(tickers.keys())[
        random.randrange(0, len(tickers) - 1)]
 
async def producer(routing_key):
    for i in range(0, 5):
        ticker = getticker()
        msg = aio_pika.Message(
                body=  json.dumps({
            'test.hello': {
                'data': {
                    'params': {
                        'condition': {
                            'ticker': ticker
                            }
                        }
                    }
                }
            } ).encode() 
            ) 
                
        await BPUB.producer.publish( msg,
            routing_key=routing_key )
        
        print (ticker)
    
asyncio.run(producer(  routing_key))





