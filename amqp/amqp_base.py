# -*- coding: utf-8 -*-
"""
Created on Wed Mar 31 17:17:33 2021
this base should be inherited into any publisher
any publisher needs to input:
    - messaging exchange
    - queue
    - routing key
    
callback functions: 
    -     
    

@author: adamw
"""
import asyncio
import aio_pika

import functools
import logging
import json, os
import random

try: AMQP_URL_c = os.environ['AMQP_URL_c']
except: AMQP_URL_c = input("Enter AMQP_URL_c: ")

try: AMQP_URL_p = os.environ['AMQP_URL_p']
except: AMQP_URL_p = input("Enter AMQP_URL_p: ")

class BaseMessager(object):
    """This is an example publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.
    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.
    It uses delivery confirmations and illustrates one way to keep track of
    messages that have been sent and if they've been confirmed by RabbitMQ.
    
    HOST = 'hornet.rmq.cloudamqp.com', 
    USER = 'lbfghyfh',
    PS = 'YTuEGHN2A_rKzmNhIFCCg4nBYF-Zli38',
    """
    
    def __init__(self, 
                 EXCHANGE = 'message', 
                 EXCHANGE_TYPE = 'direct',
                 QUEUES = ['text'],
                 ROUTING_KEY = "sample.routing_key",
                 CONN_CHECK_INTERVAL = 300, 
                 RECONN_INTERVAL = 1,
                 Cback = None):
        """Setup the example publisher object, passing in the URL we will use
        to connect to RabbitMQ.
        :param str amqp_url: The URL for connecting to RabbitMQ
        """
        self.amqp_config = dict(host = HOST, 
                    port = 1883 , 
                    username = USER, password = PS )
        self.queue_config = dict(
                exchange = EXCHANGE,
                queues = QUEUES,
                routing_key = ROUTING_KEY,
                exchange_type = EXCHANGE_TYPE,
                error_messaging = dict(
                    exchange = "error_exchange",
                    queues = ["error_queue"],
                    routing_key = "error.key",
                    exchange_type = EXCHANGE_TYPE,
                    ),
                )
        self.queue_settings = {'reconnect_backoff_secs': RECONN_INTERVAL, 
                                'connection_check_polling_secs': CONN_CHECK_INTERVAL} # Not compulsory

        self.messager = self.connect()

    def connect(self, Cback):
        def consumer_func(message: dict, func=Cback):
            # define your consumer func
            # cback = functools.partial(func
            #     , userdata = message)
            # return cback
            pass

        messager = self.Messaging(self.amqp_config, self.queue_config, 
                                 consumer_func)
        # use this messaging object globally for sending message like this:
        # messaging.send_message(message) # To send single message
        # messaging.send_messages(messages) # To send multiple bulk messages
        # Message sent must be dict.
        return messager


# ## * Only Consumer:
# #----------------
class BaseConsumer(object):
    """This is an example publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.
    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.
    It uses delivery confirmations and illustrates one way to keep track of
    messages that have been sent and if they've been confirmed by RabbitMQ.
    """
    
    def __init__(self, 
                     QUEUE = 'live_strat_data_hourly',
                     durable=True, 
                    exclusive=False, 
                    auto_delete=False
                 ):
        """Setup the example publisher object, passing in the URL we will use
        to connect to RabbitMQ.
        :param str amqp_url: The URL for connecting to RabbitMQ
        """
        self.QUEUE = QUEUE
        self.durable = durable, 
        self.exclusive = exclusive, 
        self.auto_delete = auto_delete

        # self.connection = None
        # self.channel = None
        # self.consumer = asyncio.run( 
        #             self.setConsumer(AMQP_URL,
        #                     QUEUE )
        #         )
    
    # To generate consumer objector and deal callback function in application
    
    async def setConsumer(self, loop ):
        
        connection = await aio_pika.connect_robust(AMQP_URL_c, 
                                                        loop = loop )        
        channel = await connection.channel()    # type: aio_pika.Channel
        await channel.set_qos(prefetch_count=1) # set quote =1
        
        # Declaring queues if not existing
        try:
            queue_obj = await channel.get_queue(self.QUEUE )
        except:    
            channel = await connection.channel()    # type: aio_pika.Channel
            queue_obj = await channel.declare_queue( self.QUEUE ,
                                                    durable=True, 
                                                   exclusive=False, 
                                                   auto_delete=False)

        return queue_obj, connection
    
    async def process_message(self, func, message: aio_pika.IncomingMessage):
        async with message.process():
            await func(message.body)
            await asyncio.sleep(1)
        
    async def consumer_single(self):
        message = await self.consumer.get()
        async with message.process():
            return message.body.decode() 
    


# * Only Producer:
# ----------------
class BasePublisher(object):
    """This is an example publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.
    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.
    It uses delivery confirmations and illustrates one way to keep track of
    messages that have been sent and if they've been confirmed by RabbitMQ.
    """
        
    def __init__(self,  
                     EXCHANGE = 'test', 
                     EXCHANGE_TYPE = 'direct',
                     QUEUES = ['live_strat_data_hourly'],
                     ROUTING_KEY = "sample.routing_key",
                     PUBLISH_INTERVAL = 1, 
                     DURABLE = True):
        """Setup the publisher object, passing in the URL we will use
        to connect to RabbitMQ.
        :param str amqp_url: The URL for connecting to RabbitMQ
        :exchange configs,
        :use a ROUTING_KEY to broadcast multiple queues
        """
        self.EXCHANGE = EXCHANGE
        self.EXCHANGE_TYPE = EXCHANGE_TYPE
        self.QUEUES = QUEUES
        self.ROUTING_KEY = ROUTING_KEY
        self.PUBLISH_INTERVAL = PUBLISH_INTERVAL
        self.DURABLE = DURABLE
        
        # self.connection = None
        # self.channel = None
        # self.producer = loop.run_until_complete( 
        #         self.setProducer(loop )
        #         )

    async def setProducer(self, loop):
        if loop:
            loop = loop
        else:
            loop = asyncio.get_event_loop()
        connection = await aio_pika.connect_robust(AMQP_URL_p, 
                                                   loop = loop)        
        channel = await connection.channel() 
        # Declare exchange
        exchange = await channel.declare_exchange(self.EXCHANGE, 
                                       type = self.EXCHANGE_TYPE , durable = self.DURABLE)
        # Declaring queues
        for QUEUE in self.QUEUES:
            queue = await channel.declare_queue( QUEUE, 
                                    durable=True, 
                                   exclusive=False, 
                                   auto_delete=False)
            # Binding the queue to the exchange
            await  queue.bind(exchange, routing_key = self.ROUTING_KEY)    
    
        return exchange  , connection  
       


#################
# if __name__ == "__main__":
    # pass



    







