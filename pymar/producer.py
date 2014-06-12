#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cPickle as pickle
import logging
import pika
import uuid

from threading import Timer

from pymar.exceptions import TimeOutException

logging.basicConfig(logging=logging.DEBUG,
                            format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s")
logging.getLogger("").setLevel(logging.INFO)


class Producer(object):
    """Represents the class which schedules tasks to the workers and returns the result.
    You have to inherit form it and override map_fn and reduce_fn (don't forget @staticmethod decorator).
    Also, set approximate number of workers. It the future you will perhaps not have to do that.
    To achieve maximum performance, WORKERS_NUMBER must be equal to the actual number of workers.
    If WORKERS_NUMBER is less, some workers will not get tasks.
    If greater, some workers will have more than one task.
    (Although, if each task requires a lot of memory, it is probably what you will want).
    In any case, it will not lead to incorrect answer, just to decrease of performance.

    To run it, call map (not map_fn!) with DataSourceFactory as an argument.
    It connects to MQ server (author used RabbitMQ, but any server supporting AMQP will probably be fine),
    divides task to workers, sends requests and awaits the responses. When all the responses are received,
    reduces them and returns the result.

    If some workers die in progress, tasks will just be reassigned to other workers by MQ server. So, even one worker
    is always enough to complete the whole task (not optimally, although).
    """

    WORKERS_NUMBER = 10

    def __init__(self, mq_server="localhost", local_mode=False):
        self.unprocessed_request_num = 0
        self.responses = []
        self.mq_server = mq_server
        self.logging = logging.getLogger(str(self.__class__.__name__))
        self.correlation_id = str(uuid.uuid4()).replace("_", "")
        self.local_mode = local_mode
        if not self.local_mode:
            try:
                self.connect(mq_server)
            except pika.exceptions.AMQPConnectionError:
                self.logging.warning("Cannot connect to MQ server. Working locally.")
                self.local_mode = True

    def connect(self, mq_server):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=mq_server))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.on_response, no_ack=True,
                                   queue=self.callback_queue)

    @staticmethod
    def map_fn(data_source):
        raise NotImplementedError()

    @staticmethod
    def reduce_fn(data_source):
        raise NotImplementedError()

    def on_response(self, ch, method, props, body):
        corr_id, index = props.correlation_id.split("_")
        if corr_id == self.correlation_id:
            self.unprocessed_request_num -= 1
            body = pickle.loads(body)
            self.logging.info("Got response for %d-th request: %s" % (int(index) + 1, body))
            self.responses[int(index)] = body


    @classmethod
    def routing_key(cls):
        return cls.__name__

    def workers_number(self):
        return self.WORKERS_NUMBER

    def divide(self, data_source_factory):
        """Divides the task according to the number of workers."""
        data_length = data_source_factory.length()
        data_interval_length = data_length / self.workers_number() + 1

        current_index = 0
        self.responses = []
        while current_index < data_length:
            self.responses.append(0)
            offset = current_index
            limit = min((data_length - current_index, data_interval_length))
            yield data_source_factory.part(limit, offset)
            current_index += limit

    def map(self, data_source_factory, timeout=0, on_timeout="local_mode"):
        """Sends tasks to workers and awaits the responses.
        When all the responses are received, reduces them and returns the result.

        If timeout is set greater than 0, producer will quit waiting for workers when time has passed.
        If on_timeout is set to "local_mode", after the time limit producer will run tasks locally.
        If on_timeout is set to "fail", after the time limit producer raise TimeOutException.
        """
        def local_launch():
            print "Local launch"
            return self.reduce_fn(
                        self.map_fn(data_source_factory.build_data_source())
                    )

        if self.local_mode:
            return local_launch()

        for index, factory in enumerate(self.divide(data_source_factory)):
            self.unprocessed_request_num += 1
            self.logging.info("Sending %d-th message with %d elements" % (index + 1, factory.length()))
            self.logging.info("len(data) = %d" % len(pickle.dumps(factory)))
            self.channel.basic_publish(exchange='',
                                       routing_key=self.routing_key(),
                                       properties=pika.BasicProperties(
                                           reply_to=self.callback_queue,
                                           correlation_id="_".join((self.correlation_id, str(index))),
                                       ),
                                       body=pickle.dumps(factory))

        self.logging.info("Waiting...")

        time_limit_exceeded = [False]

        def on_timeout_func():
            print "Timeout!!"
            self.logging.warning("Timeout!")
            time_limit_exceeded[0] = True

        if timeout > 0:
            self.timer = Timer(timeout, on_timeout_func)
            self.timer.start()

        while self.unprocessed_request_num:
            if time_limit_exceeded[0]:
                if on_timeout == "local_mode":
                    return local_launch()

                assert on_timeout == "fail", "Invalid value for on_timeout: %s" % on_timeout
                raise TimeOutException()

            self.connection.process_data_events()

        self.logging.info("Responses: %s" % str(self.responses))
        return self.reduce_fn(self.responses)

