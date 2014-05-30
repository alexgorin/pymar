#!/usr/bin/env python
# -*- coding: utf-8 -*-


class MockChannel:

    def __init__(self):
        pass

    def queue_declare(self, *args, **kwargs):
        pass

    def queue_purge(self, *args, **kwargs):
        pass

    def basic_qos(self, *args, **kwargs):
        pass

    def basic_consume(self, *args, **kwargs):
        pass

    def basic_ack(self, *args, **kwargs):
        pass


class ProducerMockChannel(MockChannel):
    def __init__(self, producer=None):
        self.producer = producer

    def basic_publish(self, **kwargs):
        #Looks as if the request returns immediately without changes.
        if self.producer:
            self.producer.on_response(None, None, kwargs["properties"], kwargs["body"])


class WorkerMockChannel(MockChannel):
    def __init__(self, response):
        self.response = response

    def basic_publish(self, **kwargs):
        self.response["response"] = kwargs["body"]


class ProducerMockConnection:
    def __init__(self, producer):
        self.producer = producer

    def channel(self):
        return ProducerMockChannel()


class WorkerMockConnection:
    def __init__(self, response):
        self.response = response

    def channel(self):
        return WorkerMockChannel(self.response)