#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import cPickle as pickle
import sys

from utils import WorkerMockConnection as MockConnection, WorkerMockChannel as MockChannel

sys.path.append("../..")
import worker

response = {}

class FakeProducer:

    @staticmethod
    def map_fn(data_source):
        return (x*2 for x in data_source)

    @staticmethod
    def reduce_fn(data_source):
        return sum(data_source)

    @staticmethod
    def routing_key():
        return 1


class FakeDataSourceFactory:

    def __init__(self):
        self.data = range(3)

    def build_data_source(self):
        return self.data


class FakeProperties:
    def __init__(self):
        self.reply_to = None
        self.correlation_id = 2


class FakeMethod:
    def __init__(self):
        self.delivery_tag = None


class FakePika:
    """So many fakes in the world. Even pika is a fake."""

    def __init__(self):
        self.calls = []

    def BlockingConnection(self, *args, **kwargs):
        return MockConnection(response)

    def ConnectionParameters(self, *args, **kwargs):
        pass

    def BasicProperties(self, *args, **kwargs):
        pass


class TestWorker(unittest.TestCase):

    def setUp(self):
        global response
        response = {}

    def test_worker(self):
        global worker
        worker.pika = FakePika()
        worker = worker.Worker(FakeProducer, 0)
        message = pickle.dumps(FakeDataSourceFactory())

        #"Send" message to the worker and read the result of calculations
        worker.on_request(worker.channel, FakeMethod(), FakeProperties(), message)
        result = pickle.loads(response["response"])

        self.assertEqual(result, sum(x*2 for x in range(3)))