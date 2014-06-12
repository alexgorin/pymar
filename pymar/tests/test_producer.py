#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from pika.exceptions import AMQPConnectionError

from utils import ProducerMockConnection, ProducerMockChannel
from pymar.producer import Producer


class MockFactory():
    def __init__(self, limit, offset=0):
        self.limit = limit
        self.offset = offset

    def length(self):
        return self.limit

    def part(self, limit, offset):
        return MockFactory(limit, offset)

    def params(self):
        return self.limit, self.offset

    def build_data_source(self):
        return range(self.offset, self.limit + self.offset)


def params(factories):
    return [factory.params() for factory in factories]


def fake_connect(self, mq_server):
    self.connection = ProducerMockConnection(self)
    self.channel = ProducerMockChannel(self)
    self.callback_queue = "queue_name"


def failed_connect(self, mq_server):
    raise AMQPConnectionError()


class TestProducer(unittest.TestCase):
    def setUp(self):
        Producer.connect = fake_connect
        Producer.reduce_fn = lambda cls, data : data
        self.producer = Producer()
        self.producer.workers_number = lambda: 4

    def tearDown(self):
        pass

    def test_divide(self):
        factory = MockFactory(14)   # length > workers_number
        parts = self.producer.divide(factory)
        self.assertListEqual(params(parts), [
            (4, 0), (4, 4), (4, 8), (2, 12)
        ])

        factory = MockFactory(2)    # length < workers_number
        parts = self.producer.divide(factory)
        self.assertListEqual(params(parts), [
            (1, 0), (1, 1)
        ])

    def test_map(self):
        factory = MockFactory(7)

        #"Send" factories and "receive" them back
        self.assertListEqual(params(self.producer.map(factory)),[
            (2, 0), (2,2), (2, 4), (1, 6)
        ])

    def test_local_mode(self):
        Producer.connect = failed_connect
        Producer.map_fn = lambda cls, data : [elem*2 for elem in data]
        Producer.reduce_fn = lambda cls, data : sum(data)
        producer = Producer()
        self.assertEqual(producer.map(MockFactory(10)), 2*sum(range(10)))
