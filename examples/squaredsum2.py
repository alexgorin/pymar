#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Example of using Pymar: squared sum of elements of a list.
In this case list if created on producer and sent to workers.
Each worker receives only its part of data.
"""

from pymar.datasource import DataSourceFactory
from pymar.producer import Producer


class SimpleProducer(Producer):
    """Producer for the task of sum of squares of values
    """
    WORKERS_NUMBER = 4

    @staticmethod
    def map_fn(data_source):
        for val in data_source:
            yield val**2

    @staticmethod
    def reduce_fn(data_source):
        return sum(data_source)

if __name__ == "__main__":
    """
    Before starting this script launch corresponding workers:
    worker.py -f ./examples/squaredsum2.py -p SimpleProducer -q 127.0.0.1 -w 4
    """

    #Assume that this data were formed in some long and complicated procedure
    #which we don't want to repeat on workers, so it is easier to send data there.
    #Otherwise, use DataSource subclasses.
    data = range(10**7)

    factory = DataSourceFactory(data)
    producer = SimpleProducer()

    value = producer.map(factory)
    print "Answer: ", value