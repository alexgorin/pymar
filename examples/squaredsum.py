#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Example of using Pymar: squared sum of elements of a list.
In this case list if created on workers.
"""

from pymar.datasource import DataSource, DataSourceFactory
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


class SimpleDataSource(DataSource):
    """Data source for the task of sum of squares of values from 1 to N.
    Values to summarize are calculated on workers, not on producer.
    sum((x**2 for x in range(10**7))) can be calculated fast enough without parallelism,
    so you can easily check the result. The increase of performance will be in the case of larger N.
    """
    N = 10**7

    @classmethod
    def full_length(cls):
        return cls.N

    def __iter__(self):
        return (i for i in xrange(self.offset, self.offset + self.limit))


if __name__ == "__main__":
    """
    Before starting this script launch corresponding workers:
    worker.py  ./examples/squaredsum.py -s SimpleDataSource -p SimpleProducer -q 127.0.0.1 -w 4
    """

    producer = SimpleProducer()
    factory = DataSourceFactory(SimpleDataSource)

    value = producer.map(factory)
    print "Answer: ", value