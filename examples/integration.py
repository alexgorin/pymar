#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Example of using Pymar: Integration of a function.
"""

from pymar.datasource import DataSource, DataSourceFactory
from pymar.producer import Producer


def func(x):
    """Function to integrate"""
    from math import exp
    return exp(x)


class IntegrationProducer(Producer):
    """"Producer for the task of integration of function.
    """

    #Number of parts to divide the task
    #May be not equal to the actual number of workers,
    #but if it is equal, the performance will be maximum.
    WORKERS_NUMBER = 4

    @staticmethod
    def map_fn(data_source):
        dx = data_source.dx
        return (func(val)*dx for val in data_source)

    @staticmethod
    def reduce_fn(data_source):
        return sum(data_source)


class IntegrationDataSource(DataSource):
    """"Data source for the task of integration of function
    In this case it is integral from 1 to 10 of exp(x),
    so you can easily check the answer.
    """

    interval = (1, 10)

    #Step of integration
    dx = 0.000001

    @classmethod
    def full_length(cls):
        return int((cls.interval[1] - cls.interval[0]) / cls.dx)

    def __iter__(self):
        """Returns sequence of x values on interval"""
        return (self.interval[0] + x*self.dx for x in range(self.offset, self.offset + self.limit))

if __name__ == "__main__":
    """
    Before starting this script launch corresponding workers:
    worker.py ./examples/integration.py -s IntegrationDataSource -p IntegrationProducer -q 127.0.0.1 -w 4
    """
    producer = IntegrationProducer()
    factory = DataSourceFactory(IntegrationDataSource)
    value = producer.map(factory)

    print "Answer: ", value
