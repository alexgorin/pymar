#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Set of simple examples of map-reduce tasks performed with Pymar.
To run examples, workers must be already set up.

As you can see, each example requires only several lines of code, which makes Pymar very easy to use.
"""

import logging
from pymar.datasource import DataSource, DataSourceFactory, ListDataSource, DictDataSource, SQLDataSource
from pymar.producer import Producer


class SimpleProducer(Producer):
    """Producer for the task of sum of squares of values
    For this particular task the keys are not really needed, but you still need to process them.
    """
    WORKERS_NUMBER = 4

    @staticmethod
    def map_fn(data_source):
        for key, val in data_source:
            yield key, val**2

    @staticmethod
    def reduce_fn(data_source):
        reduced_value = sum(val for _, val in data_source)
        return 0, reduced_value


class IntegrationProducer(Producer):
    """"Producer for the task of integration of function
    For this particular task the keys are not really needed, but you still need to process them.
    """
    WORKERS_NUMBER = 3

    @staticmethod
    def map_fn(data_source):
        func, dx = data_source.func, data_source.dx
        for key, val in data_source:
            yield key, func(val)*dx

    @staticmethod
    def reduce_fn(data_source):
        reduced_value = sum(val for _, val in data_source)
        return 0, reduced_value


def func(x):
    """Function to integrate"""
    from math import exp
    return exp(x)


class IntegrationDataSource(DataSource):
    """"Data source for the task of integration of function
    In this case it is integral from 1 to 10 of exp(x),
    so you can easily check the answer.
    """

    interval = (1, 10)
    dx = 0.000001

    def func(self, x):
        return func(x)

    @classmethod
    def full_length(cls):
        return int((cls.interval[1] - cls.interval[0]) / cls.dx)

    def __iter__(self):
        """Returns sequence of x values on interval"""
        return self.add_default_keys(
            (self.interval[0] + x*self.dx for x in range(self.offset, self.offset + self.limit))
        )


class SimpleDataSource(DataSource):
    """Data source for the task of sum of squares of values"""
    N = 10**7

    def __init__(self, **kwargs):
        DataSource.__init__(self, **kwargs)

    @classmethod
    def full_length(cls):
        return cls.N

    def __iter__(self):
        return self.add_default_keys(range(self.offset, self.offset + self.limit))


class SimpleListSource(ListDataSource):
    """Data source for the task of sum of squares of values.
    Illustrates work with ListDataSource
    """
    data = range(10**7)


class SimpleDictSource(DictDataSource):
    """Data source for the task of sum of squares of values.
    Illustrates work with DictDataSource
    """
    data = {
        key: value
        for key, value in enumerate(range(10**7))
    }


class SimpleSQLSource(SQLDataSource):
    """Data source for the task of sum of squares of values.
    Illustrates work with SimpleSQLSource.
    For the task of sum of squares of values the table is supposed to be filled like:

    CREATE TABLE IF NOT EXISTS examples (
        id INTEGER,
        value INTEGER,
        PRIMARY KEY(id)
    );

    import sqlalchemy
    engine = sqlalchemy.create_engine(conf, echo=True)
    INSERT_DATA = sqlalchemy.sql.text("INSERT INTO examples(id, value) VALUES (:id, :value);")
    engine.execute(INSERT_DATA, [
        {
            "id": i,
            "value": i
        }
        for i in range(10**7)
    ])

    or any values you want.
    If you use SQLite, make sure that your have access to the file of database.

    Be careful: if you use "localhost" in configuration, your workers will access local database
    on their host, not on the host of producer! If it is not what you want, use external IP-address.
    """

    CONF = 'sqlite:///exampledb'
    TABLE = "examples"
    KEY = "id"
    VALUE = "value"


if __name__ == "__main__":
    logging.basicConfig(logging=logging.INFO,
                            format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s")
    logging.getLogger("").setLevel(logging.DEBUG)

    #Set this variable depending on what example you want to run
    #Each task below requires definition of producer and factory
    task = "integration"   #alternatives: "integration", "squared_sum"

    producer, factory = None, None

    """
    It is for the task of integration of function
    To run, set "task" variable to "integration".
    To run corresponding workers:
    worker.py -f ./examples.py -s IntegrationDataSource -p IntegrationProducer -q 127.0.0.1 -w 4
    """
    if task == "integration":
        producer = IntegrationProducer()
        factory = DataSourceFactory(data_source_class=IntegrationDataSource)


    """
    It is for the task of sum of squares of values.
    To run, set "task" variable to "squared_sum"
    To run corresponding workers:
    worker.py -f ./examples.py -s CLASSNAME -p SimpleProducer -q 127.0.0.1 -w 4
    with SimpleDataSource, SimpleListSource, SimpleDictSource or SimpleSQLSource instead of CLASSNAME
    """
    if task == "squared_sum":
        producer = SimpleProducer()
        source = "sql"     #alternatives: "data", "list", "dict" or "sql"
        if source == "data":
            factory = DataSourceFactory(data_source_class=SimpleDataSource)
        elif source == "list":
            factory = DataSourceFactory(data_source_class=SimpleListSource)
        elif source == "dict":
            factory = DataSourceFactory(data_source_class=SimpleDictSource)
        elif source == "sql":
            #Requires corresponding data base
            factory = DataSourceFactory(data_source_class=SimpleSQLSource)

    value = producer.map(factory)
    print "Answer: ", value[1]