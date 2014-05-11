#!/usr/bin/env python
# -*- coding: utf-8 -*-


class DataSource(object):
    """Base class for all data sources in your programs.
    Object of data source provides a part of data according to the variables "limit" and "offset".
    (Although, you will not have to set this variables by yourself.)
    You can override __init__, __iter__ and full_length functions (don't forget @classmethod decorator for full_length).
    full_length must return the length your data set independent from offset and limit.
    __iter__ must return a generator of key-value pairs according to offset and limit variables.
    If for your task you only need a sequence of values, without keys, use add_default_keys method
    to generate key-value pairs.

    For most tasks you may need to inherit from one of the children of this class rather than this class itself.
    """
    def __init__(self, offset=0, limit=0):
        self.offset = offset
        self.limit = limit

    def set_offset(self, offset):
        self.offset = offset

    def set_limit(self, limit):
        self.limit = limit

    def __len__(self):
        self.limit

    def __iter__(self):
        raise NotImplementedError()

    @classmethod
    def full_length(cls):
        raise NotImplementedError()

    @staticmethod
    def add_default_keys(data_list):
        for val in data_list:
            yield 0, val


class ListDataSource(DataSource):
    """Data source based on already existing list.
    In your class you just need to set data variable.
    May be not suitable for large data sets, but very easy to use.
    """
    data = []

    def __iter__(self):
        return self.add_default_keys(self.data[self.offset: self.offset + self.limit])
        #return ((index, value) for index, value in enumerate(self.data[self.offset: self.offset + self.limit]))


    @classmethod
    def full_length(cls):
        return len(cls.data)


class DictDataSource(DataSource):
    """Data source based on already existing dictionary.
    In your class you just need to set data variable.
    May be not suitable for large data sets, but very easy to use.
    """
    data = []

    def __iter__(self):
        for index, (key, value) in enumerate(self.data.iteritems()):
            if index < self.offset:
                continue
            if index >= self.offset + self.limit:
                break
            yield key, value

    @classmethod
    def full_length(cls):
        return len(cls.data)


class SQLDataSource(DataSource):
    """Data source based on a table in data base.
    Your data are supposed to be in two columns of given table.
    To use, inherit from this class and set CONF, TABLE, KEY and VALUE.
    Key and value - names of columns with keys and values respectively.

    Be careful: if you use "localhost" in configuration, your workers will access local database
    on their host, not on the host of producer! If it is not what you want, use external IP-address.
    """

    REQUEST_TEMPLATE = """
    SELECT %s as key, %s as value FROM %s
    LIMIT %d
    OFFSET %d;
    """

    COUNT_TEMPLATE = """
    SELECT count(*) FROM %s;
    """

    CONF = "postgresql://login:password@127.0.0.1/exampledb"
    TABLE = "example_table"
    KEY = "id"
    VALUE = "data"

    def __init__(self, **kwargs):
        DataSource.__init__(self, **kwargs)
        import sqlalchemy
        self.engine = sqlalchemy.create_engine(self.CONF, echo=True)
        self.cursor = self.engine.execute(self.REQUEST_TEMPLATE %
                                          (self.KEY, self.VALUE, self.TABLE, self.limit, self.offset))

    def __iter__(self):
        return self._get_cursor_data(self.cursor)

    @staticmethod
    def _get_cursor_data(cursor):
        while True:
            rows = cursor.fetchmany()
            if not rows:
                return
            for row in rows:
                yield row

    @classmethod
    def full_length(cls):
        import sqlalchemy
        engine = sqlalchemy.create_engine(cls.CONF, echo=True)
        cursor = engine.execute(cls.COUNT_TEMPLATE % (cls.TABLE,))
        return cursor.scalar()


class DataSourceFactory(object):
    """Creates data source with build_data_source method.
    Pymar was designed to minimize the data flow between producer and workers,
    Producer sends to workers an object of DataSourceFactory, and data sources are created on workers.
    """
    def __init__(self, data_source_class, limit=0, offset=0):
        self.limit = limit
        if not self.limit:
            try:
                self.limit = data_source_class.full_length()
            except NotImplementedError:
                raise Exception("Impossible to determine the length of data set. Cannot create factory.")

        self.offset = offset
        self.data_source_class = data_source_class

    def length(self):
        return self.limit

    def __str__(self):
        return "%s: Limit: %d, offset: %d" % (self.__class__.__name__, self.limit, self.offset)

    def build_data_source(self):
        if self.data_source_class:
            return self.data_source_class(limit=self.limit, offset=self.offset)
        raise NotImplementedError()