#!/usr/bin/env python
# -*- coding: utf-8 -*-

import inspect


class DataSource(object):
    """Base class for all data sources in your programs.
    Object of data source provides a part of data according to the variables "limit" and "offset".
    (Although, you will not have to set this variables by yourself.)
    You can override __init__, __iter__ and full_length functions (don't forget @classmethod decorator for full_length).
    full_length must return the length of your data set independent from offset and limit.
    __iter__ must return a generator of sequence of elements according to offset and limit variables.

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

    #Redefine in subclass
    def __iter__(self):
        raise NotImplementedError()

    #Redefine in subclass
    @classmethod
    def full_length(cls):
        raise NotImplementedError()


#Part of an iterable object
def get_part(data, limit, offset):
    for index, value in enumerate(data):
        if index < offset:
            continue
        if index >= offset + limit:
            break
        yield value


class DataSourceFactory(object):
    """Creates data source with build_data_source method.
    Pymar was designed to minimize the data flow between producer and workers,
    Producer sends to workers an object of DataSourceFactory, and data sources are created on workers.
    """

    def __init__(self, data, limit=0, offset=0):
        if inspect.isclass(data):
            self.init_from_data_source(data, limit, offset)
        else:
            #Special case for already existing data - for optimization
            self.init_from_data(data)

    def init_from_data_source(self, data_source_class, limit=0, offset=0):
        assert issubclass(data_source_class, DataSource), "data_source_class must be a subclass of DataSource"
        try:
            self.limit = limit or data_source_class.full_length()
        except NotImplementedError:
            raise Exception("Impossible to determine the length of data set. Cannot create factory.")

        self.offset = offset
        self.data_source_class = data_source_class
        self.data = None

    def init_from_data(self, data):
        assert len(data), "Data set must not be empty"
        self.data = data
        self.limit = len(data)
        self.offset = 0

    def part(self, limit, offset):
        if self.data:
            data = list(get_part(self.data, limit, offset))
            new_factory = DataSourceFactory(data)
            return new_factory

        new_factory = DataSourceFactory(self.data_source_class, limit, offset)
        return new_factory

    def length(self):
        return self.limit

    def __str__(self):
        if self.data:
            return "%s: list of %d elements" % (self.__class__.__name__, len(self.data))
        return "%s: Limit: %d, offset: %d" % (self.__class__.__name__, self.limit, self.offset)

    def build_data_source(self):
        #If data is already set, just return it
        if self.data:
            return self.data

        if self.data_source_class:
            return self.data_source_class(limit=self.limit, offset=self.offset)

        raise NotImplementedError()