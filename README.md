
Pymar
=====

Pymar is a tool for fast and easy creation of distributed map-reduce systems in Python.

Its primary goals are to create very easy way to distribute your calculations in Python and
to separate logic of your distributed programs from the way they store their data as far as possible.

Tools for simplification of working with various kinds of databases will be provided as plugins. (Otherwise, there would be too many unnecessary dependencies).

Version:
-------
0.1

Requirements:
-------------
* Python 2.7
* pika
* Working AMQP-server (for example, RabbitMQ)


Using
----
As a Quick Start Guide see /examples. Everything is explained in details.

For example, you need to calculate the integral of given function on given interval.

```python
#this_example.py
from pymar.datasource import DataSource, DataSourceFactory
from pymar.producer import Producer

def func(x):
    """Function to integrate"""
    from math import exp
    return exp(x)


class IntegrationProducer(Producer):
    """"Producer for the task of integration of function.
    """
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
    dx = 0.000001

    @classmethod
    def full_length(cls):
        return int((cls.interval[1] - cls.interval[0]) / cls.dx)

    def __iter__(self):
        """Returns sequence of x values on interval"""
        return (self.interval[0] + x*self.dx for x in range(self.offset, self.offset + self.limit))

producer = IntegrationProducer()
factory = DataSourceFactory(IntegrationDataSource)
value = producer.map(factory)
print "Answer: ", value
```

Run workers using command like:
```
worker.py -f ./this_example.py -s IntegrationDataSource -p IntegrationProducer -q 127.0.0.1 -w 4
```

And run your script.
```
python this_example.py
```

Thus, there is only trivial code without much knowing about distribution.
