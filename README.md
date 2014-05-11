Pymar is a tool for fast and easy creation of distributed map-reduce systems in Python.

Its primary goals are to create very easy way to distribute your calculations in Python and
to separate logic of your distributed programs from the way they store their data for as far as possible.

As a Quick Start Guide see examples.py. Everything is explained in details.

For example, you need calculate integral of given function on given interval.
File: this_example.py

```python
from pymar.datasource import DataSource, DataSourceFactory
from pymar.producer import Producer

def func(x):
    """Function to integrate"""
    from math import exp
    return exp(x)

class IntegrationProducer(Producer):
    """"Producer for the task of integration of function
    For this particular task the keys are not really needed, but you still need to process them.
    """
    WORKERS_NUMBER = 4

    @staticmethod
    def map_fn(data_source):
        func, dx = data_source.func, data_source.dx
        for key, val in data_source:
            yield key, func(val)*dx

    @staticmethod
    def reduce_fn(data_source):
        reduced_value = sum(val for _, val in data_source)
        return 0, reduced_value


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

producer = IntegrationProducer()
factory = DataSourceFactory(data_source_class=IntegrationDataSource)
value = producer.map(factory)
print "Answer: ", value[0]
'''

Run workers using command like:
worker.py -f ./this_example.py -s IntegrationDataSource -p IntegrationProducer -q 127.0.0.1 -w 4

And run your script.
python this_example.py

Thus, there is only trivial code without much knowing about distribution.

Requirements:
Python 2.7
Working AMQP-server (for example, RabbitMQ)
