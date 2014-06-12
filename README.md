
Pymar
=====

Pymar is a tool for fast and easy scaling your calculations in Python.

Although it uses terms "map" and "reduce", it is NOT an exact implementation of map-reduce.

Its primary goals are:
* to create very easy way to scale your calculations in Python
* to minimize data transmission between producer and workers
* to separate logic of your distributed programs from the way they store their data as far as possible.

Works effectively even if your data are represented as lists instead of key-value pairs, which is usually the case in mathematical tasks.

Does not require special files with functions map and reduce, so may be easily used without significant changes in the architecture of the existing code.

Tools for simplification of working with various kinds of databases will be provided as plugins. (Otherwise, there would be too many unnecessary dependencies).

Version:
-------
0.1

Requirements:
-------------
* Python 2.7
* [pika](https://pypi.python.org/pypi/pika)
* Working AMQP-server (for example, [RabbitMQ](http://www.rabbitmq.com/))


Using
----

To install:
```
pip install pymar
```

For example, you need to calculate the integral of given function on given interval.

```python
#this_example.py
from pymar.datasource import DataSource, DataSourceFactory
from pymar.producer import Producer

def func(x):
    """Function to integrate"""
    from math import exp
    return exp(x)
```

Firstly, define the class with static methods map_fn and reduce_fn. This class must be a subclass of pymar.producer.Producer.
Variable WORKERS_NUMBER is a number of parts to which you want to divide your task.
In most cases it is optimal if WORKERS_NUMBER is equal to the actual number of workers you run.
The task will be performed correctly no matter what WORKERS_NUMBER is (of course, it must be positive integer).

```python
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
```

Secondly, we need a class which will be used by workers to get required data (if you don't want to waste time transmitting a lot of data from producer to workers). It must be a subclass of pymar.datasource.DataSource.
If you already have all the data in memory, you may not define this class and just create object of DataSourceFactory with you data object as argument.
(See [the example](https://github.com/alexgorin/pymar/blob/master/examples/squaredsum2.py) )

Your data source must be an iterable object, so we redefine function "\_\_iter\_\_". In this case data is a sequence of x values on the interval. The corresponding part of the sequence will be created on each worker, not on producer.
Also, you will need to define class method "full\_length" in order for Pymar to be able to effectively divide your task among workers.
Variables self.offset and self.limit are used by workers to get only their own part of data. You don't have to assign anything to this variables by yourself,
but use them in "\_\_iter\_\_" function if you want to use more than one worker (which is usually true).

In many cases this process may be simplified if you use subclasses of DataSource defined in additions
(for example [PymarSQL](https://github.com/alexgorin/PymarSQL) and [PymarMongo](https://github.com/alexgorin/PymarMongo)).

```python
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
```

In the end, create objects of producer and data source factory, and call method "map" of the producer with the factory as argument.

```python
producer = IntegrationProducer()
factory = DataSourceFactory(IntegrationDataSource)
value = producer.map(factory)
print "Answer: ", value
```

Run workers using command like:
```
worker.py ./this_example.py -s IntegrationDataSource -p IntegrationProducer -q 127.0.0.1 -w 4
```
4 workers are created in this case. You may add new workers whenever you want, even if your producer is already running.

And run your script.
```
python this_example.py
```

Thus, there is only trivial code without much knowing about distribution.

All the code of this example:

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

This file may contain any other logic you want, so you may use Pymar along with the existing modules.
The call "producer.map" may be in other file than the definitions of Producer and DataSource subclasses.
The only restriction - Producer and DataSource subclasses must be defined in the same file.

For more examples, see [examples](https://github.com/alexgorin/pymar/tree/master/examples)

If you want a canonical example with word counting, you can find it in [PymarMongo](https://github.com/alexgorin/PymarMongo) addition.