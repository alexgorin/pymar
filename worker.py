#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cPickle as pickle
import imp
import logging
import multiprocessing as mp
import os
import pika


class Worker(object):
    """Represents the worker which executes its part of the task
    Connects to MQ server (author used RabbitMQ, but any server supporting AMQP will probably be fine)
    and listens for messages from producer.
    Message is a factory which can create data source.
    When it receives a message, it creates a data source using the factory, executes map_fn and reduce_fn
    of producer_class on the data and sends response with the result.

    If purge_queue is set to True, workers will remove all messages in the queue on connect,
    to avoid repeating errors.
    """
    def __init__(self, producer_class, index, mq_server="localhost", purge_queue=False):
        self.producer_class = producer_class
        self.logging = logging.getLogger("Worker %d for %s" % (index, producer_class.__name__))

        #Connect to MQ sever and listen the corresponding queue
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=mq_server))
        channel = connection.channel()
        channel.queue_declare(queue=self.producer_class.routing_key())
        if purge_queue:
            channel.queue_purge(queue=self.producer_class.routing_key())

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(self.on_request, queue=self.producer_class.routing_key())
        self.channel = channel

    def on_request(self, ch, method, props, body):
        self.logging.info("\nMessage received")
        try:
            data_source_factory = pickle.loads(body)
        except Exception as e:
            self.logging.critical("Cannot read data from the message.")
            self.logging.critical(e)
            return

        try:
            data_source = data_source_factory.build_data_source()
        except Exception as e:
            self.logging.critical("Cannot create data source: ")
            self.logging.critical(e)
            return

        self.logging.info("Calculating...")
        response = self.producer_class.reduce_fn(
                        self.producer_class.map_fn(data_source)
                    )

        self.logging.info("Calculating finished. ")
        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id=\
                                                         props.correlation_id),
                         body=pickle.dumps(response))

        ch.basic_ack(delivery_tag=method.delivery_tag)
        self.logging.info("Message acknowledged.")

    def listen(self):
        self.logging.info("Awaiting requests")
        self.channel.start_consuming()


def parse_options():
    from optparse import OptionParser
    option_parser = OptionParser(usage="%prog FILENAME [options]")
    option_parser.add_option("-s", "--data_source", dest="data_source",
                             help="Name of class of data source")

    option_parser.add_option("-p", "--producer", dest="producer",
                             help="Name of class with functions Map and Reduce")

    option_parser.add_option("-q", "--mq_server", dest="mq_server", default="localhost",
                             help="Address of MQ-server")

    option_parser.add_option("-w", "--workers_number", dest="workers_number", type="int",
                             help="Number of workers to run")

    option_parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                             help="Show logs of info level.'")

    option_parser.add_option("-r", "--purge_queue", action="store_true", dest="purge_queue", default=False,
                    help="Remove all existing messages in the corresponding queue. May be useful to correct errors.'")

    (options, args) = option_parser.parse_args()

    if len(args) != 1:
        option_parser.error("Invalid usage")

    if not options.producer:
        option_parser.error("Name of class of producer is not specified.")

    if not options.workers_number:
        option_parser.error("Number of workers is not specified.")

    options.file = args[0]
    return options


def run_process(index, producer, mq_server, purge_queue):
    worker = Worker(producer, index, mq_server=mq_server, purge_queue=purge_queue)
    worker.listen()


def run(options):
    if options.verbose:
        logging.basicConfig(logging=logging.DEBUG,
                            format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s")
        logging.getLogger("").setLevel(logging.INFO)
    else:
        logging.basicConfig(logging=logging.INFO,
                            format="[%(levelname)s] [%(name)s] %(message)s")
        logging.getLogger("").setLevel(logging.CRITICAL)

    #Dynamically load the file with the descriptions of producer and data source.
    path = os.path.dirname(options.file)
    module_name = os.path.splitext(os.path.basename(options.file))[0]
    fp, pathname, description = imp.find_module(module_name, [path])
    try:
        module = imp.load_module(options.file, fp, pathname, description)
        if options.data_source:
            globals().update({options.data_source: getattr(module, options.data_source)})
        producer = getattr(module, options.producer)
        #Run given number of workers.
        for index in range(options.workers_number):
            mp.Process(target=run_process, args=(index, producer, options.mq_server, options.purge_queue)).start()

        logging.getLogger("").info("%d workers running." % options.workers_number)
    finally:
        if fp:
            fp.close()


if __name__ == "__main__":
    """Example of launch:
    worker.py ./examples.py -s IntegrationDataSource -p IntegrationProducer -q 127.0.0.1 -w 4

    If you are going to send your data without using DataSource subclasses, don't specify it:
    worker.py ./examples.py -p SimpleProducer -q 127.0.0.1 -w 4
    """
    options = parse_options()
    run(options)
