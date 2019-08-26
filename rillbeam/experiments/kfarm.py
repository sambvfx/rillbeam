import os
import time
import logging
import subprocess
import json

from termcolor import cprint, colored

import kafka
import kafka.admin
import kafka.errors

import apache_beam as beam
from apache_beam.io.external.kafka import ReadFromKafka, WriteToKafka
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.runner import PipelineResult

from rillbeam.transforms import Log, JobOutput, JobAggregateLevel
import rillbeam.data.farm

from typing import *


TOPIC = 'beam-kfarm'


def main(pipeline_options, args):



    farm_kw = (
        ('graphs', 1),
        ('jobs', 2),
        ('tasks', 3),
        ('outputs', 4),
    )

    pipe = beam.Pipeline(options=pipeline_options)

    feed = (
        pipe
        | 'KafkaInflow' >> ReadFromKafka(
              consumer_config={
                  'bootstrap.servers': 'localhost:9092',
              },
              topics=[TOPIC],
              key_deserializer='org.apache.kafka.common.serialization.ByteArrayDeserializer',
              value_deserializer='org.apache.kafka.common.serialization.ByteArrayDeserializer',
              expansion_service='localhost:8097')
        | 'RawFeed' >> Log(color=('white', ['dark']))
    )

    # (
    #     feed
    #     | JobAggregateLevel.TASK >> JobOutput(JobAggregateLevel.TASK)
    #     | 'PerTask' >> Log(color=('yellow', ['bold']))
    # )
    #
    # (
    #     feed
    #     | JobAggregateLevel.JOB >> JobOutput(JobAggregateLevel.JOB)
    #     | 'PerJob' >> Log(color=('blue', ['bold']))
    # )
    #
    # (
    #     feed
    #     | JobAggregateLevel.GRAPH >> JobOutput(JobAggregateLevel.GRAPH)
    #     | 'PerGraph' >> Log(color=('green', ['bold']))
    # )

    result = pipe.run()  # type: PipelineResult
    time.sleep(10)
    while result.state != PipelineState.RUNNING:
        time.sleep(10)

    print
    cprint('Starting streaming graph forever. Kill with ctrl+c',
           'red', attrs=['bold'])
    print

    cprint('Generating farm jobs:', 'yellow')
    for k, v in farm_kw:
        print '  {}={}'.format(k, colored(repr(v), 'white', attrs=['bold']))
    print

    admin = kafka.admin.KafkaAdminClient(
        # bootstrap_servers=['localhost:9092'],
    )
    try:
        admin.create_topics([kafka.admin.NewTopic(TOPIC, 1, 1)])
    except kafka.errors.TopicAlreadyExistsError:
        pass

    # producer = kafka.KafkaProducer(
    #     # bootstrap_servers=['localhost:9092'],
    # )
    #
    # for i, payload in enumerate(rillbeam.data.farm.gen_farm_messages(**dict(farm_kw))):
    #     print payload
    #     producer.send('beam-kfarm', 'foo')

    try:
        result.wait_until_finish()
    except KeyboardInterrupt:
        print
        cprint('Shutting down...', 'yellow')
        result.cancel()


def send(pipeline_options):

    with beam.Pipeline(options=pipeline_options) as pipe:
        (
            pipe
            | beam.Impulse()
            | beam.Map(lambda x: (1, x))
            | Log()
            # | 'KafkaWrite' >> WriteToKafka(
            #       producer_config={
            #           'bootstrap.servers': 'localhost:9092',
            #       },
            #       topic=TOPIC,
            #       # key_serializer='org.apache.kafka.common.serialization.ByteArraySerializer',
            #       # value_serializer='org.apache.kafka.common.serialization.ByteArraySerializer',
            #       expansion_service='localhost:8097',
            #   )
        )


if __name__ == '__main__':
    from rillbeam.helpers import get_parser, get_options

    parser = get_parser()
    parser.add_argument('--send', action='store_true')

    logging.getLogger().setLevel(logging.INFO)

    pipeline_args, known_args = get_options(
        __name__,
        parser,
        'streaming',
    )

    if known_args.send:
        send(pipeline_args)
    else:
        main(pipeline_args, known_args)
