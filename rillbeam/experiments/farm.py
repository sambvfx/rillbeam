import os
import time
import logging
import json

from termcolor import cprint, colored

from google.cloud import pubsub_v1

import apache_beam as beam
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.runner import PipelineResult

from rillbeam.transforms import Log, JobOutput, JobAggregateLevel
import rillbeam.data.farm

from typing import *


# Credentials for internal project
if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') is None:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.expanduser(
        # '~/projects/luma/.cred/dataflow-d3c95049758e.json'
        '~/projects/luma/.cred/render-pubsub.json'
    )


def _make_topic(project_id, name):
    # How to create a new topic

    publisher = pubsub_v1.PublisherClient()

    # type: Iterator[google.cloud.pubsub_v1.types.Topic]
    it = publisher.list_topics('projects/{}'.format(project_id))
    print 'existing:'
    print list(it)

    publisher.create_topic(publisher.topic_path(project_id, name))


# Google pubsub topics and subscriptions
INPUT_TOPIC = 'projects/render-1373/topics/farm-output'


def main(options):

    farm_kw = (
        ('source', __name__),
        ('graphs', 1),
        ('jobs', 2),
        ('tasks', 3),
        ('outputs', 4),
    )

    pipe = beam.Pipeline(options=options)

    feed = (
        pipe
        | 'PubSubInflow' >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC)
        | 'Decode' >> beam.Map(lambda x: json.loads(x.decode()))
        | 'Filter' >> beam.Filter(lambda x: x['source'] == __name__)
        | 'RawFeed' >> Log(color=('white', ['dark']))
    )

    (
        feed
        | JobAggregateLevel.TASK >> JobOutput(JobAggregateLevel.TASK)
        | 'PerTask' >> Log(color=('yellow', ['bold']))
    )

    (
        feed
        | JobAggregateLevel.JOB >> JobOutput(JobAggregateLevel.JOB)
        | 'PerJob' >> Log(color=('blue', ['bold']))
    )

    (
        feed
        | JobAggregateLevel.GRAPH >> JobOutput(JobAggregateLevel.GRAPH)
        | 'PerGraph' >> Log(color=('green', ['bold']))
    )

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

    publisher = pubsub_v1.PublisherClient()
    for payload in rillbeam.data.farm.gen_farm_messages(**dict(farm_kw)):
        publisher.publish(INPUT_TOPIC, data=bytes(json.dumps(payload)))

    try:
        result.wait_until_finish()
    except KeyboardInterrupt:
        print
        cprint('Shutting down...', 'yellow')
        result.cancel()


if __name__ == '__main__':
    from rillbeam.helpers import get_options

    logging.getLogger().setLevel(logging.INFO)

    pipeline_args, _ = get_options(
        __name__,
        None,
        'streaming'
    )

    main(pipeline_args)
