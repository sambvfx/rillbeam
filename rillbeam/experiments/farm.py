import os
import time
import logging
import json

from termcolor import cprint, colored

from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound

import apache_beam as beam
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.runner import PipelineResult

from rillbeam.transforms import Log, JobOutput, JobAggregateLevel, FilterPubsubMessages
import rillbeam.data.farm

from typing import *


# Google pubsub topics and subscriptions
INPUT_TOPIC = 'projects/render-1373/topics/farm-output'


# Credentials for internal project
if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') is None:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        '.cred',
        'render-pubsub.json'
    ))


def _make_topic(project_id, name):
    # How to create a new topic

    publisher = pubsub_v1.PublisherClient()

    # type: Iterator[google.cloud.pubsub_v1.types.Topic]
    it = publisher.list_topics('projects/{}'.format(project_id))
    print 'existing:'
    print list(it)

    publisher.create_topic(publisher.topic_path(project_id, name))


def get_subscription(topic_path, subscription_name):
    import google.auth
    _, project_id = google.auth.default()

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    # print list(subscriber.list_subscriptions(subscriber.project_path(project_id)))
    try:
        subscriber.get_subscription(subscription_path)
    except NotFound:
        subscriber.create_subscription(subscription_path, topic_path)

    return subscription_path


def main(pipeline_options, args):

    farm_kw = (
        ('graphs', 1),
        ('jobs', 2),
        ('tasks', 3),
        ('outputs', 4),
    )

    pipe = beam.Pipeline(options=pipeline_options)

    subscription = get_subscription(INPUT_TOPIC, args.subscription)

    feed = (
        pipe
        | 'PubSubInflow' >> beam.io.ReadFromPubSub(subscription=subscription,
                                                   with_attributes=True)
        | 'FilterPubsubMessage' >> FilterPubsubMessages(filters={'tags': __name__})
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
        publisher.publish(INPUT_TOPIC, data=bytes(json.dumps(payload)), tags=__name__)

    try:
        result.wait_until_finish()
    except KeyboardInterrupt:
        print
        cprint('Shutting down...', 'yellow')
        result.cancel()


if __name__ == '__main__':
    from rillbeam.helpers import get_options

    logging.getLogger().setLevel(logging.INFO)

    pipeline_args, known_args = get_options(
        __name__,
        None,
        'streaming'
    )

    main(pipeline_args, known_args)
