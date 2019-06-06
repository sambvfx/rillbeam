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


# Google pubsub topics and subscriptions
INPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-inflow'
OUTPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-outflow'
SUBSCRIPTION_PATH = 'projects/dataflow-241218/subscriptions/manual'


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
