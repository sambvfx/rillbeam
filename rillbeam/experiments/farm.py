import time
import logging

from termcolor import cprint

import apache_beam as beam
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.runner import PipelineResult

from apache_beam.typehints import *

# Google pubsub topics and subscriptions
INPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-inflow'
OUTPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-outflow'
SUBSCRIPTION_PATH = 'projects/dataflow-241218/subscriptions/manual'


def main_without_pubsub(options):
    from rillbeam.transforms import FauxFarm, Log

    pipe = beam.Pipeline(options=options)

    (
        pipe
        | beam.Create(['foo bar', 'spangle'])
        | 'Split' >> beam.Map(lambda x: [s.strip() for s in x.split()])
        | 'Farm' >> FauxFarm()
        | beam.Map(lambda x: bytes(x))
        | Log()
    )

    result = pipe.run()  # type: PipelineResult
    result.wait_until_finish()


def main_with_pubsub(options):
    from rillbeam.helpers import pubsub_interface
    from rillbeam.transforms import FauxFarm

    pipe = beam.Pipeline(options=options)

    (
        pipe
        | 'PubSubInflow' >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC)
        | 'Split' >> beam.Map(lambda x: [s.strip() for s in x.split()])
        | 'Farm' >> FauxFarm()
        | beam.Map(lambda x: bytes(x))
        | 'PubSubOutflow' >> beam.io.WriteToPubSub(OUTPUT_TOPIC)
    )

    print
    cprint('Starting pipeline...', 'yellow', attrs=['bold'])
    result = pipe.run()  # type: PipelineResult
    time.sleep(5)
    while result.state != PipelineState.RUNNING:
        time.sleep(10)

    def callback(pane, message):
        # type: (tapp.Pane, Message) -> None
        message.ack()
        with pane.batch:
            pane.write(message.data.decode(), 'cyan')

    try:
        pubsub_interface(SUBSCRIPTION_PATH, INPUT_TOPIC, callback=callback)
    finally:
        print
        cprint('Shutting down pipeline...', 'yellow', attrs=['bold'])
        result.cancel()
        print


if __name__ == '__main__':
    from rillbeam.helpers import get_options

    logging.getLogger().setLevel(logging.INFO)

    pipeline_args, _ = get_options(
        __name__,
        None,
        'streaming'
    )

    main_with_pubsub(pipeline_args)
