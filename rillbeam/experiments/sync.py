"""
Split/Synced streams
"""
import time
import logging

from termcolor import cprint

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.runner import PipelineResult

from rillbeam.transforms import Log, Sync


# Google pubsub topics and subscriptions
INPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-inflow'
OUTPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-outflow'
SUBSCRIPTION_PATH = 'projects/dataflow-241218/subscriptions/manual'


class SlowStr(beam.DoFn):
    """
    Test component thata cast to a string slowly... Emulates something
    that takes a small variable amount of processing time.
    """

    def process(self, element, duration=0.5, variation=None, **kwargs):
        import time
        import random
        if variation:
            duration += random.uniform(*variation)
        time.sleep(duration)
        yield str(element)


def main(options):
    from rillbeam.helpers import pubsub_interface

    pipe = beam.Pipeline(options=options)

    graph = (
        pipe
        | 'PubSubInflow' >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC)
    )

    b1 = (
        graph
        | 'AsInt' >> beam.Map(lambda x: int(x))
        | 'LogInt' >> Log()
    )

    b2 = (
        graph
        | 'AsStr' >> beam.Map(lambda x: str(x))
        | 'LogStr' >> Log()
    )

    synced = (
        (b1, b2)
        | Sync()
        | 'ToBytes' >> beam.Map(lambda x: bytes(x))
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

    main(pipeline_args)
