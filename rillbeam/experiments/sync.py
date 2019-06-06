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


def main_without_pubsub(options):
    from rillbeam.transforms import SleepFn

    with beam.Pipeline(options=options) as pipe:

        # FIXME: still can't "fake" timestamp data like we get from pubsub...
        graph = (
            pipe
            | 'start' >> beam.Create([(k, k) for k in range(5)])
            # The purpose of the WindowInto transform is to establish a
            # FixedWindows windowing function for the PCollection.
            # It does not bucket elements into windows since the timestamps
            # from Create are not spaced 5 ms apart and very likely they all
            # fall into the same window.
            | 'w' >> beam.WindowInto(window.FixedWindows(5))
            # Generate timestamped values using the values as timestamps.
            # Now there are values 5 ms apart and since Map propagates the
            # windowing function from input to output the output PCollection
            # will have elements falling into different 5ms windows.
            | beam.Map(lambda x_t2: window.TimestampedValue(x_t2[0], x_t2[1]))
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

        b3 = (
            b1
            | 'Sleep' >> beam.ParDo(SleepFn(), duration=0.2)
            | 'AsFloat' >> beam.Map(lambda x: float(x))
            | 'LogFloat' >> Log()
        )

        (
            (b1, b2, b3)
            | Sync()
            | 'SyncLog' >> Log()
        )


def main_with_pubsub(options):
    from rillbeam.helpers import pubsub_interface
    from rillbeam.transforms import SleepFn

    pipe = beam.Pipeline(options=options)

    graph = (
        pipe
        | 'PubSubInflow' >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC)
        | 'Split' >> beam.FlatMap(lambda x: [s.strip() for s in x.split()])
        | 'Key' >> beam.Map(lambda x: (int(x), x))
    )

    b1 = (
        graph
        | 'AsInt' >> beam.Map(lambda kv: (kv[0], int(kv[1])))
        | 'LogInt' >> Log()
    )

    b2 = (
        graph
        | 'AsStr' >> beam.Map(lambda kv: (kv[0], str(kv[1])))
        | 'LogStr' >> Log()
    )

    b3 = (
        b1
        | 'Sleep' >> beam.ParDo(SleepFn(), duration=0.1)
        | 'AsFloat' >> beam.Map(lambda kv: (kv[0], float(kv[1])))
        | 'LogFloat' >> Log()
    )

    b4 = (
        b3
        | 'AsRange' >> beam.Map(lambda kv: (kv[0], range(int(kv[1]))))
        | 'LogRange' >> Log()
    )

    (
        (b1, b2, b3, b4)
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

    main_with_pubsub(pipeline_args)
