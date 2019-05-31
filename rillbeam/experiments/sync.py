"""
Split/Synced streams
"""
import time

import apache_beam as beam
import apache_beam.transforms.window as window

from rillbeam.transforms import Log, Sync


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

    with beam.Pipeline(options=options) as pipe:
        graph = (
            pipe
            | beam.Create(range(500))
        )

        b1 = (
            graph
            | 'Map1' >> beam.Map(lambda x: window.TimestampedValue(x, time.time()))
            | 'Window1' >> beam.WindowInto(window.FixedWindows(5))
            | 'Log1' >> Log()
        )

        b2 = (
            graph
            | '"Process"' >> beam.ParDo(SlowStr(), duration=0.1, variation=(0.0, 1.1))
            | 'Map2' >> beam.Map(lambda x: window.TimestampedValue(x, time.time()))
            | 'Window2' >> beam.WindowInto(window.FixedWindows(5))
            | 'Log2' >> Log()
        )

        synced = (
            (b1, b2)
            | Sync()
            | 'Success' >> Log()
        )


if __name__ == '__main__':
    import argparse
    from rillbeam.helpers import get_options
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_args = get_options(pipeline_args)
    main(pipeline_args)
