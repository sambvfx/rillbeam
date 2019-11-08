from __future__ import print_function
import sys
import time

import apache_beam as beam


class Log(beam.PTransform):

    COLORS = {
        'white': '',
        'purple': '\033[95m',
        'cyan': '\033[96m',
        'blue': '\033[94m',
        'green': '\033[92m',
        'yellow': '\033[93m',
        'red': '\033[91m',
    }

    def __init__(self, color='white', **kwargs):
        super(Log, self).__init__(**kwargs)
        self.color = color

    def expand(self, pcoll):

        def _iter(element, name):
            print('{} : {!r}'.format(name, element))
            yield element

        name = '\033[1m{}{}\033[0m'.format(self.COLORS[self.color], self.label)

        return (
            pcoll
            | beam.ParDo(_iter, name)
        )


class GenerateSequence(beam.PTransform):

    def __init__(self, start=1, stop=sys.maxint, sleep=1.0):
        self.start = start
        self.stop = stop
        self.sleep = sleep

    def expand(self, pcoll):

        def _iter(element):
            for i in xrange(*element):
                yield i
                time.sleep(self.sleep)

        return (
            pcoll
            | beam.Create([(self.start, self.stop)])
            | beam.ParDo(_iter)
        )


def main(options):
    pipe = beam.Pipeline(options=options)

    gencoll1 = (
        pipe
        | 'Gen1' >> GenerateSequence(start=1, stop=5, sleep=0.2)
        | 'Pre1' >> Log(color='yellow')
    )
    gencoll2 = (
        pipe
        | 'Gen2' >> GenerateSequence(start=5, stop=10, sleep=0.4)
        | 'Pre2' >> Log(color='yellow')
    )

    inflow = (
        (gencoll1, gencoll2)
        | beam.Flatten()
    )

    inflow | 'Post' >> Log(color='green')

    result = pipe.run()
    result.wait_until_finish()


if __name__ == '__main__':
    import argparse
    from apache_beam.options.pipeline_options import PipelineOptions
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args()

    main(PipelineOptions(pipeline_args))
