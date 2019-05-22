import sys
import argparse
import logging

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import apache_beam.transforms.trigger as trigger
import apache_beam.transforms.window as window


def test_flowbased(argv):
    from rillbeam.components import Sleep, Log

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipe = beam.Pipeline(options=pipeline_options)

    (
        pipe
        | 'begin' >> beam.Create(range(10))
        | 'Sleep' >> beam.ParDo(Sleep(), duration=1.0)
        | 'Log' >> beam.ParDo(Log())
    )

    result = pipe.run()
    result.wait_until_finish()


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.INFO)

    tests = {k: v for k, v in globals().items()
             if k.startswith('test') and callable(v)}

    parser = argparse.ArgumentParser()
    parser.add_argument('--filter')
    known_args, pipeline_args = parser.parse_known_args()

    for name, func in tests.items():
        if known_args.filter and name not in known_args.filter:
            continue
        print
        print '-' * 80
        print name
        print '-' * 80
        func(pipeline_args)
