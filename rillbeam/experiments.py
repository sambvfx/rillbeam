import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import apache_beam.transforms.trigger as trigger
import apache_beam.transforms.window as window


def test_flowbased(argv):
    from rillbeam.components import SleepFn, Log

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipe = beam.Pipeline(options=pipeline_options)

    (
        pipe
        | 'Init' >> beam.Create(range(10))
        | 'Sleep' >> beam.ParDo(SleepFn(), duration=1.0)
        | 'Log' >> Log()
    )

    result = pipe.run()
    result.wait_until_finish()


def test_windowing(argv):
    # FIXME: This doesn't seem to behave as expected in the DirectRunner.

    from rillbeam.components import Log, SleepFn

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipe = beam.Pipeline(options=pipeline_options)

    graph = (
        pipe
        | beam.Create([(k, k) for k in range(10)])
        | beam.Map(lambda x_t: window.TimestampedValue(x_t[0], x_t[1]))
    )

    (
        graph | 'Stream' >> Log()
    )

    (
        graph
        | beam.ParDo(SleepFn(), 1.0)
        | 'Window' >> beam.WindowInto(
              window.FixedWindows(3),
              trigger=trigger.AfterCount(3),
              accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
              # default
              timestamp_combiner=window.TimestampCombiner.OUTPUT_AT_EOW,
         )
        | 'WindowLog' >> Log()
    )

    result = pipe.run()
    result.wait_until_finish()


def test_fail(argv):
    from rillbeam.components import SleepFn, Log, FailOnFive

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipe = beam.Pipeline(options=pipeline_options)

    (
        pipe
        | 'begin' >> beam.Create(range(10))
        | 'Sleep' >> beam.ParDo(SleepFn(), duration=1.0)
        | 'Log' >> Log()
        | 'Fail' >> beam.ParDo(FailOnFive())
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
        if known_args.filter and known_args.filter not in name:
            continue
        print
        print '-' * 80
        print name
        print '-' * 80
        func(pipeline_args)
