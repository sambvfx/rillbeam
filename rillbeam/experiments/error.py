"""
See how beam handles erroring.
"""
import apache_beam as beam

from rillbeam.transforms import SleepFn, Log, FailOnFive


def main(options):

    with beam.Pipeline(options=options) as pipe:
        (
            pipe
            | 'begin' >> beam.Create(range(10))
            | 'Sleep' >> beam.ParDo(SleepFn(), duration=1.0)
            | 'Log' >> Log()
            | 'Fail' >> beam.ParDo(FailOnFive())
        )


if __name__ == '__main__':
    import argparse
    from rillbeam.helpers import get_options
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_args = get_options(pipeline_args)
    main(pipeline_args)
