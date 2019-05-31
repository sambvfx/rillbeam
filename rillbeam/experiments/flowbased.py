"""
Basic graph to showcase that logging happens eagerly.
"""
import apache_beam as beam

from rillbeam.transforms import SleepFn, Log


def main(options):
    with beam.Pipeline(options=options) as pipe:
        (
            pipe
            | 'Init' >> beam.Create(range(10))
            | 'Sleep' >> beam.ParDo(SleepFn(), duration=1.0)
            | 'Log' >> Log()
        )


if __name__ == '__main__':
    import argparse
    from rillbeam.helpers import get_options
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_args = get_options(pipeline_args)
    main(pipeline_args)
