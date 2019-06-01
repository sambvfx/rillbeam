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
    from rillbeam.helpers import get_options
    pipeline_args, _ = get_options(__name__)
    main(pipeline_args)
