"""
Basic graph to showcase that logging happens eagerly.
"""
import apache_beam as beam

from rillbeam.transforms import SleepFn, Log

runner = None
try:
    import rill.runner
except ImportError:
    print("Not using rill")
else:
    runner = rill.runner.RillRunner()
    print("Using rill")


def main(options):
    from rillbeam.helpers import write_pipeline_text, write_pipeline_svg

    pipe = beam.Pipeline(options=options, runner=runner)
    (
        pipe
        | 'Init' >> beam.Create(range(10))
        | 'Sleep' >> beam.ParDo(SleepFn(), duration=1.0)
        | 'Log' >> Log()
    )

    write_pipeline_svg(pipe, __name__)

    result = pipe.run()
    result.wait_until_finish()


if __name__ == '__main__':
    from rillbeam.helpers import get_options
    pipeline_args, _ = get_options(__name__)
    main(pipeline_args)
