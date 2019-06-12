"""
Testing the retry behavior.
"""
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.utils import retry

from rillbeam.transforms import Log


def retry_on_value_error_filter(exception):
    return isinstance(exception, ValueError)


@retry.with_exponential_backoff(num_retries=5, initial_delay_secs=0.1,
                                max_delay_secs=10,
                                retry_filter=retry_on_value_error_filter)
def fail_on_five(item):
    if item == 5:
        raise ValueError("I hate 5!")


@beam.typehints.with_input_types(item=int)
@beam.typehints.with_output_types(int)
class FailOnFiveWithRetry(beam.DoFn):

    # when the retry decorator is directly applied on `process`,
    # it wasn't able to catch the exception
    # @retry.with_exponential_backoff(num_retries=5, initial_delay_secs=0.1,
    #                             max_delay_secs=10,
    #                             retry_filter=retry_on_value_error_filter)
    def process(self, item, **kwargs):
        try:
            fail_on_five(item)
        except Exception as e:
            yield pvalue.TaggedOutput('exceptions', e)
        yield item


def main(options):

    with beam.Pipeline(options=options) as pipe:
        result = (
            pipe
            | 'begin' >> beam.Create(range(10))
            | 'Log' >> Log()
            | 'RetryAndFail' >> beam.ParDo(FailOnFiveWithRetry()).with_outputs('exceptions')
        )

        (
            result.exceptions
            | 'LogExceptions' >> Log()
        )
        (
            result[None]
            | 'LogMainResult' >> Log()
        )


if __name__ == '__main__':
    from rillbeam.helpers import get_options
    pipeline_args, _ = get_options(__name__)
    main(pipeline_args)
