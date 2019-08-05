import os
import apache_beam as beam


def touch(f):
    with open(f, 'a'):
        os.utime(f, None)


@beam.PTransform.register_urn('touch', None)
class Touch(beam.PTransform):

    def expand(self, pcoll):
        return pcoll | beam.Map(touch)

    def to_runner_api_parameter(self, context):
        return 'touch', None

    @staticmethod
    def from_runner_api_parameter(payload, context):
        return Touch()
