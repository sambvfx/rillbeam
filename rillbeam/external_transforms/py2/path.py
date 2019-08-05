import os
import apache_beam as beam


@beam.PTransform.register_urn('join', None)
class Join(beam.PTransform):

    def expand(self, pcoll):
        return pcoll | beam.Map(lambda x: os.path.join(*x))

    def to_runner_api_parameter(self, context):
        return 'join', None

    @staticmethod
    def from_runner_api_parameter(payload, context):
        return Join()
