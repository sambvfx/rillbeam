import os
import apache_beam as beam


@beam.PTransform.register_urn('xmult', bytes)
class XMult(beam.PTransform):
    def __init__(self, payload):
        self._payload = payload

    def expand(self, pcoll):
        return (
            pcoll
            | beam.Map(lambda x, s: int(x.decode('ascii')) * s, self._payload)
        )

    def to_runner_api_parameter(self, context):
        return 'xmult', str(self._payload).encode('ascii')

    @staticmethod
    def from_runner_api_parameter(payload, context):
        return XMult(int(payload.decode('ascii')))
