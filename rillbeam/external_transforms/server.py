import sys
import signal
import concurrent.futures as futures
import grpc

import apache_beam as beam
from apache_beam.portability.api import beam_expansion_api_pb2_grpc
from apache_beam.options.pipeline_options import PipelineOptions, PortableOptions
from apache_beam.runners.portability import expansion_service
from apache_beam.portability import python_urns


@beam.PTransform.register_urn('xmult', bytes)
class XMult(beam.PTransform):
    def __init__(self, payload):
        print('XMULT INIT')
        self._payload = payload

    def expand(self, pcoll):
        return (
            pcoll
            | beam.Map(lambda x, s: int(x.decode('ascii')) * s, self._payload)
        )

    def to_runner_api_parameter(self, context):
        return b'payload', str(self._payload).encode('ascii')

    @staticmethod
    def from_runner_api_parameter(payload, context):
        return XMult(int(payload.decode('ascii')))


server = None


def run(port='8197'):
    global server

    options = PipelineOptions()
    options.view_as(PortableOptions).environment_type = \
        python_urns.SUBPROCESS_SDK
    options.view_as(PortableOptions).environment_config = \
        b'{} -m apache_beam.runners.worker.sdk_worker_main'.format(
            sys.executable.encode('ascii'))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    beam_expansion_api_pb2_grpc.add_ExpansionServiceServicer_to_server(
        expansion_service.ExpansionServiceServicer(options), server
    )
    url = 'localhost:{}'.format(port)
    server.add_insecure_port(url)
    server.start()
    print('Listening for expansion requests at {!r}'.format(url))

    # blocking main thread forever.
    signal.pause()


def cleanup(unused_signum, unused_frame):
    print('Shutting down expansion service.')
    server.stop(None)


signal.signal(signal.SIGTERM, cleanup)
signal.signal(signal.SIGINT, cleanup)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p', '--port',
        default='8197',
    )
    args = parser.parse_args()
    run(args.port)
