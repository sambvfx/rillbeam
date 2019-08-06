import signal
import concurrent.futures as futures
import grpc

from apache_beam.portability.api import beam_expansion_api_pb2_grpc
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.portability import expansion_service


server = None


def run(port=8197, options=None):
    global server

    if options is None:
        import argparse
        parser = argparse.ArgumentParser()
        known_args, pipeline_args = parser.parse_known_args()
        options = PipelineOptions(pipeline_args)

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
    run()
