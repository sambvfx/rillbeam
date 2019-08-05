from apache_beam.options.pipeline_options import PipelineOptions, PortableOptions

from . import fs

if __name__ == '__main__':
    from ..server import run

    options = PipelineOptions()

    # import sys
    # from apache_beam.portability import python_urns
    # options.view_as(PortableOptions).environment_type = \
    #     python_urns.SUBPROCESS_SDK
    # options.view_as(PortableOptions).environment_config = \
    #     b'{} -m apache_beam.runners.worker.sdk_worker_main'.format(
    #         sys.executable.encode('ascii'))

    options.view_as(PortableOptions).environment_type = \
        'DOCKER'
    options.view_as(PortableOptions).environment_config = \
        'localhost:5000/beam/python3:latest'

    run(8297, options=options)
