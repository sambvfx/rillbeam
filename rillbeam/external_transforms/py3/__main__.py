from apache_beam.options.pipeline_options import PipelineOptions, PortableOptions

from . import fs

if __name__ == '__main__':
    from ..server import run

    options = PipelineOptions()
    options.view_as(PortableOptions).environment_type = \
        'DOCKER'
    options.view_as(PortableOptions).environment_config = \
        'localhost:5000/beam/python3:latest'

    run(8297, options=options)
