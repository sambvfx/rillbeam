"""
Basic graph to test docker volume mounting.
"""
import apache_beam as beam


DIR_NAME = '/tmp/beam_test'


def touch(f):
    import os
    with open(f, 'a'):
        os.utime(f, None)


def main(options):
    with beam.Pipeline(options=options) as pipe:
        (
            pipe
            | 'Init' >> beam.Create(['{}/f1.ext'.format(DIR_NAME)])
            | 'Touch' >> beam.Map(touch)
        )


if __name__ == '__main__':
    from rillbeam.helpers import get_options, REGISTRY_URL
    pipeline_args, _ = get_options(
        __name__,
        environment_config='-v {}:{} {}/beam/python:latest'.format(
            DIR_NAME, DIR_NAME, REGISTRY_URL),
    )
    main(pipeline_args)
