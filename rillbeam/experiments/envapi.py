import os
import functools
import pickle

import apache_beam as beam
from apache_beam.runners.portability import expansion_service

from rillbeam.transforms import Log


class EnvTransform(beam.ExternalTransform):

    def __init__(self, transform, envvars, *args, **kwargs):
        urn = '{}.{}'.format(transform.__module__, transform.__name__)
        if urn not in beam.PTransform._known_urns:
            beam.PTransform.register_urn(
                urn,
                bytes,
                constructor=functools.partial(self.constructor, transform))

        # options = beam.pipeline.PipelineOptions(
        #     environment_type='DOCKER',
        #     environment_config=(
        #         '{env} '
        #         '{container}'.format(
        #             env=' '.join('-e {}={}'.format(k, v)
        #                          for k, v in envvars.items()),
        #             container='localhost:5000/beam/python:latest',
        #         )),
        #     )
        import sys
        from apache_beam.portability import python_urns
        env = ' '.join('{}={}'.format(k, v) for k, v in envvars.items())
        options = beam.pipeline.PipelineOptions(
            environment_type=python_urns.SUBPROCESS_SDK,
            environment_config=(
                b'{} {} -m apache_beam.runners.worker.sdk_worker_main'.format(
                    env, sys.executable.encode('ascii'))
            ))

        payload = pickle.dumps((args, kwargs))
        endpoint = expansion_service.ExpansionServiceServicer(options=options)
        super(EnvTransform, self).__init__(urn, payload, endpoint)

    @staticmethod
    def constructor(transform, payload, context):
        args, kwargs = pickle.loads(payload)
        return transform(*args, **kwargs)


class GetEnv(beam.PTransform):
    def __init__(self, name=None):
        assert name
        self.name = name
        super(GetEnv, self).__init__()

    def expand(self, pcoll):
        def _getenv(_, name):
            return os.environ.get(name)
        return pcoll | beam.Map(_getenv, self.name)


class AssertEnv(beam.PTransform):
    def __init__(self, **kwargs):
        assert kwargs
        self.kwargs = kwargs
        super(AssertEnv, self).__init__()

    def expand(self, pcoll):
        def _assert(_, kwargs):
            for k, v in kwargs.items():
                assert os.environ.get(k) == v
        return pcoll | beam.Map(_assert, self.kwargs)


# Defining transforms with default envs

# Option 1: Decorator
def env(**kwargs):
    def _wraps(cls):
        return functools.partial(EnvTransform, cls, kwargs)
    return _wraps


@env(FOO='BAR')
class NeedsEnv(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | AssertEnv(FOO='BAR')


def main(options):
    with beam.Pipeline(options=options) as pipe:
        (
            pipe
            | 'Impulse' >> beam.Impulse()
            | 'LocalEnv' >> GetEnv('FOO')
            | 'Local' >> Log(color='green')
            | 'RemoteEnv' >> EnvTransform(GetEnv, {'FOO': 'BAR'}, 'FOO')
            | 'Remote' >> Log(color='cyan')
            | 'AssertEnv' >> EnvTransform(AssertEnv, {'FOO': 'BAR'}, FOO='BAR')
            | 'ClsLvlNeedsEnv' >> NeedsEnv()
            # TODO:
            # | 'InstLvlNeedsEnv' >> NeedsEnv().with_env(FOO='BAZ')
        )


if __name__ == '__main__':
    from rillbeam.helpers import get_options
    pipeline_args, _ = get_options(__name__)
    main(pipeline_args)
