import os
import shutil
import pickle
import functools

import apache_beam as beam
from apache_beam.runners.portability import expansion_service

from rillbeam.transforms import Log


class Join(beam.PTransform):

    def expand(self, pcoll):
        return pcoll | beam.Map(lambda x: os.path.join(*x))


class Touch(beam.PTransform):

    def expand(self, pcoll):
        def touch(f):
            with open(f, 'a'):
                os.utime(f, None)
            return f
        return pcoll | beam.Map(touch)


class EnvTransform(beam.ExternalTransform):

    DEFAULT_PKGS = (
        'git',
        'pipe-breanches.luma',
        'luma_python',
        'beam',
    )

    DEFAULT_CONTAINER_NAME = 'localhost:5000/beam/python:latest'

    def __init__(self, transform, pkgs=DEFAULT_PKGS,
                 container=DEFAULT_CONTAINER_NAME, *args, **kwargs):

        transform_urn = '{}.{}'.format(transform.__module__, transform.__name__)
        beam.PTransform.register_urn(
            transform_urn,
            bytes,
            constructor=functools.partial(self.constructor, transform))

        payload = pickle.dumps((args, kwargs))

        options = beam.pipeline.PipelineOptions(
            environment_type='DOCKER',
            # environment_config=(
            #     '-v /tmp/rillbeam/multiexternal '
            #     '-e DOCKER_ENTRYPOINT_SETPKG="{}" '
            #     '{}'.format(':'.join(pkgs), container)),
            environment_config=container,
        )

        # from apache_beam.portability import python_urns
        # options = beam.pipeline.PipelineOptions(
        #     environment_type=python_urns.SUBPROCESS_SDK,
        #     environment_config=(
        #         b'{} -m apache_beam.runners.worker.sdk_worker_main'.format(
        #             sys.executable.encode('ascii')))
        # )

        endpoint = expansion_service.ExpansionServiceServicer(options=options)
        super(EnvTransform, self).__init__(transform_urn, payload, endpoint)

    @staticmethod
    def constructor(transform, payload, context):
        args, kwargs = pickle.loads(payload)
        return transform(*args, **kwargs)


def main(options):

    root = '/tmp/rillbeam/multiexternal'
    if os.path.exists(root):
        shutil.rmtree(root)
    os.makedirs(root)

    pipe = beam.Pipeline(options=options)
    (
        pipe
        | 'Gen' >> beam.Create([
              (root, 'f1.ext'),
              (root, 'f2.ext'),
              (root, 'f3.ext'),
          ])
        | 'Join' >> EnvTransform(Join)
        | 'Touch' >> EnvTransform(Touch)
        | 'Log' >> Log()
    )

    # proto, ctx = pipe.to_runner_api(return_context=True)
    result = pipe.run()
    result.wait_until_finish()


if __name__ == '__main__':
    from rillbeam.helpers import get_options
    pipeline_args, _ = get_options(__name__)
    main(pipeline_args)
