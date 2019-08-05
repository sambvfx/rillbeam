import os
import apache_beam as beam

from rillbeam.transforms import Log


# 1) Build and push docker containers:
#   ./gradlew -p sdks/python docker -P docker-repository-root="localhost:5000/beam"
#   docker push localhost:5000/beam/python:latest
#   docker push localhost:5000/beam/python3:latest
#
# 2) Start external transform services:
#   python -m rillbeam.external_transforms.py2
#   python -m rillbeam.external_transforms.py3
#
# 3) Run:
#   python -m rillbeam.experiments.multiexternal


def main(options):

    DIR = '/tmp/rillbeam/multiexternal'
    if not os.path.exists(DIR):
        os.makedirs(DIR)

    pipe = beam.Pipeline(options=options)
    (
        pipe
        | 'Gen' >> beam.Create([
              (DIR, 'f1.ext'),
              (DIR, 'f2.ext'),
              (DIR, 'f3.ext'),
          ])
        | 'Join' >> beam.ExternalTransform(
              'join',
              None,
              'localhost:8197',
          )
        | 'Touch' >> beam.ExternalTransform(
              'touch',
              None,
              'localhost:8297',
          )
        | 'Log' >> Log()
    )

    # proto, ctx = pipe.to_runner_api(return_context=True)
    result = pipe.run()
    result.wait_until_finish()


if __name__ == '__main__':
    from rillbeam.helpers import get_options
    pipeline_args, _ = get_options(__name__)
    main(pipeline_args)
