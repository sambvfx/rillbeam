import apache_beam as beam

from rillbeam.transforms import Log

# To test with the portibility framework, you can run everything within python!
#
# 1) Start local job server:
#   python -m apache_beam.runners.portability.local_job_service_main -p 8199
#
# 2) Start expansion service:
#   python rillbeam/external_transforms/server.py -p 8197
#
# 3) Run using "local" runner:
#   python -m rillbeam.experiments.external --runner local


def main(options):

    pipe = beam.Pipeline(options=options)
    (
        pipe
        | 'Gen' >> beam.Create(list(range(10)))
        | 'AsBytes' >> beam.Map(lambda x: b'{}'.format(x))
        | 'XMult' >> beam.ExternalTransform(
              'xmult',
              b'3',
              'localhost:8197',
          )
        | 'Log' >> Log()
    )

    # proto, ctx = pipe.to_runner_api(return_context=True)
    result = pipe.run()
    result.wait_until_finish()


if __name__ == '__main__':
    from rillbeam.helpers import get_options
    pipeline_args, _ = get_options(__name__, None)
    main(pipeline_args)
