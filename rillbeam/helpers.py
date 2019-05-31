import os
from apache_beam.options.pipeline_options import PipelineOptions


# NOTE: I don't *think* this is needed when using the DataflowRunner?
if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') is None:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.expanduser(
        '~/projects/luma/.cred/dataflow-d3c95049758e.json')


DEFAULTS = (
    '--save_main_session',
    # FIXME: Make default?
    # '--streaming',
)

DATAFLOW_DEFAULTS = (
    '--runner', 'DataflowRunner',
    '--project', 'dataflow-241218',
    '--temp_location', 'gs://dataflow-241218/temp',
    '--setup_file', './setup.py',
    '--region', 'us-west1',
    '--max_num_workers', '4',
)

FLINK_DEFAULTS = (
    '--runner', 'PortableRunner',
    'job_endpoint', 'localhost:8099',
    '--setup_file', './setup.py'
)


def get_options(pipeline_args, *args, **kwargs):
    for k in args:
        k = '--{}'.format(k)
        if k not in pipeline_args:
            pipeline_args.append(k)

    for k, v in kwargs.items():
        k = '--{}'.format(k)
        if k not in pipeline_args:
            pipeline_args.extend([k, v])

    for arg in DEFAULTS:
        if arg not in pipeline_args:
            pipeline_args.append(arg)

    return PipelineOptions(pipeline_args)
