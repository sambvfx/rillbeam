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


def pubsub_interface(subscription_path, input_topic):
    import functools
    from google.cloud import pubsub_v1
    from google.cloud.pubsub_v1.subscriber.message import Message
    from rillbeam import tapp

    def callback(pane, message):
        # type: (tapp.Pane, Message) -> None
        message.ack()
        with pane.batch:
            pane.write('{} : {}'.format(
                message.publish_time, message.data.decode()), 'cyan')

    subscriber = pubsub_v1.SubscriberClient()
    publisher = pubsub_v1.PublisherClient()

    with tapp.App() as app:
        app.write('Beginning interactive pubsub session.', 'yellow',
                  attrs=['bold'])
        app.write()
        app.write('Subscriber {!r}...'.format(subscription_path),
                  'yellow')
        app.write('Publisher {!r}...'.format(subscription_path),
                  'yellow')
        app.write()

        app.write('Send words to pubsub to be counted. Messages will print '
                  'when they are received.', 'green')
        app.write('Type \'exit\' to stop.', 'green',
                  attrs=['bold'])
        app.write()

        streampane = app.pane(40, 80, app.line + 2, 0)

        future = subscriber.subscribe(
            subscription_path,
            callback=functools.partial(callback, streampane))

        try:
            while True:
                try:
                    msg = app.prompt()
                except KeyboardInterrupt:
                    continue
                if not msg:
                    continue
                elif msg.lower() == 'exit':
                    break
                else:
                    publisher.publish(input_topic, data=msg)
        finally:
            future.cancel()
