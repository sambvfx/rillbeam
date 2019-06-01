from __future__ import print_function

import os
import argparse
import time
import sys

from termcolor import cprint, colored

from apache_beam.options.pipeline_options import PipelineOptions


# NOTE: I don't *think* this is needed when using the DataflowRunner?
if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') is None:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.expanduser(
        '~/projects/luma/.cred/dataflow-d3c95049758e.json')

DEFAULTS = {
    'dataflow': (
        '--save_main_session',
        '--runner', 'DataflowRunner',
        '--project', 'dataflow-241218',
        '--temp_location', 'gs://dataflow-241218/temp',
        '--setup_file', './setup.py',
        '--region', 'us-west1',
        '--max_num_workers', '4',
    ),
    'flink': (
        '--save_main_session',
        '--runner', 'PortableRunner',
        'job_endpoint', 'localhost:8099',
        '--setup_file', './setup.py'
    ),
    'direct': (
        '--save_main_session',
    )
}


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--runner',
                        type=str,
                        default='direct',
                        choices=['direct', 'flink', 'dataflow'])
    return parser


def get_jobname(mod_name):
    file_name = os.path.split(sys.modules[mod_name].__file__)[1]
    return os.path.splitext(file_name)[0].replace('_', '-')


def get_options(mod_name, parser=None, *args, **kwargs):
    if parser is None:
        parser = get_parser()

    known_args, pipeline_args = parser.parse_known_args()

    runner_defaults = DEFAULTS[known_args.runner]

    if '--job_name' not in pipeline_args:
        pipeline_args.extend(['--job_name', get_jobname(mod_name)])

    for k in args:
        k = '--{}'.format(k)
        if k not in pipeline_args:
            pipeline_args.append(k)

    for k, v in kwargs.items():
        k = '--{}'.format(k)
        if k not in pipeline_args:
            pipeline_args.extend([k, v])

    for arg in runner_defaults:
        if arg not in pipeline_args:
            pipeline_args.append(arg)

    return PipelineOptions(pipeline_args), known_args


def pubsub_interface(subscription_path, input_topic, initial_data=None,
                     delay_seconds=1.0):
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
        app.write('Publisher {!r}...'.format(input_topic),
                  'yellow')
        app.write()

        streampane = app.pane(40, 80, app.line + 4, 0)

        sub_future = subscriber.subscribe(
            subscription_path,
            callback=functools.partial(callback, streampane))

        if initial_data:
            streampane.write('Sending {} initial packages...'.format(
                len(initial_data)), 'yellow')
            for msg in initial_data:
                streampane.write('Sending {!r}...'.format(msg))
                time.sleep(delay_seconds)
                publisher.publish(input_topic, data=msg)

        app.write('Send messages to pubsub. Output messages will print '
                  'when they are received.', 'green')
        app.write('Type \'exit\' to stop.', 'green',
                  attrs=['bold'])
        app.write()

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
                    streampane.write('Sending {!r}...'.format(msg))
                    publisher.publish(input_topic, data=msg)
        finally:
            sub_future.cancel()


def pubsub_interface2(subscription_path, input_topic, initial_data=None,
                      delay_seconds=1.0):

    from google.cloud import pubsub_v1
    from google.cloud.pubsub_v1.subscriber.message import Message

    def callback(message):
        # type: (Message) -> None
        message.ack()
        cprint('{} : {}'.format(
            message.publish_time, message.data.decode()), 'cyan')

    subscriber = pubsub_v1.SubscriberClient()
    publisher = pubsub_v1.PublisherClient()

    cprint('Beginning interactive pubsub session.', 'yellow',
              attrs=['bold'])
    print()
    cprint('Subscriber {!r}...'.format(subscription_path),
              'yellow')
    cprint('Publisher {!r}...'.format(input_topic),
              'yellow')
    print()

    sub_future = subscriber.subscribe(
        subscription_path,
        callback=callback)

    if initial_data:
        cprint('Sending {} initial packages...'.format(
            len(initial_data)), 'yellow')
        for msg in initial_data:
            cprint('Sending {!r}...'.format(msg))
            time.sleep(delay_seconds)
            publisher.publish(input_topic, data=msg)

    cprint('Send messages to pubsub. Output messages will print '
              'when they are received.', 'green')
    cprint('Type \'exit\' to stop.', 'green',
              attrs=['bold'])
    print()

    try:
        while True:
            try:
                msg = raw_input()
            except KeyboardInterrupt:
                continue
            if not msg:
                continue
            elif msg.lower() == 'exit':
                break
            else:
                cprint('Sending {!r}...'.format(msg), 'yellow')
                publisher.publish(input_topic, data=msg)
    finally:
        sub_future.cancel()
