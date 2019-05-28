"""
Dataflow / PubSub test graph.

NOTE: You'll have to manually stop the job in GCP or it will run forever.

Run in two shells:

Submit the Dataflow graph:
    $ python -m rillbeam.wordcount --submit

Insert some pubsub records of some text to count.
    $ python -m rillbeam.wordcount --path _data/HarryPotter.txt
"""
from __future__ import absolute_import

import os
import argparse
import time
import logging

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions

from google.cloud import pubsub_v1


DEFAULTS = (
    '--runner', 'DataflowRunner',
    '--project', 'dataflow-241218',
    '--temp_location', 'gs://dataflow-241218/temp',
    '--setup_file', './setup.py',
    '--region', 'us-west1',
    '--save_main_session',
    '--streaming',
    '--max_num_workers', '4',
)


PROJECT_ID = 'dataflow-241218'
INPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-inflow'
OUTPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-outflow'
SUBSCRIPTION_PATH = 'projects/dataflow-241218/subscriptions/manual'


if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') is None:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.expanduser(
        '~/projects/luma/.cred/dataflow-d3c95049758e.json')


def submit_graph(argv):
    """
    Submits the beam graph to Dataflow to run forever.
    """
    import re
    from apache_beam.metrics import Metrics
    from rillbeam.components import Log

    print ' '.join(argv)
    pipeline_options = PipelineOptions(argv)
    pipe = beam.Pipeline(options=pipeline_options)

    class WordExtractingDoFn(beam.DoFn):
        """Parse each line of input text into words."""

        def __init__(self):
            self.words_counter = Metrics.counter(
                self.__class__, 'words')
            self.word_lengths_counter = Metrics.counter(
                self.__class__, 'word_lengths')
            self.word_lengths_dist = Metrics.distribution(
                self.__class__, 'word_len_dist')
            self.empty_line_counter = Metrics.counter(
                self.__class__, 'empty_lines')

        def process(self, element):
            """
            Returns an iterator over the words of this element.

            The element is a line of text.  If the line is blank, note that,
            too.
            """
            text_line = element.strip()
            if not text_line:
                self.empty_line_counter.inc(1)
            words = re.findall(r'[\w\']+', text_line, re.UNICODE)
            for w in words:
                self.words_counter.inc()
                self.word_lengths_counter.inc(len(w))
                self.word_lengths_dist.update(len(w))
            return words

    def count_ones(word_ones):
        (word, ones) = word_ones
        return word, sum(ones)

    def format_result(word_count):
        (word, count) = word_count
        return '%s: %d' % (word, count)

    (
        pipe
        | 'PubSubInflow' >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC)
        | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
        | 'Split' >> (beam.ParDo(WordExtractingDoFn())
                      .with_output_types(unicode))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'Window' >> beam.WindowInto(window.FixedWindows(5, 0))
        | 'GroupByKey' >> beam.GroupByKey()
        | 'CountOnes' >> beam.Map(count_ones)
        | 'Format' >> beam.Map(format_result)
        | 'Log' >> Log()
        | 'ToBytes' >> beam.Map(lambda x: bytes(x))
        | 'PubSubOutflow' >> beam.io.WriteToPubSub(OUTPUT_TOPIC)
    )

    result = pipe.run()
    result.wait_until_finish()


def push(data):
    publisher = pubsub_v1.PublisherClient()
    print('Pushing payload to {!r}'.format(INPUT_TOPIC))
    publisher.publish(INPUT_TOPIC, data=data)


def pull():
    subscriber = pubsub_v1.SubscriberClient()

    def callback(message):
        print('Received message: {}'.format(message))
        message.ack()

    subscriber.subscribe(SUBSCRIPTION_PATH, callback=callback)

    print('Listening for messages on {!r}'.format(SUBSCRIPTION_PATH))
    while True:
        time.sleep(20)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--submit', action='store_true')
    parser.add_argument('--path')
    parser.add_argument('--words', nargs='+')

    known_args, pipeline_args = parser.parse_known_args()

    for arg in DEFAULTS:
        if arg not in pipeline_args:
            pipeline_args.append(arg)

    if known_args.submit:
        submit_graph(pipeline_args)
    else:
        if known_args.path:
            with open(os.path.expanduser(os.path.expandvars(known_args.path))) as f:
                payload = f.read()
        elif known_args.words:
            payload = '\n'.join(known_args.words)
        else:
            raise NotImplementedError('Must provide a valid option.')

        push(payload)

        # blocks forever
        pull()
