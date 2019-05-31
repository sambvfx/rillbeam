"""
Test pubsub graph.
"""
import re
import time
import functools

from termcolor import cprint

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.runners.runner import PipelineState
from apache_beam.metrics import Metrics

from apache_beam.runners.runner import PipelineResult
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.message import Message

from rillbeam import tapp


# Google pubsub topics and subscriptions
INPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-inflow'
OUTPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-outflow'
SUBSCRIPTION_PATH = 'projects/dataflow-241218/subscriptions/manual'


class WordExtractingDoFn(beam.DoFn):
    """
    Parse each line of input text into words.
    """

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


def interface():

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
        app.write('Subscriber {!r}...'.format(SUBSCRIPTION_PATH),
                  'yellow')
        app.write('Publisher {!r}...'.format(SUBSCRIPTION_PATH),
                  'yellow')
        app.write()

        app.write('Send words to pubsub to be counted. Messages will print '
                  'when they are received.', 'green')
        app.write('Type \'exit\' to stop.', 'green',
                  attrs=['bold'])
        app.write()

        streampane = app.pane(40, 80, app.line + 2, 0)

        future = subscriber.subscribe(
            SUBSCRIPTION_PATH,
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
                    publisher.publish(INPUT_TOPIC, data=msg)
        finally:
            future.cancel()


def main(options):

    def count_ones(word_ones):
        (word, ones) = word_ones
        return word, sum(ones)

    def format_result(word_count):
        (word, count) = word_count
        return '%s: %d' % (word, count)

    pipe = beam.Pipeline(options=options)

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
        | 'ToBytes' >> beam.Map(lambda x: bytes(x))
        | 'PubSubOutflow' >> beam.io.WriteToPubSub(OUTPUT_TOPIC)
    )

    print
    cprint('Starting pipeline...', 'yellow', attrs=['bold'])
    result = pipe.run()  # type: PipelineResult
    time.sleep(10)
    while result.state != PipelineState.RUNNING:
        time.sleep(10)

    try:
        interface()
    finally:
        print
        cprint('Shutting down pipeline...', 'yellow', attrs=['bold'])
        result.cancel()
        print


if __name__ == '__main__':
    import argparse
    from rillbeam.helpers import get_options
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_args = get_options(
        pipeline_args,
        'streaming',
        runner='DataflowRunner',
    )
    main(pipeline_args)
