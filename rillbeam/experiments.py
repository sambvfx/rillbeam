from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
import apache_beam.transforms.trigger as trigger
import apache_beam.transforms.window as window


def test_flowbased(argv):
    # Tests that the sleep happens between each logged packet.
    from rillbeam.components import SleepFn, Log

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipe = beam.Pipeline(options=pipeline_options)

    (
        pipe
        | 'Init' >> beam.Create(range(10))
        | 'Sleep' >> beam.ParDo(SleepFn(), duration=1.0)
        | 'Log' >> Log()
    )

    result = pipe.run()
    result.wait_until_finish()


def test_synced(argv):
    import time
    from rillbeam.components import Log, Sync

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    pipe = beam.Pipeline(options=pipeline_options)

    class SlowStr(beam.DoFn):
        """
        Test component thata cast to a string slowly... Emulates something
        that takes a small variable amount of processing time.
        """
        def process(self, element, duration=0.5, variation=None, **kwargs):
            import time
            import random
            if variation:
                duration += random.uniform(*variation)
            time.sleep(duration)
            yield str(element)

    graph = (
        pipe
        | beam.Create(range(500))
    )

    b1 = (
        graph
        | 'Map1' >> beam.Map(lambda x: window.TimestampedValue(x, time.time()))
        | 'Window1' >> beam.WindowInto(window.FixedWindows(5))
        | 'Log1' >> Log()
    )

    b2 = (
        graph
        | '"Process"' >> beam.ParDo(SlowStr(), duration=0.1, variation=(0.0, 1.1))
        | 'Map2' >> beam.Map(lambda x: window.TimestampedValue(x, time.time()))
        | 'Window2' >> beam.WindowInto(window.FixedWindows(5))
        | 'Log2' >> Log()
    )

    synced = (
        (b1, b2)
        | Sync()
        | 'Success' >> Log()
    )

    result = pipe.run()
    result.wait_until_finish()


def test_windowing(argv):
    from rillbeam.components import Log, SleepFn

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipe = beam.Pipeline(options=pipeline_options)

    graph = (
        pipe
        | beam.Create([(k, k) for k in range(10)])
        | 'Timestamp' >> beam.Map(lambda x_t: window.TimestampedValue(x_t[0], x_t[1]))
    )

    stream_branch = (
        graph | 'LabelStream' >> beam.Map(lambda x: 'Stream{}'.format(x))
    )

    window_branch = (
        graph
        | 'Sleep' >> beam.ParDo(SleepFn(), 1.0)
        | 'Window' >> beam.WindowInto(
              window.FixedWindows(3),
              trigger=trigger.AfterCount(3),
              accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
              # default
              timestamp_combiner=window.TimestampCombiner.OUTPUT_AT_EOW,
         )
        | 'LabelWindow' >> beam.Map(lambda x: 'Window{}'.format(x))
    )

    # FIXME: beam.Flatten does not appear to eagerly consume. Currently we
    #  get alternating packets.
    #    ['Stream1', 'Window1', 'Stream2', 'Window2', ...]
    ((stream_branch, window_branch)
     | beam.Flatten()
     | Log())

    result = pipe.run()
    result.wait_until_finish()


def test_fail(argv):
    from rillbeam.components import SleepFn, Log, FailOnFive

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipe = beam.Pipeline(options=pipeline_options)

    (
        pipe
        | 'begin' >> beam.Create(range(10))
        | 'Sleep' >> beam.ParDo(SleepFn(), duration=1.0)
        | 'Log' >> Log()
        | 'Fail' >> beam.ParDo(FailOnFive())
    )

    result = pipe.run()
    result.wait_until_finish()


def test_pubsub(argv):
    """
    After starting this, from another shell insert some pubsub messages:

        $ gcloud pubsub topics publish projects/dataflow-241218/topics/rillbeam-inflow --message "$(< _data/HarryPotter.txt)"

    Then you should see some movement in this beam graph. After it finishes
    you can confirm that it wrote into the output topic by running:

        $ gcloud pubsub subscriptions pull projects/dataflow-241218/subscriptions/manual --auto-ack --limit 100000
    """
    import os
    import re

    from apache_beam.metrics import Metrics
    from rillbeam.components import Log

    if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') is None:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/samb/projects/luma/.cred/dataflow-d3c95049758e.json'

    INPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-inflow'
    OUTPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-outflow'

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
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


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.INFO)

    tests = {k: v for k, v in globals().items()
             if k.startswith('test') and callable(v)}

    parser = argparse.ArgumentParser()
    parser.add_argument('--filter')
    known_args, pipeline_args = parser.parse_known_args()

    for name, func in tests.items():
        if known_args.filter and known_args.filter not in name:
            continue
        print
        print '-' * 80
        print name
        print '-' * 80
        func(pipeline_args)
