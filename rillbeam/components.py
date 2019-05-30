from __future__ import absolute_import

import logging

import apache_beam as beam
from apache_beam import pvalue
import apache_beam.transforms.window as window
import apache_beam.transforms.trigger as trigger
import apache_beam.utils.timestamp as timestamp
from apache_beam.typehints import *


T = TypeVariable('T')


@beam.typehints.with_input_types(element=T, duration=float,
                                 variation=Optional[Tuple[float, float]])
@beam.typehints.with_output_types(T)
class SleepFn(beam.DoFn):
    def process(self, element, duration=0.5, variation=None, **kwargs):
        import time
        import random
        if variation:
            duration += random.uniform(*variation)
        time.sleep(duration)
        yield element


@beam.typehints.with_input_types(item=int)
@beam.typehints.with_output_types(int)
class FailOnFive(beam.DoFn):
    def process(self, item, **kwargs):
        if item == 5:
            raise ValueError("I hate 5!")
        yield item


@beam.typehints.with_input_types(element=T)
@beam.typehints.with_output_types(T)
class LogFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam, name=None, repr=True, **kwargs):
        if name is None:
            name = self.default_label()

        _logger = logging.getLogger(name)
        _logger.setLevel(logging.DEBUG)

        try:
            window_end = window.end.to_utc_datetime()
        except OverflowError:
            window_end = str(int(window.end))


        if repr:
            _logger.info('{!r} {}'.format(element, window_end))
        else:
            _logger.info('{} {}'.format(element, window_end))
        yield element


class Log(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | beam.ParDo(LogFn(), name=self.label)
        )


@beam.typehints.with_output_types(float)
class RandomFn(beam.DoFn):
    def process(self, _, **kwargs):
        import random
        yield random.random()


@beam.typehints.with_input_types(element=T)
@beam.typehints.with_output_types(T)
class ErrorFn(beam.DoFn):
    def __init__(self):
        super(ErrorFn, self).__init__()
        self.position = 0

    def process(self, element, index=4, exception=RuntimeError):
        if self.position >= index:
            raise exception('Test Error')
        self.position += 1
        yield element


@beam.typehints.with_input_types(element=float)
@beam.typehints.with_output_types(Tuple[float, float])
class SplitFn(beam.DoFn):
    def process(self, element, split=0.5, **kwargs):
        if element < split:
            yield pvalue.TaggedOutput('lower', element)
        else:
            yield pvalue.TaggedOutput('upper', element)


class Sync(beam.PTransform):
    """
    Group PCollections by element index.

    ([1, 2, 3], [4, 5, 6]) -> [(1, 4), (2, 5), (3, 6)]
    """

    def __init__(self, **kwargs):
        super(Sync, self).__init__()
        self.pipeline = kwargs.pop('pipeline', None)

        # FIXME: make this work
        self.mode = kwargs.pop('mode', 'synced')
        assert self.mode in ('shortest', 'longest', 'synced')

        if kwargs:
            raise ValueError(
                'Unexpected keyword arguments: %s' % list(kwargs.keys()))

    def _check_pcollections(self, pcolls):
        # Check input PCollections for PCollection-ness, and that they all
        # belongto the same pipeline.
        for pcoll in pcolls:
            self._check_pcollection(pcoll)
            if self.pipeline:
                assert pcoll.pipeline == self.pipeline

    def expand(self, pcolls):
        assert isinstance(pcolls, (tuple, list))
        self._check_pcollections(pcolls)

        # Tracks the current index per-pcoll.
        _position = {i: 0 for i in range(len(pcolls))}

        def _keymap(item, idx_coll):
            idx = _position[idx_coll]
            _position[idx_coll] += 1
            return idx, item

        return (
            [pcoll | 'coll{}'.format(i) >> beam.Map(_keymap, i)
             for i, pcoll in enumerate(pcolls)]
            # Passing self.pipeline here is copying what's done in CoGroupByKey
            # Which is the only other multi-pcollection transform I had to
            # model this after.
            | beam.Flatten(pipeline=self.pipeline)
            # I think we may need a custom trigger that fires once we have an
            # element from each collection.
            | beam.WindowInto(
                  window.FixedWindows(10),
                  trigger=trigger.Repeatedly(trigger.AfterCount(2)),
                  accumulation_mode=trigger.AccumulationMode.DISCARDING
              )
            | beam.GroupByKey()
            | beam.Values()
        )
