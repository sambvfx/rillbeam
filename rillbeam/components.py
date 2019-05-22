from __future__ import absolute_import

import logging

import apache_beam as beam


_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)


T = beam.typehints.TypeVariable('T')


@beam.typehints.with_input_types(item=T, duration=float)
@beam.typehints.with_output_types(T)
class Sleep(beam.DoFn):
    def process(self, item, duration=0.5, **kwargs):
        import time
        time.sleep(duration)
        yield item


@beam.typehints.with_input_types(item=T)
@beam.typehints.with_output_types(T)
class Log(beam.DoFn):
    def process(self, item, **kwargs):
        _logger.info(item)
        yield item


@beam.typehints.with_input_types(item=int)
@beam.typehints.with_output_types(int)
class FailOnFive(beam.DoFn):
    def process(self, item, **kwargs):
        if item == 5:
            raise ValueError("I hate 5!")
        yield item
