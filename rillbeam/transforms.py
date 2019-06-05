from __future__ import absolute_import

import logging
import threading
import functools

from google.protobuf import duration_pb2

import apache_beam as beam
import apache_beam.pvalue as pvalue
import apache_beam.transforms.window as window
import apache_beam.transforms.trigger as trigger
import apache_beam.coders as coders
import apache_beam.transforms.userstate as userstate
import apache_beam.transforms.timeutil as timeutil
from apache_beam.utils.timestamp import MIN_TIMESTAMP, MAX_TIMESTAMP

from apache_beam.typehints import *


T = TypeVariable('T')


def cache(fn):
    @functools.wraps(fn)
    def _wrap(*args, **kwargs):
        if hasattr(fn, '__cache__'):
            return getattr(fn, '__cache__')
        ret = fn(*args, **kwargs)
        setattr(fn, '__cache__', ret)
        return ret
    return _wrap


_logger = logging.getLogger(__name__)


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

    lock = threading.RLock()

    def process(self, element, name=None, repr=True, **kwargs):
        with self.lock:
            if name is None:
                name = self.default_label()
            log = logging.getLogger(name)
            log.setLevel(logging.DEBUG)
            if repr:
                log.info('{!r}'.format(element))
            else:
                log.info(element)
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


class SyncWindowFn(window.WindowFn):

    def __init__(self, size):
        if size <= 0:
            raise ValueError('Must provide a positive stream size.')
        self.size = size

    def __eq__(self, other):
        if type(self) == type(other):
            return self.size == other.size

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.size)

    # def to_runner_api_parameter(self, context):
    #     return (common_urns.session_windows.urn,
    #             standard_window_fns_pb2.SessionsPayload(
    #                 gap_size=self.MAX_DURATION))
    #
    # @urns.RunnerApiFn.register_urn(
    #     common_urns.session_windows.urn,
    #     standard_window_fns_pb2.SessionsPayload)
    # def from_runner_api_parameter(fn_parameter, unused_context):
    #     return SyncWindowFn(
    #         streams=fn_parameter.gap_size)

    def assign(self, context):
        return [window.IntervalWindow(context.element[0], MAX_TIMESTAMP)]

    def get_window_coder(self):
        return coders.IntervalWindowCoder()

    def merge(self, merge_context):
        to_merge = None
        for w in sorted(merge_context.windows, key=lambda x: x.start):
            _logger.info('WINDOW: {!r}'.format(w))

            if to_merge is None:
                to_merge = [w]
                continue

            current = to_merge[0].start

            if w.start > current:
                merge_context.merge(
                    to_merge, window.IntervalWindow(
                        to_merge[0].start, MAX_TIMESTAMP))
                to_merge = [w]
                continue

            assert w.start == current
            to_merge.append(w)

            if len(to_merge) == self.size:
                _logger.info('MERGING: {!r}'.format(to_merge))
                merge_context.merge(
                    to_merge, window.IntervalWindow(current, current))
                to_merge = None

        if len(to_merge) > 1:
            merge_context.merge(
                to_merge, window.IntervalWindow(
                    to_merge[0].start, MAX_TIMESTAMP))


class SyncFn(beam.DoFn):

    STATE = userstate.BagStateSpec('state', coders.PickleCoder())

    def __init__(self, size):
        assert size > 0, 'Must provide a positive size'
        self.size = size

    def process(self, element, state=beam.DoFn.StateParam(STATE)):
        idx, value = element

        cache = list(state.read())
        if cache:
            cache = cache[0]
        else:
            cache = {}

        values = cache.get(idx, [])
        values.append(value)

        if len(values) == self.size:
            del cache[idx]
            yield tuple(values)
        else:
            cache[idx] = values

        state.clear()
        if cache:
            state.add(cache)


class ByIndexFn(beam.DoFn):

    STATE = userstate.BagStateSpec('index', coders.VarIntCoder())

    def process(self, element, state=beam.DoFn.StateParam(STATE)):
        unused_key, value = element
        cache = list(state.read())
        if not cache:
            cache = [0]
        idx = cache[0]
        yield idx, value
        state.clear()
        state.add(idx + 1)


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

        return (
            (pcoll
             | 'key{}'.format(i) >> beam.Map(lambda x: (i, x))
             # | 'win{}'.format(i) >> beam.WindowInto(
             #        window.GlobalWindows(),
             #        trigger=trigger.AfterCount(1),
             #        accumulation_mode=trigger.AccumulationMode.DISCARDING,
             #  )
             | 'coll{}'.format(i) >> beam.ParDo(ByIndexFn())
             for i, pcoll in enumerate(pcolls))
            | beam.Flatten(pipeline=self.pipeline)
            | beam.ParDo(SyncFn(len(pcolls)))
        )


@beam.typehints.with_input_types(Tuple[Any, Iterable[T]])
@beam.typehints.with_output_types(Tuple[str, int, T])
class FauxFarmFn(beam.DoFn):

    STATE = userstate.BagStateSpec(
        'state',
        coders.PickleCoder()
    )
    VALUES = userstate.BagStateSpec(
        'values',
        coders.PickleCoder()
    )
    WAIT = userstate.TimerSpec('freq', timeutil.TimeDomain.REAL_TIME)

    def process(self, element,
                state=beam.DoFn.StateParam(STATE),
                values=beam.DoFn.StateParam(VALUES),
                timer=beam.DoFn.TimerParam(WAIT)):
        import uuid
        _, data = element
        id_ = str(uuid.uuid4())
        state.add((id_, list(range(len(data)))))
        values.add((id_, data))
        timer.set(20)

    @userstate.on_timer(WAIT)
    def task_done(self,
                  state=beam.DoFn.StateParam(STATE),
                  values=beam.DoFn.StateParam(VALUES),
                  timer=beam.DoFn.TimerParam(WAIT)):
        import random

        refresh = False

        pending = list(state.read())
        results = {}  # type: Dict[str, List[Any]]
        for data in values.read():
            results[data[0]] = data[1]
        state.clear()
        values.clear()
        for jobid, tasks in pending:
            idx = random.randint(0, len(tasks) - 1)
            yield jobid, tasks.pop(idx), results[jobid].pop(idx)
            if tasks:
                state.add((jobid, tasks))
                refresh = True
        for jobid, data in results.items():
            if data:
                values.add((jobid, data))
        if refresh:
            timer.set(20)


class FauxFarm(beam.PTransform):

    def expand(self, pcoll):
        self._check_pcollection(pcoll)
        return (
            pcoll
            | beam.Map(lambda x: ('_null_', x))
            | beam.ParDo(FauxFarmFn())
        )
