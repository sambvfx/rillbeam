from __future__ import absolute_import

import logging
import threading

from termcolor import colored

import apache_beam as beam
import apache_beam.pvalue as pvalue
import apache_beam.transforms.window as window
import apache_beam.coders as coders
import apache_beam.transforms.userstate as userstate
import apache_beam.transforms.timeutil as timeutil
from apache_beam.utils.timestamp import MAX_TIMESTAMP

from apache_beam.typehints import *


T = TypeVariable('T')


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

    def process(self, element, name=None, color=None, repr=True, **kwargs):
        with self.lock:
            if name is None:
                name = self.default_label()
            log = logging.getLogger(name)
            log.setLevel(logging.DEBUG)

            if repr:
                msg = '{!r}'.format(element)
            else:
                msg = element
            if color:
                if isinstance(color, (str, unicode)):
                    msg = colored(msg, color)
                else:
                    color, at = color
                    msg = colored(msg, color, attrs=at)
            log.info(msg)
        yield element


class Log(beam.PTransform):

    def __init__(self, color=None, **kwargs):
        super(Log, self).__init__(**kwargs)
        self.color = color

    def expand(self, pcoll):
        return (
            pcoll
            | beam.ParDo(LogFn(), name=self.label, color=self.color)
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


class JobAggregateLevel:
    """
    Enum for specifying the aggregation level of job output collections.
    """

    GRAPH = 'graph'
    JOB = 'job'
    TASK = 'task'

    STATEFUL = (JOB, GRAPH)
    STATELESS = (TASK,)
    ALL = (GRAPH, JOB, TASK)


@beam.typehints.with_input_types(element=Tuple[str, Dict[Any, Any]], level=str)
@beam.typehints.with_output_types(List[str])
class _StatefulJobOutputsFn(beam.DoFn):

    STATE = userstate.BagStateSpec('state', coders.PickleCoder())

    @staticmethod
    def keybylevel(level):
        # This exists to simply reduce the number of places we specify fields
        # within our farm payload.
        if level == JobAggregateLevel.JOB:
            return 'jobid'
        elif level == JobAggregateLevel.GRAPH:
            return 'graphid'
        raise NotImplementedError

    def process(self, element, level, state=beam.DoFn.StateParam(STATE)):
        assert level in JobAggregateLevel.STATEFUL

        # example payload structure...
        # {
        #     'source': Any
        #     'graphid': 0,
        #     'jobtasks': {0: 3, 1: 3},
        #     'jobid': 0,
        #     'taskid': 2,
        #     'output': [
        #         '/tmp/job-0_output-0.task-2.ext',
        #         '/tmp/job-0_output-1.task-2.ext',
        #     ],
        # }
        _, payload = element

        # There are two values we will track that differ depending on the
        # aggregation type/level desired.
        #
        # - key : aggregation per-unique value
        # - size : total number of times expected to see `key`

        key = payload[self.keybylevel(level)]
        if level == JobAggregateLevel.JOB:
            # str(key) is to deal with json making all dict keys strings
            size = payload['jobtasks'][str(key)]
        elif level == JobAggregateLevel.GRAPH:
            size = sum(payload['jobtasks'].values())
        else:
            raise NotImplementedError

        cache = dict(state.read())
        seen, data = cache.get(key, (0, []))
        seen += 1
        data.extend(payload['output'])
        cache[key] = (seen, data)
        state.clear()

        for k, v in cache.items():
            # size == seen
            if size == v[0]:
                # cprint('fire-{}: {}'.format(level, k), 'red', attrs=['bold'])
                yield cache.pop(k)[1]
            else:
                state.add((k, v))


@beam.typehints.with_input_types(element=Dict[Any, Any], level=str)
@beam.typehints.with_output_types(List[str])
class _StatelessJobOutputsFn(beam.DoFn):

    def process(self, element, level):
        assert level in JobAggregateLevel.STATELESS
        if level == JobAggregateLevel.TASK:
            yield element['output']
        else:
            raise NotImplementedError


class JobOutput(beam.PTransform):
    """
    Transform for aggregating job output payloads.

    Graph authors can choose a `JobAggregateLevel` to produce the desired
    aggregation level (by task, by job, or by graph).
    """

    def __init__(self, level=JobAggregateLevel.GRAPH, **kwargs):
        super(JobOutput, self).__init__(**kwargs)
        assert level in JobAggregateLevel.ALL
        self.level = level

    def expand(self, pcoll):
        self._check_pcollection(pcoll)

        if self.level in JobAggregateLevel.STATEFUL:
            key = _StatefulJobOutputsFn.keybylevel(self.level)
            return (
                pcoll
                # TODO: Needs researching...
                #  Stateful transforms must be keyed. Here I'm using a key
                #  based on how we're going to do aggregation. I don't think
                #  this changes anything, but perhaps this will affect the
                #  windows?
                | beam.Map(lambda x: (x[key], x))
                | beam.ParDo(_StatefulJobOutputsFn(), self.level)
            )

        elif self.level in JobAggregateLevel.STATELESS:
            return (
                pcoll
                | beam.ParDo(_StatelessJobOutputsFn(), self.level)
            )
        else:
            raise NotImplementedError
