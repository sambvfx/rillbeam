from __future__ import absolute_import, print_function

import logging
import datetime

from google.protobuf import duration_pb2

from apache_beam.coders import coders
from apache_beam.portability import common_urns
from apache_beam.portability.api import standard_window_fns_pb2
from apache_beam.utils import proto_utils
from apache_beam.utils import urns
from apache_beam.utils.timestamp import MIN_TIMESTAMP, MAX_TIMESTAMP
from apache_beam.utils.timestamp import Duration

from apache_beam.transforms.window import WindowFn, IntervalWindow


_logger = logging.getLogger(__name__)


def format_timestamp(t):
    try:
        return str(datetime.datetime.fromtimestamp(t))
    except ValueError:
        return 'MAX'


def is_final(x):
    return isinstance(x, tuple) and len(x) == 2 and not x[1]


class CustomWindow(WindowFn):
    """
    A windowing function that groups elements into sessions.

    A session is defined as a series of consecutive events
    separated by a specified gap size.

    Attributes:
        gap_size: Size of the gap between windows as floating-point seconds.
    """

    def __init__(self, gap_size):
        if gap_size <= 0:
            raise ValueError('The size parameter must be strictly positive.')
        self.gap_size = Duration.of(gap_size)

    def assign(self, context):
        timestamp = context.timestamp
        _logger.info("ASSIGN: %s %s" %
                     (context.element, format_timestamp(timestamp)))
        if is_final(context.element):
            return [IntervalWindow(timestamp, MAX_TIMESTAMP)]
        else:
            return [IntervalWindow(timestamp, timestamp + self.gap_size)]

    def get_window_coder(self):
        return coders.IntervalWindowCoder()

    # def merge(self, merge_context):
    #     logging.info("%d windows" % len(merge_context.windows))
    #     start = min([w.start for w in merge_context.windows])
    #     end = min([w.start for w in merge_context.windows])
    #     merge_context.merge(merge_context.windows, IntervalWindow(start, end))

    def merge(self, merge_context):
        to_merge = []
        end = MIN_TIMESTAMP
        _logger.info("%d windows" % len(merge_context.windows))
        for w in sorted(merge_context.windows, key=lambda w: w.start):
            _logger.info("WINDOW: (%s, %s)" %
                         (format_timestamp(w.start), format_timestamp(w.end)))
            if to_merge:
                if end > w.start:
                    # window `w` overlaps with `to_merge`: add it
                    to_merge.append(w)
                    if w.end == MAX_TIMESTAMP:
                        _logger.info("FINAL: (%s, %s)" %
                                     (format_timestamp(to_merge[0].start),
                                      format_timestamp(end)))
                        # we don't want any more windows on this key
                        end = w.start
                        break
                    elif w.end > end:
                        end = w.end
                else:
                    # FIXME: this check seems superfluous
                    if len(to_merge) > 1:
                        _logger.info("NEW: (%s, %s)" %
                                     (format_timestamp(to_merge[0].start),
                                      format_timestamp(end)))
                        merge_context.merge(to_merge,
                                            IntervalWindow(to_merge[0].start, end))
                    to_merge = [w]
                    end = w.end
            else:
                to_merge = [w]
                end = w.end
        if len(to_merge) > 1:
            _logger.info("NEW: (%s, %s)" %
                         (format_timestamp(to_merge[0].start),
                          format_timestamp(end)))
            merge_context.merge(to_merge, IntervalWindow(to_merge[0].start, end))

    def __eq__(self, other):
        if type(self) == type(other) == CustomWindow:
            return self.gap_size == other.gap_size

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.gap_size)

    def to_runner_api_parameter(self, context):
        return (common_urns.session_windows.urn,
                standard_window_fns_pb2.SessionsPayload(
                    gap_size=proto_utils.from_micros(
                        duration_pb2.Duration, self.gap_size.micros)))

    @urns.RunnerApiFn.register_urn(
        common_urns.session_windows.urn,
        standard_window_fns_pb2.SessionsPayload)
    def from_runner_api_parameter(fn_parameter, unused_context):
        return CustomWindow(
          gap_size=Duration(micros=fn_parameter.gap_size.ToMicroseconds()))
