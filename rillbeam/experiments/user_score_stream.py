from __future__ import absolute_import

import time

from termcolor import cprint

import apache_beam as beam
from apache_beam.runners.runner import PipelineState

from rillbeam.helpers import pubsub_interface
from . import user_score

# Google pubsub topics and subscriptions
# create topic:
#   gcloud pubsub topics create rillbeam-user_score-outflow
INPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-user_score-inflow'
OUTPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-user_score-outflow'
# create subscription:
#  gcloud pubsub subscriptions create rillbeam-user_score-outflow --topic rillbeam-user_score-outflow
SUBSCRIPTION_PATH = 'projects/dataflow-241218/subscriptions/rillbeam-user_score-outflow'


SAMPLE_DATA = [
  'user3,team3,3,1447679463000,2015-11-16 13:11:03.000',
  'user4,team3,2,1447683063000,2015-11-16 14:11:03.000',
  'user1,team1,18,1447683063000,2015-11-16 14:11:03.000',
  'user1,team1,18,1447683063000,2015-11-16 14:11:03.000',
  # 2 hour gap
  'user2,team2,2,1447690263000,2015-11-16 16:11:03.000',
  # end user2
  'user3,team3,8,1447690263000,2015-11-16 16:11:03.000',
  'user3,team3,5,1447690263000,2015-11-16 16:11:03.000',
  # end user3
  'user4,team3,5,1447690263000,2015-11-16 16:11:03.000',
  # end user4
  'user1,team1,14,1447697463000,2015-11-16 18:11:03.000',
  # end user1
]


def main(options):
    from rillbeam.transforms import Log

    def format_result(kv):
        key, value = kv
        return '%s: %s' % (key, value)

    pipe = beam.Pipeline(options=options)
    (
        pipe
        | 'PubSubInflow' >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC)
        | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
        | user_score.UserScore()
        | 'PostLog' >> Log()
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
        pubsub_interface(SUBSCRIPTION_PATH, INPUT_TOPIC,
                         initial_data=SAMPLE_DATA)
    finally:
        print
        cprint('Shutting down pipeline...', 'yellow', attrs=['bold'])
        result.cancel()
        print


if __name__ == '__main__':
    from rillbeam.helpers import get_options
    pipeline_args, _ = get_options(
        __name__, None,
        'streaming',
    )
    main(pipeline_args)
