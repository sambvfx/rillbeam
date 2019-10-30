import time
from termcolor import cprint
import apache_beam as beam
from apache_beam.io.external.generate_sequence import GenerateSequence

from rillbeam.transforms import Log


# 1) Start flink cluster
# 2) Build sdk docker container for the expansion service:
#   ./gradlew -p sdks/java docker
# 3) Start runShadow (includes job server and expansion service):
#   ./gradlew -p runners/flink/1.8/job-server runShadow -P flinkMasterUrl=localhost:8081
# 4) Submit job:
#   python -m rillbeam.experiments.seqgen --defaults flink


def main(options):

    pipe = beam.Pipeline(options=options)

    (
        pipe
        # | 'Gen' >> beam.Create(range(10))
        | 'Gen' >> GenerateSequence(
              start=1,
              stop=22,
              expansion_service='localhost:8097',
          )
        | 'Log' >> Log()
    )

    print
    cprint('Starting streaming graph forever. Kill with ctrl+c',
           'red', attrs=['bold'])
    print

    result = pipe.run()
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print
        cprint('Shutting down...', 'yellow')
        result.cancel()


if __name__ == '__main__':
    from rillbeam.helpers import get_options
    # pipeline_args, _ = get_options(__name__, None, 'streaming')
    pipeline_args, _ = get_options(__name__, None)
    main(pipeline_args)
