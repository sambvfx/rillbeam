import sys

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import apache_beam.transforms.trigger as trigger
import apache_beam.transforms.window as window


def test_flowbased():
    from rillbeam.components import Sleep, Log

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipe = beam.Pipeline(options=pipeline_options)

    (
        pipe
        | 'begin' >> beam.Create(range(10))
        | 'Sleep' >> beam.ParDo(Sleep(), duration=1.0)
        | 'Log' >> beam.ParDo(Log())
    )

    result = pipe.run()
    result.wait_until_finish()


if __name__ == '__main__':

    tests = {k: v for k, v in globals().items()
             if k.startswith('test') and callable(v)}

    fltr = sys.argv[1:]

    for name, func in tests.items():
        if fltr and name not in fltr:
            continue
        print
        print '-' * 80
        print name
        print '-' * 80
        func()
