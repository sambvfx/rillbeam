package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// Basic test for Reading PubSub messages.
// Send pubsub messages via python or other:
//
// # python
// from google.cloud import pubsub_v1
// publisher = pubsub_v1.PublisherClient()
// publisher.publish('projects/render-1373/topics/farm-output', data=b'foo')


public class PubSub {

    public interface Options extends PubsubOptions {

        @Description("Pub/Sub topic to read from")
        @Default.String("projects/render-1373/topics/farm-output")
        String getTopic();

        void setTopic(String value);
    }

    static class LogFn extends DoFn<String, String> {

        private static final Logger LOG = LoggerFactory.getLogger(LogFn.class);

        @ProcessElement
        public void processElement(ProcessContext c) {
            String element = c.element();
            LOG.info(element);
            c.output(element);
        }
    }

    public static class Log
            extends PTransform<PCollection<String>, PCollection<String>> {
        @Override
        public PCollection<String> expand(PCollection<String> pcoll) {
            return pcoll.apply(ParDo.of(new LogFn()));
        }
    }

    public static void main(String[] args) throws Exception {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        // Enforce that this pipeline is always run in streaming mode.
        options.setStreaming(true);
        options.as(PubsubOptions.class).setProject("render-1373");

        Pipeline pipe = Pipeline.create(options);

        pipe
            .apply(PubsubIO.readStrings().fromTopic(options.getTopic()))
//            .apply(Create.of("Hello", "World"))
            .apply(new Log());

        pipe.run().waitUntilFinish();
    }
}
