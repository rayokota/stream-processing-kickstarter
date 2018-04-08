/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.rosetta.beam;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.rosetta.WordCount;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class BeamWordCount implements WordCount {

  @Override
  public void countWords(
      String bootstrapServers, String zookeeperConnect,
      String inputTopic, String outputTopic) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    KafkaIO.Read<String, String> reader = KafkaIO.<String, String>read()
        .withBootstrapServers(bootstrapServers)
        .withTopic(inputTopic)
        .withKeyDeserializer(StringDeserializer.class)
        .withValueDeserializer(StringDeserializer.class)
        .updateConsumerProperties(
            ImmutableMap.of(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));

    PCollection<String> input = p.apply(reader.withoutMetadata())
        .apply(MapElements
            .into(TypeDescriptors.strings())
            .via(KV::getValue))
        .apply(ParDo.of(new AddTimestampFn()));

    PCollection<String> windowedWords = input.apply(
        Window.<String>into(FixedWindows.of(Duration.standardSeconds(1)))
            .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
            .withAllowedLateness(Duration.ZERO)
            .accumulatingFiredPanes());

    PCollection<KV<String, Long>> wordCounts =
        windowedWords.apply(new CountWords());

    wordCounts.apply(KafkaIO.<String, Long>write()
        .withBootstrapServers(bootstrapServers)
        .withTopic(outputTopic)
        .withKeySerializer(StringSerializer.class)
        .withValueSerializer(LongSerializer.class));

    p.run().waitUntilFinish(Duration.standardSeconds(5));
  }

  @Override
  public void close() {
  }

  static class AddTimestampFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.outputWithTimestamp(c.element(), Instant.now());
    }
  }

  static class ExtractWordsFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      // Split the line into words.
      String[] words = c.element().toLowerCase().split("\\W+");

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  static class CountWords extends PTransform<PCollection<String>,
      PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
          ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
          words.apply(Count.perElement());

      return wordCounts;
    }
  }
}
