/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.rosetta.kstreams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.rosetta.Utils;
import org.apache.kafka.rosetta.WordCount;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class KafkaStreamsWordCount implements WordCount {

  private KafkaStreams streams;

  @Override
  public void countWords(
      String bootstrapServers, String zookeeperConnect,
      String inputTopic, String outputTopic) {

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
    props.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-example-client");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName()
    );
    props.put(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName()
    );
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    props.put(
        StreamsConfig.STATE_DIR_CONFIG,
        Utils.tempDirectory().getAbsolutePath()
    );

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> input = builder.stream(inputTopic);

    KTable<String, Long> wordCounts = input
        .flatMapValues(
            value -> Arrays.asList(value.toLowerCase().split("\\W+")))
        .groupBy((key, word) -> word)
        .count();

    wordCounts.toStream().to(
        outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

    streams = new KafkaStreams(builder.build(), props);
    streams.start();
  }

  @Override
  public void close() {
    streams.close();
  }
}
