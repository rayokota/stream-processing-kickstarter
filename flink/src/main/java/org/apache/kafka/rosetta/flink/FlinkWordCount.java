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

package org.apache.kafka.rosetta.flink;

import java.util.Arrays;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.rosetta.WordCount;

public class FlinkWordCount implements WordCount {

  @Override
  public void countWords(
      String bootstrapServers, String zookeeperConnect,
      String inputTopic, String outputTopic) throws Exception {

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("group.id", "testgroup");
    props.put("auto.offset.reset", "earliest");

    // set up the execution environment
    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> input = env
        .addSource(new FlinkKafkaConsumer011<>(
            inputTopic,
            new SimpleStringSchema(),
            props
        ));

    DataStream<Tuple2<String, Long>> counts = input
        .map(line -> line.toLowerCase().split("\\W+"))
        .flatMap((String[] tokens, Collector<Tuple2<String, Long>> out) -> {
          // emit the pairs with non-zero-length words
          Arrays.stream(tokens)
              .filter(t -> t.length() > 0)
              .forEach(t -> out.collect(new Tuple2<>(t, 1L)));
        })
        .keyBy(0)
        .sum(1);

    counts.addSink(
        new FlinkKafkaProducer011<>(
            outputTopic,
            new WordCountSerializer(outputTopic),
            props
        ));

    // execute program
    env.execute("Streaming WordCount Example");
  }

  @Override
  public void close() {
  }

  static class WordCountSerializer implements
      KeyedSerializationSchema<Tuple2<String, Long>>,
      java.io.Serializable {

    private final String outputTopic;

    public WordCountSerializer(String outputTopic) {
      this.outputTopic = outputTopic;
    }

    public byte[] serializeKey(Tuple2<String, Long> element) {
      return new StringSerializer().serialize(null, element.getField(0));
    }

    public byte[] serializeValue(Tuple2<String, Long> element) {
      return new LongSerializer().serialize(null, element.getField(1));
    }

    public String getTargetTopic(Tuple2<String, Long> element) {
      return outputTopic;
    }
  }
}
