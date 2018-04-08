/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.rosetta.trident;

import java.util.Properties;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.rosetta.WordCount;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaStateUpdater;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TridentWordCount implements WordCount {

  private LocalCluster cluster;

  @Override
  public void countWords(
      String bootstrapServers, String zookeeperConnect,
      String inputTopic, String outputTopic) {

    KafkaSpoutConfig<String, String> config =
        KafkaSpoutConfig.builder(bootstrapServers, inputTopic).build();
    KafkaTridentSpoutOpaque<String, String> spout =
        new KafkaTridentSpoutOpaque<>(config);

    TridentTopology topology = new TridentTopology();
    Stream wordCounts = topology.newStream("spout1", spout)
        .each(new Fields("value"), new Split(), new Fields("word"))
        .groupBy(new Fields("word"))
        .persistentAggregate(
            new MemoryMapState.Factory(), new Count(), new Fields("count"))
        .newValuesStream();

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("acks", "1");
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", LongSerializer.class.getName());

    TridentKafkaStateFactory<String, Long> stateFactory =
        new TridentKafkaStateFactory<String, Long>()
            .withProducerProperties(props)
            .withKafkaTopicSelector(new DefaultTopicSelector(outputTopic))
            .withTridentTupleToKafkaMapper(
                new FieldNameBasedTupleToKafkaMapper<String, Long>(
                    "word","count"));
    wordCounts.partitionPersist(
        stateFactory,
        new Fields("word", "count"),
        new TridentKafkaStateUpdater(),
        new Fields()
    );

    Config conf = new Config();
    cluster = new LocalCluster();
    cluster.submitTopology("wordCount", conf, topology.build());
  }

  @Override
  public void close() {
    cluster.shutdown();
  }

  static class Split extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String sentence = tuple.getString(0);
      for (String word : sentence.toLowerCase().split("\\W+")) {
        collector.emit(new Values(word));
      }
    }
  }
}
