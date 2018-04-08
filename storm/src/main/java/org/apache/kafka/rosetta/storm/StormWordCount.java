/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.rosetta.storm;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.rosetta.WordCount;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class StormWordCount implements WordCount {

  private LocalCluster cluster;

  @Override
  public void countWords(
      String bootstrapServers, String zookeeperConnect,
      String inputTopic, String outputTopic) {

    KafkaSpoutConfig<String, String> config =
        KafkaSpoutConfig.builder(bootstrapServers, inputTopic)
            .setRecordTranslator((r) ->
                new Values(r.value()), new Fields("value"))
            .build();
    KafkaSpout<String, String> spout = new KafkaSpout<>(config);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", spout);
    builder.setBolt("split", new SplitSentence(), 4).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 4)
        .fieldsGrouping("split", new Fields("word"));

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("acks", "1");
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", LongSerializer.class.getName());

    KafkaBolt<String, Long> bolt = new KafkaBolt<String, Long>()
        .withProducerProperties(props)
        .withTopicSelector(new DefaultTopicSelector(outputTopic))
        .withTupleToKafkaMapper(
            new FieldNameBasedTupleToKafkaMapper<>("word","count"));
    builder.setBolt("forwardToKafka", bolt, 8)
        .fieldsGrouping("count", new Fields("word"));

    Config conf = new Config();
    cluster = new LocalCluster();
    cluster.submitTopology("wordCount", conf, builder.createTopology());
  }

  @Override
  public void close() {
    cluster.shutdown();
  }

  static class SplitSentence extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String sentence = tuple.getString(0);

      for (String word : sentence.toLowerCase().split("\\W+")) {
        collector.emit(new Values(word, 1L));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  static class WordCount extends BaseBasicBolt {
    Map<String, Long> counts = new HashMap<>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Long count = counts.get(word);
      if (count == null) {
        count = 0L;
      }
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }
}
