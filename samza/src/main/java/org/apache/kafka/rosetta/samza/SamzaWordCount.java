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

package org.apache.kafka.rosetta.samza;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.rosetta.WordCount;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.LongSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;

public class SamzaWordCount implements WordCount {

  @Override
  public void countWords(
      String bootstrapServers, String zookeeperConnect,
      String inputTopic, String outputTopic) {

    Map<String, String> configs = new HashMap<>();

    configs.put(JobConfig.JOB_NAME(), "word-count");
    configs.put(JobConfig.PROCESSOR_ID(), "1");
    configs.put(
        JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY,
        PassthroughCoordinationUtilsFactory.class.getName()
    );
    configs.put(
        JobCoordinatorConfig.JOB_COORDINATOR_FACTORY,
        PassthroughJobCoordinatorFactory.class.getName()
    );
    configs.put(
        TaskConfig.GROUPER_FACTORY(),
        SingleContainerGrouperFactory.class.getName()
    );
    configs.put(
        "systems.kafka.samza.factory",
        "org.apache.samza.system.kafka.KafkaSystemFactory"
    );
    configs.put("systems.kafka.producer.bootstrap.servers", bootstrapServers);
    configs.put("systems.kafka.consumer.zookeeper.connect", zookeeperConnect);
    configs.put("systems.kafka.samza.offset.default", "oldest");
    configs.put("systems.kafka.default.stream.replication.factor", "1");
    configs.put("job.default.system", "kafka");

    LocalApplicationRunner runner =
        new LocalApplicationRunner(new MapConfig(configs));
    runner.run(new WordCountApplication(inputTopic, outputTopic));
    runner.waitForFinish();
  }

  @Override
  public void close() {
  }

  static class WordCountApplication implements StreamApplication {

    private String inputTopic;
    private String outputTopic;

    public WordCountApplication(String inputTopic, String outputTopic) {
      this.inputTopic = inputTopic;
      this.outputTopic = outputTopic;
    }

    @Override
    public void init(StreamGraph graph, Config config) {
      MessageStream<KV<String, String>> words =
          graph.getInputStream(
              inputTopic, KVSerde.of(new StringSerde(), new StringSerde()));
      OutputStream<KV<String, Long>> counts =
          graph.getOutputStream(
              outputTopic, KVSerde.of(new StringSerde(), new LongSerde()));

      words.flatMap(kv -> Arrays.asList(kv.value.toLowerCase().split("\\W+")))
          .window(Windows.keyedTumblingWindow(
              (String kv) -> kv, Duration.ofSeconds(1),
              () -> 0L, (m, prevCount) -> prevCount + 1,
              new StringSerde(), new LongSerde()
          )
              .setEarlyTrigger(Triggers.repeat(Triggers.count(1)))
              .setAccumulationMode(AccumulationMode.ACCUMULATING), "count")
          .map(windowPane -> {
            String word = windowPane.getKey().getKey();
            long count = windowPane.getMessage();
            return KV.of(word, count);
          })
          .sendTo(counts);
    }
  }
}
