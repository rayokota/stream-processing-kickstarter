/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.rosetta.spark;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.rosetta.Utils;
import org.apache.kafka.rosetta.WordCount;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;

public class SparkWordCount implements WordCount, java.io.Serializable {

  private StreamingQuery query;

  @Override
  public void countWords(
      String bootstrapServers, String zookeeperConnect,
      String inputTopic, String outputTopic) {

    SparkSession spark = SparkSession
        .builder()
        .appName("JavaStructuredKafkaWordCount")
        .config("spark.master", "local[*]")
        .getOrCreate();

    spark.udf().register(
        "serializeLong", new SparkLongSerializer(), DataTypes.BinaryType);

    // Create DataSet representing the stream of input lines from kafka
    Dataset<String> lines = spark
        .readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", inputTopic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value as STRING)")
        .as(Encoders.STRING());

    // Generate running word count
    Dataset<Row> wordCounts = lines.flatMap(
        (FlatMapFunction<String, String>) x ->
            Arrays.asList(x.toLowerCase().split("\\W+"))
                .iterator(),
        Encoders.STRING()
    ).groupBy("value").count();

    // Start running the query that outputs the running counts to Kafka
    query = wordCounts
        .selectExpr("CAST(value AS STRING) key",
            "CAST(serializeLong(count) AS BINARY) value")
        .writeStream()
        .outputMode("update")
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("topic", outputTopic)
        .option("checkpointLocation", Utils.tempDirectory().getAbsolutePath())
        .start();
  }

  @Override
  public void close() {
    query.stop();
  }

  static class SparkLongSerializer implements UDF1<Long, byte[]> {
    @Override
    public byte[] call(Long i) {
      return new LongSerializer().serialize(null, i.longValue());
    }
  }
}
