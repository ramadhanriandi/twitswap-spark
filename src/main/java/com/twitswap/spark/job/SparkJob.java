package com.twitswap.spark.job;

import com.twitswap.spark.config.KafkaConsumerConfig;
import com.twitswap.spark.config.KafkaConsumerProperties;
import lombok.Data;
import org.apache.spark.SparkConf;

import java.util.Collection;
import java.util.Collections;

@Data
public abstract class SparkJob {
  private final SparkConf sparkConf;

  private final KafkaConsumerConfig kafkaConsumerConfig;

  private final KafkaConsumerProperties kafkaConsumerProperties;

  private final Collection<String> topics;

  public SparkJob(SparkConf sparkConf, KafkaConsumerConfig kafkaConsumerConfig, KafkaConsumerProperties kafkaConsumerProperties) {
    this.sparkConf = sparkConf;
    this.kafkaConsumerConfig = kafkaConsumerConfig;
    this.kafkaConsumerProperties = kafkaConsumerProperties;
    this.topics = Collections.singletonList(kafkaConsumerProperties.getTemplate().getDefaultTopic());
  }

  public abstract void run();
}
