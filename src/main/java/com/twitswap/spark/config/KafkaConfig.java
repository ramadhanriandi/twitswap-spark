package com.twitswap.spark.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

//@EnableKafka
@Configuration
public class KafkaConfig {
  private final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

  private static final String AUTO_OFFSET_RESET_CONFIG = "earliest";

  private static final String CLIENT_ID = "earliest";

  private final KafkaProperties kafkaProperties;

  public KafkaConfig(KafkaProperties kafkaProperties) {
    this.kafkaProperties = kafkaProperties;
  }

  @Bean
  public Map<String, Object> consumerConfigs() {
    Map<String, Object> props = new HashMap<>();

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaProperties.getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, this.kafkaProperties.getConsumer().getGroupId());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_CONFIG);
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);

    return props;
  }

  @Bean
  public ConsumerFactory<String, String> consumerFactory() {
    return new DefaultKafkaConsumerFactory<>(consumerConfigs());
  }

  @Bean
  public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    return factory;
  }

//  @Bean
//  public ConsumerFactory<String, String> consumerFactory() {
//    Map<String, Object> config = new HashMap<>();
//
//    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
//    config.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id");
//    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//    return new DefaultKafkaConsumerFactory<>(config);
//  }
//
//  @Bean
//  public ConcurrentKafkaListenerContainerFactory concurrentKafkaListenerContainerFactory() {
//    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//    factory.setConsumerFactory(consumerFactory());
//
//    return factory;
//  }
}