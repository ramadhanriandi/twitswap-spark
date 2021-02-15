package com.twitswap.spark.util;

import com.twitswap.spark.config.KafkaProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerUtils {
  public static KafkaProducer<String, String> createKafkaProducer() {
    // Create producer properties
    Properties prop = new Properties();
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProducerConfig.BOOTSTRAPSERVERS);
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create safe Producer
    prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    prop.setProperty(ProducerConfig.ACKS_CONFIG, KafkaProducerConfig.ACKS_CONFIG);
    prop.setProperty(ProducerConfig.RETRIES_CONFIG, KafkaProducerConfig.RETRIES_CONFIG);
    prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, KafkaProducerConfig.MAX_IN_FLIGHT_CONN);

    // Additional settings for high throughput producer
    prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, KafkaProducerConfig.COMPRESSION_TYPE);
    prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, KafkaProducerConfig.LINGER_CONFIG);
    prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, KafkaProducerConfig.BATCH_SIZE);

    // Create producer
    return new KafkaProducer<>(prop);
  }
}
