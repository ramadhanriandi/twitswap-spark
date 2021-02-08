package com.twitswap.spark.service;

import com.twitswap.spark.config.KafkaConsumerConfig;
import com.twitswap.spark.config.KafkaConsumerProperties;
import com.twitswap.spark.config.KafkaProducerConfig;
import com.twitswap.spark.util.HashTagsUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

@Service
public class SparkService {
  private final Logger log = LoggerFactory.getLogger(SparkService.class);

  private final SparkConf sparkConf;

  private final KafkaConsumerConfig kafkaConsumerConfig;

  private final KafkaConsumerProperties kafkaConsumerProperties;

  private final Collection<String> topics;

  private KafkaProducer<String, String> kafkaProducer;

  private KafkaProducer<String, String> createKafkaProducer() {
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

  public SparkService(SparkConf sparkConf, KafkaConsumerConfig kafkaConsumerConfig, KafkaConsumerProperties kafkaConsumerProperties) {
    this.sparkConf = sparkConf;
    this.kafkaConsumerConfig = kafkaConsumerConfig;
    this.kafkaConsumerProperties = kafkaConsumerProperties;
    this.topics = Arrays.asList(kafkaConsumerProperties.getTemplate().getDefaultTopic());
  }

  public void run() {
    log.debug("Running Spark Consumer Service..");

    this.kafkaProducer = createKafkaProducer();

    // Create context with a 10 seconds batch interval
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

    // Create direct kafka stream with brokers and topics
    JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
            jssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(topics, kafkaConsumerConfig.consumerConfigs()));

    // Get the lines, split them into words, count the words and print
    JavaDStream<String> lines = messages.map(ConsumerRecord::value);

    //Count the tweets and print
    lines.count()
            .map(cnt -> "Popular hash tags in last 60 seconds (" + cnt + " total tweets):")
            .print();

    lines.flatMap(HashTagsUtils::hashTagsFromTweet)
            .mapToPair(hashTag -> new Tuple2<>(hashTag, 1))
            .reduceByKey(Integer::sum)
            .mapToPair(Tuple2::swap)
            .foreachRDD(rrdd -> {
              log.info("---------------------------------------------------------------");

              final String[] popularHashtagsString = {""};

              rrdd.sortByKey(false).collect()
                      .forEach(record -> {
                        String hashtagString = String.format("%s|%d\n", record._2, record._1);
                        popularHashtagsString[0] = popularHashtagsString[0].concat(hashtagString);
                        log.info(hashtagString);
                      });

              this.kafkaProducer.send(new ProducerRecord<>(KafkaProducerConfig.TOPIC, null, popularHashtagsString[0]));
            });

    // Start the computation
    jssc.start();

    try {
      jssc.awaitTermination();
    } catch (InterruptedException e) {
      log.error("Interrupted: {}", e);
      // Restore interrupted state...
    }
  }
}
