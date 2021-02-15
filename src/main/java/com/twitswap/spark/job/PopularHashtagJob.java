package com.twitswap.spark.job;

import com.twitswap.spark.config.KafkaConsumerConfig;
import com.twitswap.spark.config.KafkaConsumerProperties;
import com.twitswap.spark.config.KafkaProducerConfig;
import com.twitswap.spark.service.SparkService;
import com.twitswap.spark.util.HashTagsUtils;
import com.twitswap.spark.util.KafkaProducerUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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

@Service
public class PopularHashtagJob extends SparkJob {
  private final Logger log = LoggerFactory.getLogger(SparkService.class);

  public PopularHashtagJob(SparkConf sparkConf, KafkaConsumerConfig kafkaConsumerConfig, KafkaConsumerProperties kafkaConsumerProperties) {
    super(sparkConf, kafkaConsumerConfig, kafkaConsumerProperties);
  }

  @Override
  public void run() {
    // Create kafka producer
    KafkaProducer<String, String> kafkaProducer = KafkaProducerUtils.createKafkaProducer();

    // Create context with a 10 seconds batch interval
    JavaStreamingContext jssc = new JavaStreamingContext(getSparkConf(), Durations.seconds(10));

    // Create direct kafka stream with brokers and topics
    JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
            jssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(getTopics(), getKafkaConsumerConfig().consumerConfigs()));

    // Get the lines, split them into words, count the words and print
    JavaDStream<String> tweets = messages.map(ConsumerRecord::value);

    // Count the tweets and print
    tweets.count()
            .map(cnt -> "Popular hashtags in last 10 seconds (" + cnt + " total tweets):")
            .print();

    // Parse hashtags using the util
    tweets.flatMap(HashTagsUtils::hashTagsFromTweet)
            .mapToPair(hashTag -> new Tuple2<>(hashTag, 1))
            .reduceByKey(Integer::sum)
            .mapToPair(Tuple2::swap)
            .foreachRDD(rrdd -> {
              final String[] popularHashtagsString = {""};

              rrdd.sortByKey(false).collect()
                      .forEach(record -> {
                        String hashtagString = String.format("%s|%d\n", record._2, record._1);
                        popularHashtagsString[0] = popularHashtagsString[0].concat(hashtagString);
                        log.info(hashtagString);
                      });

              kafkaProducer.send(new ProducerRecord<>(KafkaProducerConfig.POPULAR_HASHTAG_TOPIC, null, popularHashtagsString[0]));
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
