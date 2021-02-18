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

import java.util.Collection;
import java.util.Collections;

@Service
public class SparkJob {
  private final Logger log = LoggerFactory.getLogger(SparkService.class);

  private final SparkConf sparkConf;

  private final KafkaConsumerConfig kafkaConsumerConfig;

  private final KafkaConsumerProperties kafkaConsumerProperties;

  private final Collection<String> topics;

  private int tweetCount = 0;

  public SparkJob(SparkConf sparkConf, KafkaConsumerConfig kafkaConsumerConfig, KafkaConsumerProperties kafkaConsumerProperties) {
    this.sparkConf = sparkConf;
    this.kafkaConsumerConfig = kafkaConsumerConfig;
    this.kafkaConsumerProperties = kafkaConsumerProperties;
    this.topics = Collections.singletonList(kafkaConsumerProperties.getTemplate().getDefaultTopic());
  }

  public void run() {
    // Create kafka producer
    KafkaProducer<String, String> kafkaProducer = KafkaProducerUtils.createKafkaProducer();

    // Create context with a 10 seconds batch interval
    JavaStreamingContext jssc = new JavaStreamingContext(this.sparkConf, Durations.seconds(10));

    // Check reset message
//    JavaInputDStream<ConsumerRecord<String, String>> resetMessages = KafkaUtils.createDirectStream(
//            jssc,
//            LocationStrategies.PreferConsistent(),
//            ConsumerStrategies.Subscribe(Collections.singleton("reset-value-topic"), this.kafkaConsumerConfig.consumerConfigs()));
//    JavaDStream<String> resetValues = resetMessages.map(ConsumerRecord::value);
//    resetValues.foreachRDD(rrdd -> {
//      if ((int) rrdd.count() > 0) {
//        this.tweetCount = 0;
//        log.info("reset, tweetCount: " + this.tweetCount);
//      }
//    });

    // Create direct kafka stream with brokers and topics
    JavaInputDStream<ConsumerRecord<String, String>> tweetMessages = KafkaUtils.createDirectStream(
            jssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(this.topics, this.kafkaConsumerConfig.consumerConfigs()));

    // Get the lines, split them into words, count the words and print
    JavaDStream<String> tweets = tweetMessages.map(ConsumerRecord::value);

    // Count the tweets and print
    tweets.count()
            .map(cnt -> "Preprocessed tweets in last 10 seconds (" + cnt + " total tweets):")
            .print();

    // POPULAR HASHTAGS
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

    // TOTAL TWEET
    tweets.foreachRDD(rrdd -> {
      int newTweetCount = this.tweetCount + (int) rrdd.count();

      if (this.tweetCount != newTweetCount) {
        this.tweetCount = newTweetCount;
        log.info("tweetCount: " + this.tweetCount);
        kafkaProducer.send(new ProducerRecord<>(KafkaProducerConfig.TWEET_COUNT_TOPIC, null, String.valueOf(this.tweetCount)));
      }
    });

    // Start the computation
    jssc.start();

    try {
      jssc.awaitTermination();
    } catch (InterruptedException e) {
      log.error("Interrupted: {}", e);
      // Restore interrupted state...
    }
  };
}
