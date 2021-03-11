package com.twitswap.spark.config;

public class KafkaProducerConfig {
  public static final String BOOTSTRAPSERVERS  = "127.0.0.1:9092";
  public static final String METRICS_COUNT_TOPIC = "public-metrics-topic";
  public static final String POPULAR_HASHTAG_TOPIC = "popular-hashtag-topic";
  public static final String TWEET_COUNT_TOPIC = "tweet-count-topic";
  public static final String ACKS_CONFIG = "all";
  public static final String MAX_IN_FLIGHT_CONN = "5";
  public static final String COMPRESSION_TYPE = "snappy";
  public static final String RETRIES_CONFIG = Integer.toString(Integer.MAX_VALUE);
  public static final String LINGER_CONFIG = "20";
  public static final String BATCH_SIZE = Integer.toString(32*1024);
}
