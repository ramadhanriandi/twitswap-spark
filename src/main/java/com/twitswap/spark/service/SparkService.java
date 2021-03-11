package com.twitswap.spark.service;

import com.twitswap.spark.job.SparkJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class SparkService {
  private final Logger log = LoggerFactory.getLogger(SparkService.class);

  private final SparkJob sparkJob;

  public SparkService(SparkJob sparkJob) {
    this.sparkJob = sparkJob;
  }

  public void run() {
    log.debug("Running Spark Consumer Service..");

    this.sparkJob.run();
  }
}
