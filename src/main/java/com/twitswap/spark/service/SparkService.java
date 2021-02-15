package com.twitswap.spark.service;

import com.twitswap.spark.job.PopularHashtagJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class SparkService {
  private final Logger log = LoggerFactory.getLogger(SparkService.class);

  private final PopularHashtagJob popularHashtagJob;

  public SparkService(PopularHashtagJob popularHashtagJob) {
    this.popularHashtagJob = popularHashtagJob;
  }

  public void run() {
    log.debug("Running Spark Consumer Service..");

    this.popularHashtagJob.run();
  }
}
