package com.twitswap.spark;

import com.twitswap.spark.service.SparkService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkApplication implements CommandLineRunner {
	private final SparkService sparkService;

	public SparkApplication(SparkService sparkService) {
		this.sparkService = sparkService;
	}

	public static void main(String[] args) {
		SpringApplication.run(SparkApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		sparkService.run();
	}
}
