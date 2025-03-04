package com.thinkport.streams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableRetry
@EnableScheduling
public class AdvancedFeaturesApp {
    public static void main(String[] args) {
        SpringApplication.run(AdvancedFeaturesApp.class, args);
    }
}
