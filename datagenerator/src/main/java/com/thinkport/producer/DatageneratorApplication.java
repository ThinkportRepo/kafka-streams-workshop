package com.thinkport.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DatageneratorApplication {
  public static void main(String[] args) {
    SpringApplication.run(DatageneratorApplication.class, args);
  }
}
