package com.thinkport.avro;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class AvroApplication {
  public static void main(String[] args) {
    SpringApplication.run(AvroApplication.class, args);
  }
}
