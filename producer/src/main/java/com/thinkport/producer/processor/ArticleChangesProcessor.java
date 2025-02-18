package com.thinkport.producer.processor;

import com.example.avro.ArticleChange;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class ArticleChangesProcessor {

    private final KafkaTemplate<String, ArticleChange> template;
    private final String ERROR_TOPIC = "";
    private final String TOPIC = "";


 //   @KafkaListener(topics = "aricle.changes", containerFactory = "articleChangeKafkaListenerFactory")
    public void consumeJson(ArticleChange articleChange) {
        System.out.println("Consumed JSON Message: " + articleChange.toString());
/*
        if (articleChange.getPrice() <= 0) {
            template.send(new ProducerRecord<>());
        }
        else if (articleChange.getStock() < 0){
            template.send(new ProducerRecord<>());
        }
 */

    }
}
