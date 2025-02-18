package com.thinkport.producer.resource;

import digital.thinkport.avro.Address;
import digital.thinkport.avro.Article;
import digital.thinkport.avro.User;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import net.datafaker.Faker;
import net.datafaker.providers.base.Commerce;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class ArticleResource {

    /*
    TODO compacted topic mit Kafka admin client anlegen.
     */

    private final KafkaTemplate<String, Article> kafkaTemplate;
    private static final String TOPIC = "shop.articles";
    private final Faker faker = new Faker();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ConcurrentHashMap<Integer, User> users = new ConcurrentHashMap<>();


    //@Scheduled(fixedRate = 1000)
    public void send() {
/*
        if (!initialized.get()) {
            for (int i = 0; i <= 500; i++){
                Commerce commerce = faker.commerce();
                Article article = Article.newBuilder()
                        .setArticleID(String.valueOf(i))
                        .setName(commerce.productName())
                        .setCategory(commerce.department())
                        .setPrice(faker.number().numberBetween(1, 30))
                        .setDescription(faker.lorem().sentence())
                        .build();


                kafkaTemplate.send(new ProducerRecord<>(TOPIC, String.valueOf(i), article));
                System.out.println("Gesendet: " + article);
            }
            initialized.set(true);
            return;

        }

 */
    }
}
