package com.thinkport.producer.resource;

import com.example.avro.ArticleChange;
import com.example.avro.ChangeType;
import lombok.AllArgsConstructor;
import net.datafaker.Faker;
import net.datafaker.providers.base.Commerce;
import net.datafaker.providers.base.Options;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;

@Service
@AllArgsConstructor
public class ArtickleChangeResource {

    private final KafkaTemplate<String, ArticleChange> kafkaTemplate;
    private static final String TOPIC = "aricle.changes";
    private final Faker faker = new Faker();

    //@Scheduled(fixedRate = 1000)
    public void send() {
        Commerce product = faker.commerce();
        Options ops = faker.options();

        ArticleChange article = ArticleChange.newBuilder()
                .setArticleId(generateIdFromName(product.productName()))
                .setTimestamp(Instant.now().toString())
                .setChangeType(ops.option(ChangeType.class))
                .setName(product.productName())
                .setCategory(product.department())
                .setPrice(faker.number().numberBetween(-1, 500))
                .setStock(faker.number().numberBetween(-1, 1000))
                .setDescription(faker.lorem().sentence())
                .build();

        kafkaTemplate.send(new ProducerRecord<>(TOPIC, generateIdFromName(product.productName()), article));
        System.out.println("Gesendet: " + article);
    }
    private static String generateIdFromName(String name) {
        try {
            // SHA-256 Hash-Algorithmus verwenden
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(name.getBytes());
            StringBuilder hexString = new StringBuilder();
            for (byte b : hashBytes) {
                hexString.append(String.format("%02x", b));
            }
            return hexString.toString().substring(0, 8); // Kürze den Hash für die ID
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }
}
