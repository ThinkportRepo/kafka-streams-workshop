package com.thinkport.producer.resource;

import digital.thinkport.avro.Address;
import digital.thinkport.avro.User;
import lombok.AllArgsConstructor;
import net.datafaker.Faker;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@AllArgsConstructor
public class UserAvroResource {

    /*
    TODO compacted topic mit Kafka admin client anlegen.
     */

    private final KafkaTemplate<String, User> kafkaTemplate;
    private static final String TOPIC = "shop.users";
    private final Faker faker = new Faker();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ConcurrentHashMap<Integer, User> users = new ConcurrentHashMap<>();


    //@Scheduled(fixedRate = 1000)
    public void send() {

        if (initialized.get()) {
            User currentUser = users.get(faker.number().numberBetween(0, 500));
            if (faker.bool().bool()) {
                currentUser.setMail(faker.internet().emailAddress(String.valueOf(currentUser.getName())));
            }
            if (faker.bool().bool()) {

                Address addressUpdate = Address.newBuilder()
                        .setStreet(faker.address().streetAddress())
                        .setHouseNr(faker.address().buildingNumber())
                        .setZipCode(faker.address().zipCode())
                        .build();
                currentUser.setAddress(addressUpdate);
            }
            if (faker.bool().bool()) {
                currentUser.setPhone(faker.phoneNumber().phoneNumber());
            }
            kafkaTemplate.send(new ProducerRecord<>(TOPIC, String.valueOf(currentUser.getID()), currentUser));
            return;
        }

        for (int i = 0; i <= 500; i++){
            String name = faker.rickAndMorty().character() + faker.elderScrolls().lastName();
            Address address = Address.newBuilder()
                    .setStreet(faker.address().streetAddress())
                    .setHouseNr(faker.address().buildingNumber())
                    .setZipCode(faker.address().zipCode())
                    .build();

            User user = User.newBuilder()
                    .setID(String.valueOf(i))
                    .setName(name)
                    .setMail(faker.internet().emailAddress(name))
                    .setAddress(address)
                    .setPhone(faker.phoneNumber().cellPhone())
                    .build();


            kafkaTemplate.send(new ProducerRecord<>(TOPIC, String.valueOf(i), user));
            System.out.println("Gesendet: " + user);
            users.put(i, user);
        }
        initialized.set(true);
    }
}
