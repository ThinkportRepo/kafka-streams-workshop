package com.thinkport.avro.config;

import digital.thinkport.avro.ClickAvro;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

@EnableKafka
@Configuration
public class ConsumerConfiguration {
    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public Map<String, Object> consumerConfigAvro() {
        Map<String, Object> props =
                new HashMap<>(kafkaProperties.buildConsumerProperties());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
    @Bean
    public ConsumerFactory<String, Object> clickAvroConsumerFactory() {
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
        kafkaAvroDeserializer.configure(consumerConfigAvro(),false);
        return new DefaultKafkaConsumerFactory<>(consumerConfigAvro(), new StringDeserializer(),
                kafkaAvroDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ClickAvro> clickAvroConcurrentKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ClickAvro> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(clickAvroConsumerFactory());
        return factory;
    }


}
