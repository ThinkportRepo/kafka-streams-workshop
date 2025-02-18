package com.thinkport.producer.config;

import com.example.avro.ArticleChange;
import com.thinkport.producer.model.ClickJson;
import com.thinkport.producer.model.User;
import com.thinkport.producer.resource.ShoppingCartAggregateResource;
import digital.thinkport.avro.Article;
import digital.thinkport.avro.CartItem;
import digital.thinkport.avro.OrderPlaced;
import digital.thinkport.avro.ShoppingCartAggregate;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ProducerConfiguration {

    /* TODO

     */
    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props =
                new HashMap<>(kafkaProperties.buildProducerProperties());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return props;
    }

    private  <T> ProducerFactory<String, T> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    private  <T> KafkaTemplate<String, T> createKafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public KafkaTemplate<String, User> kafkaTemplateUser() {
        return createKafkaTemplate();
    }

    @Bean
    public KafkaTemplate<String, ClickJson> kafkaTemplateClickJson() {
        return createKafkaTemplate();
    }

    @Bean
    public <T> ProducerFactory<String, T> avroProducerFactory() {
        Map<String, Object> props = new HashMap<>(producerConfigs());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, ArticleChange> kafkaTemplateArticleChange() {
        return new KafkaTemplate<>(avroProducerFactory());
    }

    @Bean
    public KafkaTemplate<String, digital.thinkport.avro.User> kafkaTemplateUserAvro() {
        return new KafkaTemplate<>(avroProducerFactory());
    }


    @Bean
    public KafkaTemplate<String, ShoppingCartAggregate> kafkaTemplateShoppingCartAggregate() {
        return new KafkaTemplate<>(avroProducerFactory());
    }

    @Bean
    public KafkaTemplate<String, CartItem> kafkaTemplateShoppingCart() {
        return new KafkaTemplate<>(avroProducerFactory());
    }


    @Bean
    public KafkaTemplate<String, OrderPlaced> kafkaTemplateOrderPlaced() {
        return new KafkaTemplate<>(avroProducerFactory());
    }

    @Bean
    public KafkaTemplate<String, Article> kafkaTemplateArticle() {
        return new KafkaTemplate<>(avroProducerFactory());
    }

}
