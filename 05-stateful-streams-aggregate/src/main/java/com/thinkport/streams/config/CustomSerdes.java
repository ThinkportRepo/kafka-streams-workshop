package com.thinkport.streams.config;

import digital.thinkport.avro.Article;
import digital.thinkport.avro.CartItem;
import digital.thinkport.avro.ClickAvro;
import digital.thinkport.avro.ShoppingCartAggregate;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CustomSerdes {

    public static SpecificAvroSerde<CartItem> getCartItemSerde(Properties properties) {
        return getGenericSerde(properties);
    }
    public static SpecificAvroSerde<Article> getArticleSerde(Properties properties) {
        return getGenericSerde(properties);
    }
    public static SpecificAvroSerde<ShoppingCartAggregate> getCartItemAggregatSerde(Properties properties) {
        return getGenericSerde(properties);
    }
    private static <T extends SpecificRecord> SpecificAvroSerde<T> getGenericSerde(Properties properties){
        final Map<String, String> genericSerdeConfig = new HashMap<>();
        genericSerdeConfig.put("schema.registry.url", properties.getProperty("schema.registry.url"));
        //genericSerdeConfig.put("basic.auth.credentials.source", properties.getProperty("basic.auth.credentials.source"));
        //genericSerdeConfig.put("basic.auth.user.info", properties.getProperty("basic.auth.user.info"));
        final SpecificAvroSerde<T> genericSerde = new SpecificAvroSerde<>();
        genericSerde.configure(genericSerdeConfig, false); // `false` for record values
        return genericSerde;
    }
}
