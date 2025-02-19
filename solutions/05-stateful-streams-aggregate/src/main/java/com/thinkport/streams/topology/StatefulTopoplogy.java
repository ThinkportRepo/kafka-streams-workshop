package com.thinkport.streams.topology;

import com.thinkport.producer.model.ClickJson;
import com.thinkport.streams.config.CustomSerdes;
import digital.thinkport.avro.CartItem;
import digital.thinkport.avro.ClickAvro;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
@Slf4j
public class StatefulTopoplogy {

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka-topics.shop-cart-in}")
    private String clicksTopicIn;

    @Value("${kafka-topics.shop-cart-out}")
    private String clicksTopicOut;

    @Bean
    public KStream<String, CartItem> cartItemStream(StreamsBuilder kStreamBuilder) {
        KStream<String,CartItem> stream = kStreamBuilder.stream(clicksTopicIn, Consumed.with(Serdes.String(), CustomSerdes.getCartItemSerde(getSchemaProperties())));
        stream.peek((k,v)->LOG.info("Key: " + k + ", Value: " + v ));
                /*
                .mapValues(clickJson-> {
                    return ClickAvro.newBuilder()
                            .setClickId(clickJson.getClickId())
                            .setBytes(clickJson.getBytes())
                            .setIp(clickJson.getIp())
                            .setKnownIp(clickJson.isKnownIp())
                            .setUserId(clickJson.getUserId())
                            .setProductId(clickJson.getProductId())
                            .setReferrer(clickJson.getReferrer())
                            .setStatus(clickJson.getStatus())
                            .setRequest(clickJson.getRequest())
                            .setUserAgent(clickJson.getUserAgent())
                            .build();
                }).to(clicksTopicOut, Produced.with(Serdes.String(), CustomSerdes.getCartItemSerde(getSchemaProperties())));

         */
         return stream;
    }


    private Properties getSchemaProperties(){
        Properties properties = new Properties();
        properties.put("schema.registry.url",schemaRegistryUrl);
        return  properties;
    }
}
