package com.thinkport.streams.topology;

import com.thinkport.producer.model.ClickJson;
import com.thinkport.streams.config.CustomSerdes;
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
public class StatelessTopoplogy {
    private final int ADMIN_CLICK_ID = 9000;
    private final int LAST_VALID_HTTP_RESPONSE = 201;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka-topics.clicks-in}")
    private String clicksTopicIn;

    @Value("${kafka-topics.clicks-out}")
    private String clicksTopicOut;

    @Bean
    public KStream<String, ClickJson> storeStream(StreamsBuilder kStreamBuilder) {
        KStream<String,ClickJson> stream = kStreamBuilder.stream(clicksTopicIn, Consumed.with(Serdes.String(), new JsonSerde<>(ClickJson.class)));
        stream.peek((k,v)->LOG.info("Key: " + k + ", Value: " + v ))
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
                }).to(clicksTopicOut, Produced.with(Serdes.String(), CustomSerdes.getClickSerde(getSchemaProperties())));
         return stream;
    }

    @Bean
    public KStream<String, ClickJson> storeStreamWithoutError(StreamsBuilder kStreamBuilder) {
        KStream<String,ClickJson> stream = kStreamBuilder.stream(clicksTopicIn, Consumed.with(Serdes.String(), new JsonSerde<>(ClickJson.class)));
        stream.peek((k,v)->LOG.info("Key: " + k + ", Value: " + v ))
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
                })
                .filter((k, v) -> Integer.valueOf(v.getUserId()) != ADMIN_CLICK_ID)
                .peek((k, v) -> LOG.info("Key: " + k + ", Value: " + v.toString()))
                .filter((k, v) -> Integer.valueOf(v.getRequest()) > LAST_VALID_HTTP_RESPONSE )
                .peek((k, v) -> LOG.info("Key: " + k + ", Value: " + v.toString()));
        return stream;
    }

    private Properties getSchemaProperties(){
        Properties properties = new Properties();
        properties.put("schema.registry.url",schemaRegistryUrl);
        return  properties;
    }
}
