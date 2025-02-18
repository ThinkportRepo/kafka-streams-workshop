package com.thinkport.producer.processor;


import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class ShoppingCartAggregateProccessor {
    private static final String SHOPPING_CART_TOPIC = "shopping_cart";
    private static final String ORDER_PLACED_TOPIC = "order_placed";
    private static final String ORDER_SUMMARY_TOPIC = "order_summary";

    @Bean
    public void processShoppingCart(StreamsBuilder builder) {

    }
}
