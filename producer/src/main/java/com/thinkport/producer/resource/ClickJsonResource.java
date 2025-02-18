package com.thinkport.producer.resource;

import ch.qos.logback.core.util.TimeUtil;
import com.thinkport.producer.model.ClickJson;
import lombok.AllArgsConstructor;
import lombok.ToString;
import net.datafaker.Faker;
import net.datafaker.providers.base.Commerce;
import org.rocksdb.Options;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.xml.datatype.DatatypeConstants;
import java.math.BigDecimal;
import java.time.Period;
import java.util.List;

@Service
@AllArgsConstructor
public class ClickJsonResource {

    private final KafkaTemplate<String, ClickJson> template;
    private final Faker faker = new Faker();

    private static final String TOPIC = "user.topic";

    //@Scheduled(fixedRate = 1000)
    public void send(){
        Commerce product = faker.commerce();
        ClickJson msg = ClickJson.
                builder().
                clickId(faker.internet().uuid()).
                userId(String.valueOf(faker.number().numberBetween(1,100))).
                ip(faker.internet().ipV4Address()).
                knownIp(faker.bool().bool()).
                //timestamp(faker.timeAndDate().past(1, TimeUnit.SECONDS)).
                request(faker.internet().httpMethod()).
                status(Integer.valueOf(faker.expression("#{options.option '200','201','400','404', '500'}")))
                .bytes(faker.number().positive())
                .productId(product.productName())
                .category(product.department())
                .price(new BigDecimal(product.price()))
                .referrer(faker.internet().webdomain())
                .userAgent(faker.internet().botUserAgentAny())
                .build();

        //template.send(TOPIC, msg);
        System.out.println("Published");
    }


}
