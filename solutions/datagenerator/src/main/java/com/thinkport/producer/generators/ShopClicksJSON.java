package com.thinkport.producer.generators;

import com.thinkport.producer.model.ClickJson;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.datafaker.Faker;
import net.datafaker.providers.base.Commerce;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Service
@AllArgsConstructor
@Slf4j
public class ShopClicksJSON {

  private static final String TOPIC = "shop.clicks.raw";
  private final KafkaTemplate<String, ClickJson> template;
  private final Faker faker = new Faker();

  @Scheduled(fixedRate = 300)
  public void send() {
    Commerce product = faker.commerce();
    String userId = faker.number().numberBetween(1,30) == 1 ? "9000" : String.valueOf(faker.number().numberBetween(1, 500));
    ClickJson msg =
        ClickJson.builder()
            .clickId(faker.internet().uuid())
            .userId(userId)
            .ip(faker.internet().ipV4Address())
            .knownIp(faker.bool().bool())
            .request(faker.internet().httpMethod())
            .status(
                Integer.parseInt(
                    faker.expression("#{options.option '200','201','400','404', '500'}")))
            .bytes(faker.number().positive())
            .productId(product.productName())
            .referrer(faker.internet().webdomain())
            .userAgent(faker.internet().botUserAgentAny())
            .build();
    try {
      template.send(TOPIC, msg.getClickId(), msg).get(10, TimeUnit.SECONDS);
      log.debug("Sent Click.");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Failed to wait for record send.", e);
    } catch (Exception e) {
      log.error("Failed to send record.", e);
    }
    }


  @Scheduled(fixedRate = 30000)
  public void sendMalicious() {
    if(faker.number().numberBetween(0,2)!=1){
      return;
    }
    Commerce product = faker.commerce();
    ClickJson msg =
            ClickJson.builder()
                    .clickId(faker.internet().uuid())
                    .userId("666")
                    .ip("192.168.1.1")
                    .knownIp(true)
                    .request(faker.internet().httpMethod())
                    .status(401)
                    .bytes(faker.number().positive())
                    .productId(product.productName())
                    .referrer(faker.internet().webdomain())
                    .userAgent(faker.internet().botUserAgentAny())
                    .build();
    int runs = faker.number().numberBetween(100,300);
    for (int i = 0;i<runs;i++){
      template.send(TOPIC, msg.getClickId(), msg).whenComplete((v,t)->{
        if(!Objects.isNull(t)){
          log.error("Failed to publish." + t.getMessage());
        }
      });
    }
      log.info("Send Malicious.");
  }

}
