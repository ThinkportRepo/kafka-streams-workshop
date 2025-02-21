package com.thinkport.topologytest.streams;

import com.thinkport.producer.model.ClickJson;
import com.thinkport.streams.config.CustomSerdes;
import com.thinkport.streams.topology.UserTopology;
import com.thinkport.streams.topology.StatelessTopology;
import digital.thinkport.avro.ClickAvro;
import digital.thinkport.avro.User;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(classes = {com.thinkport.streams.StatelessStreamsApplication.class})
public class TopologyTestDriverTests {
  private static final String SCHEMA_REGISTRY_SCOPE = TopologyTestDriverTests.class.getName();
  private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

  private TopologyTestDriver testDriver;

  private TestInputTopic<String, ClickJson> clickJsonTopic;
  private TestOutputTopic<String, ClickAvro> shopClicksFiltered;
  private TestOutputTopic<String, ClickAvro> shopClicksErroneous;

  @BeforeEach
  @SuppressWarnings("resource")
  void beforeEach() throws Exception {

    StreamsBuilder builder = new StreamsBuilder();
    new StatelessTopology(
            MOCK_SCHEMA_REGISTRY_URL,
            "shop.clicks.raw",
            "shop.clicks.filtered",
            "shop.clicks.erroneous")
        .statelessStream(builder);
    org.apache.kafka.streams.Topology topology = builder.build();

    // Dummy properties needed for test diver
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock:1234");
    props.put("schema.registry.url", MOCK_SCHEMA_REGISTRY_URL);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

    // Create test driver
    testDriver = new TopologyTestDriver(topology, props);

    Serde<String> stringSerde = Serdes.String();
    Serde<byte[]> bytesSerializerSerde = Serdes.ByteArray();
    Serde<Integer> integerSerde = Serdes.Integer();

    Serde<ClickAvro> avroClickSerde = new SpecificAvroSerde<>();

    JsonSerde<ClickJson> jsonSerde = new JsonSerde<>(ClickJson.class);

    Map<String, String> config =
        Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
    avroClickSerde.configure(config, false);

    clickJsonTopic =
        testDriver.createInputTopic(
            "shop.clicks.raw", stringSerde.serializer(), jsonSerde.serializer());

    shopClicksFiltered =
        testDriver.createOutputTopic(
            "shop.clicks.filtered", stringSerde.deserializer(), avroClickSerde.deserializer());

    shopClicksErroneous =
        testDriver.createOutputTopic(
            "shop.clicks.erroneous", stringSerde.deserializer(), avroClickSerde.deserializer());
  }

  @Test
  void correctUser() throws Exception {
    ClickJson cj =
        ClickJson.builder()
            .clickId("1")
            .userId("1001")
            .ip("169.156.89.232")
            .knownIp(Boolean.TRUE)
            .request("PATCH")
            .status(201)
            .bytes(1833002309)
            .productId("1")
            .referrer("www.reggie-upton.info")
            .userAgent("Mozilla/5.0")
            .build();

    clickJsonTopic.pipeInput("123456789", cj);
    assertEquals("1001", shopClicksFiltered.readValue().getUserId());
  }

  @Test
  void adminUser() throws Exception {
    //TODO:  implement
  }

  private Properties getSchemaProperties() {
    Properties properties = new Properties();
    properties.put("schema.registry.url", MOCK_SCHEMA_REGISTRY_URL);
    return properties;
  }
}
