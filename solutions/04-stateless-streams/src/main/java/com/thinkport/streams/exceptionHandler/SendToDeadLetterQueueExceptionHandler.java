package com.thinkport.streams.exceptionHandler;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;

import java.util.Map;
import java.util.Properties;

public class SendToDeadLetterQueueExceptionHandler implements DeserializationExceptionHandler {
  KafkaProducer dltProducer;
  String dlt;

  @Override
  public DeserializationHandlerResponse handle(
      ErrorHandlerContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
    ProducerRecord<byte[], byte[]> produceRecord =
        new ProducerRecord<>(dlt, null, record.key(), record.value());

    dltProducer.send(
        produceRecord,
        (recordMetadata, e) -> {
          // executes every time a record is successfully sent or an exception is thrown
          if (e == null) {
            // the record was successfully sent
            System.out.println(
                "Received new metadata. \n"
                    + "Topic:"
                    + recordMetadata.topic()
                    + "\n"
                    + "Partition: "
                    + recordMetadata.partition()
                    + "\n"
                    + "Offset: "
                    + recordMetadata.offset()
                    + "\n"
                    + "Timestamp: "
                    + recordMetadata.timestamp());
          } else {
            System.out.println("Error while producing" + e);
          }
        });
    return DeserializationHandlerResponse.CONTINUE;
  }

  @Override
  public void configure(Map<String, ?> map) {
    map.containsKey("dead.letter.topic");
    if (map.containsKey("dead.letter.topic")) {
      this.dlt = (String) map.get("dead.letter.topic");
    } else {
      this.dlt = "shop.dlt";
    }
    dltProducer = new KafkaProducer(map);
  }
}
