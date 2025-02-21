package com.thinkport.streams.service;


import digital.thinkport.avro.User;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class UserController {
    private final KafkaStreamsInteractiveQueryService kafkaStreamsInteractiveQueryService;

    UserController(KafkaStreamsInteractiveQueryService kafkaStreamsInteractiveQueryService) {
        this.kafkaStreamsInteractiveQueryService = kafkaStreamsInteractiveQueryService;
    }
    @GetMapping("/users/{id}")
    @ResponseStatus(HttpStatus.OK)
    public String getUser(@PathVariable String id) {
        ReadOnlyKeyValueStore<String, User> queryableStore = kafkaStreamsInteractiveQueryService.retrieveQueryableStore("all-users", QueryableStoreTypes.keyValueStore());
        if (queryableStore.get(id) == null) {return "User Not Found.";}
        else{return "Found " + queryableStore.get(id).toString();}
    }

}

