package com.thinkport.streams.service;


import digital.thinkport.avro.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestTemplate;

@RestController
@Slf4j
public class UserController {
    private final KafkaStreamsInteractiveQueryService kafkaStreamsInteractiveQueryService;

    UserController(KafkaStreamsInteractiveQueryService kafkaStreamsInteractiveQueryService) {
        this.kafkaStreamsInteractiveQueryService = kafkaStreamsInteractiveQueryService;
    }

    private final RestClient restClient = RestClient.create();

    @GetMapping("/users/{id}")
    @ResponseStatus(HttpStatus.OK)
    public String getUser(@PathVariable String id) {
        User user = null;
        HostInfo activeHost = kafkaStreamsInteractiveQueryService.getKafkaStreamsApplicationHostInfo("all-users",id, Serdes.String().serializer());
        if(activeHost==null){
            return "User Not Found Globally.";
        }

        if(activeHost.toString().equals(kafkaStreamsInteractiveQueryService.getCurrentKafkaStreamsApplicationHostInfo().toString())){
            LOG.info("Search locally for user.");
            ReadOnlyKeyValueStore<String, User> queryableStore = kafkaStreamsInteractiveQueryService.retrieveQueryableStore("all-users", QueryableStoreTypes.keyValueStore());
            user = queryableStore.get(id);
            if (user == null) {return "User Not Found Locally.";}
            else{return "Found " + user.toString();}
        }else{
            return restClient.get().uri("http://" + activeHost.host() +":" + activeHost.port() +"/users/"+id).retrieve().body(String.class);
        }
    }

}

