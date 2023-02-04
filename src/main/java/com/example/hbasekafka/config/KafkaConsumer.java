package com.example.hbasekafka.config;

import com.example.hbasekafka.entities.Employee;
import com.example.hbasekafka.service.HbaseService;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

    @Autowired
    HbaseService hbaseService;

    Gson gson = new Gson();

    @KafkaListener(topics = "ExampleJson", groupId = "myGroup")
    public void consumer(Employee message){
        String value = gson.toJson(message);
        System.out.println(value);
        hbaseService.writeData("HTTest","4","cf1","name",value);
        System.out.println(hbaseService.getRow("HTTest","4"));
        System.out.println(message.getName());
    }

    @KafkaListener(topics = "Json")
    public void consumer1(Employee message){
        LOGGER.info(String.format("Message received -> %s", message));
    }
}
