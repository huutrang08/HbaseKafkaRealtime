package com.example.hbasekafka.config;

import com.example.hbasekafka.entities.Employee;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class JsonKafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonKafkaProducer.class);

    private KafkaTemplate<String, Employee> kafkaTemplate;

    public void sendMessage(Employee employee){
        LOGGER.info(String.format("sent message %s", employee.toString()));
        Message<Employee> message = MessageBuilder.withPayload(employee).setHeader(KafkaHeaders.TOPIC,"ExampleJson").build();
        kafkaTemplate.send(message);
    }
    public void sendMessage1(Employee employee){
        LOGGER.info(String.format("sent message %s", employee.toString()));
        Message<Employee> message = MessageBuilder.withPayload(employee).setHeader(KafkaHeaders.TOPIC,"Json").build();
        kafkaTemplate.send(message).addCallback(new KafkaSendCallback<String,Employee>(){

            @Override
            public void onSuccess(SendResult<String, Employee> result) {
                System.out.println(result.getProducerRecord().value());
            }

            @Override
            public void onFailure(KafkaProducerException e) {

            }
        });
    }
}
