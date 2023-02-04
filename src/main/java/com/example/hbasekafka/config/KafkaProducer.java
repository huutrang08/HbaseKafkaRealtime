package com.example.hbasekafka.config;

import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;



@Service
@AllArgsConstructor
public class KafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

    private KafkaTemplate<String,String> kafkaTemplate;



    public void sendMessage(String message){
        LOGGER.info(String.format("sent message %s", message));
        kafkaTemplate.send("Example",message);
    }

    public void sendMessage1(String message){
        LOGGER.info(String.format("sent message %s", message));
        kafkaTemplate.send("Json",message).addCallback(new KafkaSendCallback<String,String>(){

            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println(result.getProducerRecord().toString());
            }

            @Override
            public void onFailure(KafkaProducerException e) {

            }
        });
    }

}
