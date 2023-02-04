package com.example.hbasekafka.Controller;

import com.example.hbasekafka.config.JsonKafkaProducer;
import com.example.hbasekafka.config.KafkaProducer;
import com.example.hbasekafka.entities.Employee;
import com.example.hbasekafka.service.HbaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("api/hbase")
public class ConnectionController {

    @Autowired
    HbaseService hbaseService;

    @GetMapping("get")
    public ResponseEntity listTable() throws IOException {
        return ResponseEntity.ok().body(hbaseService.showTable());
    }

    @GetMapping("create")
    public void create() {
        List<String> cf = new ArrayList<>();
        cf.add("cf1");
        hbaseService.createTable("HTTest",cf);
    }

    @GetMapping("write")
    public void write(){
       hbaseService.writeData("HTTest","1","cf1","name","SayHi");
    }


    @GetMapping("getRow")
    public ResponseEntity getRow(@RequestParam("key") String key){
        return ResponseEntity.ok().body(hbaseService.getRow("HTTest",key));
    }



    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private JsonKafkaProducer jsonKafkaProducer;

    @GetMapping("publish")
    public ResponseEntity<String> publish(@RequestParam("message") String message){
        kafkaProducer.sendMessage(message);
        return ResponseEntity.ok("Message sent to the topic");
    }

    @PostMapping("publish")
    public ResponseEntity<String> publish(@RequestBody Employee employee){
        jsonKafkaProducer.sendMessage(employee);
        return ResponseEntity.ok("Sent");
    }

    @PostMapping("publish2")
    public ResponseEntity<String> publish2(@RequestBody Employee employee){
        jsonKafkaProducer.sendMessage1(employee);
        return ResponseEntity.ok("Sent");
    }


}
