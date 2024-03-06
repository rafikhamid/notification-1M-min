package com.poc.notification.controller;

import com.poc.notification.messagequeue.Sender;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class UserController {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private Sender sender;

    @GetMapping("/send")
    public void send(){
        //rabbitTemplate.convertAndSend("", "users", new User("name"));

        sender.send();
    }

}
