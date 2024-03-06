package com.poc.notification.messagequeue;

import com.poc.notification.entity.Notification;
import com.poc.notification.repositories.NotificationRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;

@Service
@Slf4j

public class Sender {

    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private NotificationRepository notificationRepository;

    public void send(){
        int count = 0;
        Instant start = Instant.now();
        List<Notification> all = notificationRepository.findAll();
        log.info("#### Start processing : " + all.size() + " notifications");
        Iterator<Notification> iterator = all.iterator();
        do {
            rabbitTemplate.convertAndSend("", "notifications", iterator.next());
            count++;
        } while (iterator.hasNext() && Duration.between(start, Instant.now()).getSeconds() <= 60) ;
        log.info("#### End processing notifications : " + count);
    }
}
