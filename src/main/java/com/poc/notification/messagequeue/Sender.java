package com.poc.notification.messagequeue;

import com.poc.notification.entity.Notification;
import com.poc.notification.repositories.NotificationRepository;
import com.poc.notification.services.SafeCounter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class Sender {

    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private NotificationRepository notificationRepository;

    @Async
    public CompletableFuture<Long> send(List<Notification> notifications, SafeCounter safeCounter){
        log.info("#### Start sending : " + notifications.size() + " notifications to message queue");

        notifications.parallelStream().forEach( notif -> {
            rabbitTemplate.convertAndSend("", "notifications", notif);
            safeCounter.increment(1);
        });

        /*
        Iterator<Notification> iterator = notifications.iterator();
        do {
            rabbitTemplate.convertAndSend("", "notifications", iterator.next());
            safeCounter.increment(1);
        } while (iterator.hasNext());
        */
        log.info("#### End sending notifications to message queue : " + safeCounter.getCount());
        return CompletableFuture.completedFuture(safeCounter.getCount());
    }
}
