package com.poc.notification.messagequeue;

import com.poc.notification.entity.Notification;
import com.poc.notification.repositories.NotificationRepository;
import com.poc.notification.services.EmailService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class Receiver {

    @Autowired
    private NotificationRepository notificationRepository;
    @Autowired
    private EmailService emailService;

    @RabbitListener(queues = {"notifications"}, containerFactory = "prefetchTenRabbitListenerContainerFactory", concurrency = "128")
    public void onNotificationReceived(Notification event) {
        //log.info("Event Received: {}", event);
        emailService.send(event);
        event.setSent(Boolean.TRUE);
        notificationRepository.save(event);
    }

}
