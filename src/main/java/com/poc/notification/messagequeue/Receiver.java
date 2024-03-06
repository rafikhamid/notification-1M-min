package com.poc.notification.messagequeue;

import com.poc.notification.entity.Notification;
import com.poc.notification.repositories.NotificationRepository;
import com.poc.notification.services.EmailService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class Receiver {

    @Autowired
    private NotificationRepository notificationRepository;
    @Autowired
    private EmailService emailService;

    @RabbitListener(id = "Notification-Consumer",
            autoStartup = "false",
            queues = {"notifications"},
            containerFactory = "prefetchTenRabbitListenerContainerFactory",
            concurrency = "192")
    public void onNotificationReceived(Notification event) {
        emailService.send(event);
        event.setSent(Boolean.TRUE);
        notificationRepository.save(event);
    }

    @Async
    public CompletableFuture<Long> waitForTimeout() throws InterruptedException {
        while (true){
            Thread.sleep(3000);
        }
    }
}
