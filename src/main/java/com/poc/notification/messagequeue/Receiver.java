package com.poc.notification.messagequeue;

import com.poc.notification.entity.Notification;
import com.poc.notification.repositories.NotificationRepository;
import com.poc.notification.services.EmailService;
import com.poc.notification.services.NotificationService;
import com.poc.notification.services.SafeCounter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class Receiver {

    public SafeCounter safeCounter = new SafeCounter();

    @Autowired
    private NotificationService notificationService;
    @Autowired
    private EmailService emailService;

    @RabbitListener(id = "Notification-Single-Consumer-1",
            autoStartup = "false",
            queues = {"notifications-1"},
            containerFactory = "prefetchTenRabbitListenerContainerFactory",
            concurrency = "100")
    public void onNotificationReceived(Notification notification) {
        internalSend(notification);
        //notificationService.save(notification);
        //safeCounter.increment(notifications.size());
    }

    @RabbitListener(id = "Notification-Single-Consumer-2",
            autoStartup = "false",
            queues = {"notifications-2"},
            containerFactory = "prefetchTenRabbitListenerContainerFactory",
            concurrency = "100")
    public void onNotificationReceived2(Notification notification) {
        internalSend(notification);
    }

    private void internalSend(Notification notification) {
        emailService.send(notification);
        notification.setSent(Boolean.TRUE);
        notificationService.save(notification);
        //safeCounter.increment(1l);
        //safeCounter.increment(notifications.size());
    }

    @RabbitListener(id = "Notification-Single-Consumer-3",
            autoStartup = "false",
            queues = {"notifications-3"},
            containerFactory = "prefetchTenRabbitListenerContainerFactory",
            concurrency = "100")
    public void onNotificationReceived3(Notification notification) {
        internalSend(notification);
        //notificationService.save(notification);
        //safeCounter.increment(notifications.size());
    }

    @RabbitListener(id = "Notification-Single-Consumer-4",
            autoStartup = "false",
            queues = {"notifications-4"},
            containerFactory = "prefetchTenRabbitListenerContainerFactory",
            concurrency = "100")
    public void onNotificationReceived4(Notification notification) {
        internalSend(notification);
        //notificationService.save(notification);
        //safeCounter.increment(notifications.size());
    }

    @RabbitListener(id = "Notification-Group-Consumer",
            autoStartup = "false",
            queues = {"notifications"},
            containerFactory = "prefetchTenRabbitListenerContainerFactory",
            concurrency = "128")
    public void onNotificationReceived(List<Notification> notifications) {
        //log.warn(" >>>>>>>>>>>>>>>>>>>>>>>>> REceived : " + notifications.size());
        notifications.parallelStream().forEach( event -> {
            emailService.send(event);
            event.setSent(Boolean.TRUE);
        });
        notificationService.saveAll(notifications);
        /*
        notifications.parallelStream().forEach( event -> {
            emailService.send(event);
            event.setSent(Boolean.TRUE);
            notificationService.save(event);
        });

         */
        //safeCounter.increment(notifications.size());
    }


}
