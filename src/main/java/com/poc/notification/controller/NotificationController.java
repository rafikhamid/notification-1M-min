package com.poc.notification.controller;

import com.poc.notification.entity.Notification;
import com.poc.notification.messagequeue.Receiver;
import com.poc.notification.messagequeue.Sender;
import com.poc.notification.services.NotificationService;
import com.poc.notification.services.SafeCounter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class NotificationController {

    @Autowired
    private NotificationService notificationService;
    @Autowired
    private RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry;
    @Autowired
    private Sender sender;
    @Autowired
    private Receiver receiver;

    @GetMapping("/startProducerAndConsumer")
    public String startProducerAndConsumer() {
        this.startConsumer();

        List<Notification> allUnsentNotifications = notificationService.findAllUnsentNotifications();
        if (allUnsentNotifications.isEmpty()) {
            return "No unsent notifications found.";
        }
        SafeCounter safeCounter = new SafeCounter();
        CompletableFuture<Long> future = sender.send(allUnsentNotifications, safeCounter);
        try {
            Long count = future.get(1, TimeUnit.MINUTES);
            return "Found : " + allUnsentNotifications.size() + " unsent notifications. Processed : " + count;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            log.error("TimeoutException in waiting for Producer.", e.getMessage());
        } finally {
            rabbitListenerEndpointRegistry.getListenerContainer("Notification-Consumer").stop();
            return "Found : " + allUnsentNotifications.size() + " unsent notifications. Processed : " + safeCounter.getCount();
        }
    }

    @GetMapping("/startProducer")
    public String startProducer() {
        List<Notification> allUnsentNotifications = notificationService.findAllUnsentNotifications();
        if (allUnsentNotifications.isEmpty()) {
            return "No unsent notifications found.";
        }
        SafeCounter safeCounter = new SafeCounter();
        CompletableFuture<Long> future = sender.send(allUnsentNotifications, safeCounter);
        try {
            Long count = future.get(1, TimeUnit.MINUTES);
            return "Found : " + allUnsentNotifications.size() + " unsent notifications. Processed : " + count;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            return "Found : " + allUnsentNotifications.size() + " unsent notifications. Processed : " + safeCounter.getCount();
        }
    }

    @GetMapping("/startConsumer")
    public String startConsumer() {
        rabbitListenerEndpointRegistry.getListenerContainer("Notification-Consumer").start();
        return "Started Notification consumers";
    }
}