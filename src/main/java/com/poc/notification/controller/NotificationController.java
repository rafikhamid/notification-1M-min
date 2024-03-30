package com.poc.notification.controller;

import com.poc.notification.entity.Notification;
import com.poc.notification.messagequeue.Receiver;
import com.poc.notification.messagequeue.Sender;
import com.poc.notification.services.NotificationService;
import com.poc.notification.services.SafeCounter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
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

    @GetMapping("/startProducerBatch")
    public void startProducerBatch() {
        // Batch of 50000 sent 1M to queue in 43 s
        sender.producerBatchInsert();
    }



    @GetMapping("/startProducerAndConsumer")
    public String startProducerAndConsumer() {
        //this.startConsumer();
        rabbitListenerEndpointRegistry.getListenerContainer("Notification-Single-Consumer-1").start();
        rabbitListenerEndpointRegistry.getListenerContainer("Notification-Single-Consumer-2").start();
        rabbitListenerEndpointRegistry.getListenerContainer("Notification-Single-Consumer-3").start();
        rabbitListenerEndpointRegistry.getListenerContainer("Notification-Single-Consumer-4").start();

        List<Notification> allUnsentNotifications = notificationService.findAllUnsentNotifications();
        if (allUnsentNotifications.isEmpty()) {
            return "No unsent notifications found.";
        }
        SafeCounter safeCounter = new SafeCounter();
        CompletableFuture<Long> future = sender.send(allUnsentNotifications, safeCounter);
        try {
            Long count = future.get(1, TimeUnit.MINUTES);
            return "Found : " + allUnsentNotifications.size() + " unsent notifications. Sent : " + count;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            log.error("TimeoutException in waiting for Producer.", e.getMessage());
        } finally {
            String ret = "Found : " + allUnsentNotifications.size() + " unsent notifications. Sent : " + safeCounter.getCount();
            rabbitListenerEndpointRegistry.getListenerContainer("Notification-Single-Consumer-1").stop();
            rabbitListenerEndpointRegistry.getListenerContainer("Notification-Single-Consumer-2").stop();
            rabbitListenerEndpointRegistry.getListenerContainer("Notification-Single-Consumer-3").stop();
            rabbitListenerEndpointRegistry.getListenerContainer("Notification-Single-Consumer-4").stop();
            return ret;
        }
    }

    @GetMapping("/startProducer")
    public String startProducer() {
        Instant now = Instant.now();
        List<Notification> allUnsentNotifications = notificationService.findAllUnsentNotifications();
        if (allUnsentNotifications.isEmpty()) {
            return "No unsent notifications found.";
        }
        SafeCounter safeCounter = new SafeCounter();
        CompletableFuture<Long> future = sender.send(allUnsentNotifications, safeCounter);
        try {
            Long count = future.get(1, TimeUnit.MINUTES);
            long seconds = Duration.between(now, Instant.now()).getSeconds();
            return "Found : " + allUnsentNotifications.size() + " unsent notifications. Processed : " + count + " --> took " + seconds + " s";
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            return "Found : " + allUnsentNotifications.size() + " unsent notifications. Processed : " + safeCounter.getCount();
        }
    }

    @GetMapping("/startConsumer")
    public String startConsumer() {
        Instant now = Instant.now();
        System.out.println("Started Notification consumers");
        rabbitListenerEndpointRegistry.start();
        try {
            Thread.sleep(50000);
            long seconds = Duration.between(now, Instant.now()).getSeconds();
            long count = receiver.safeCounter.getCount();
            System.out.println("########### Consumed : " + count + " -> took " + seconds + " s.");
            rabbitListenerEndpointRegistry.stop();
            return "Finished consumers";
        } catch (InterruptedException  e) {
            return "TimeoutException.";
        }
    }
}