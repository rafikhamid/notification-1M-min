package com.poc.notification.messagequeue;

import com.google.common.collect.Lists;
import com.poc.notification.entity.Notification;
import com.poc.notification.repositories.NotificationRepository;
import com.poc.notification.services.SafeCounter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.*;
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

        // Batch message of 10
        /*
        List<List<Notification>> partitions = Lists.partition(notifications, 10);
        log.info("############################## " + partitions.size());
        log.info("############################## " + partitions.get(0).size());

        partitions.parallelStream().forEach( notif -> {
            rabbitTemplate.convertAndSend("", "notifications", notif.stream().toList());
            //rabbitTemplate.waitForConfirms(1000);
            safeCounter.increment(1);
        });
        */

        List<List<Notification>> partitions = Lists.partition(notifications, 250000);

        Random random = new Random();
        Set usedIndex = new HashSet<>();
        partitions.parallelStream().forEach(part -> {
            int index;
            do {
                index = random.nextInt(4) + 1;
            }
            while (usedIndex.contains(index));
            usedIndex.add(index);

            System.out.println(" ############# INDEX : " + index);
            Iterator<Notification> inIt = part.iterator();
            do {
                rabbitTemplate.convertAndSend("", "notifications-" + index, inIt.next());
                safeCounter.increment(1l);
            } while (inIt.hasNext());
        });

        /*
        int index = 1;
        Iterator<List<Notification>> outIt = partitions.iterator();
        do {
            List<Notification> next = outIt.next();
            Iterator<Notification> inIt = next.iterator();
            do {
                rabbitTemplate.convertAndSend("", "notifications-" + index, inIt.next());
            } while (inIt.hasNext());
            index++;
        } while (outIt.hasNext());

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
