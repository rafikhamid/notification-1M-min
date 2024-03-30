package com.poc.notification.messagequeue;

import com.google.common.collect.Lists;
import com.poc.notification.entity.Notification;
import com.poc.notification.repositories.NotificationRepository;
import com.poc.notification.services.SafeCounter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
public class Sender {

    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private NotificationRepository notificationRepository;

    public Long sendSync(List<Notification> notifications, SafeCounter safeCounter) {
        //log.info("#### Start sending : " + notifications.size() + " notifications to message queue");
        List<List<Notification>> partitions = Lists.partition(notifications, 250000);

        Random random = new Random();
        Set usedIndex = new HashSet<>();
        partitions.parallelStream().forEach(part -> {
            /*int index;
            do {
                index = random.nextInt(4) + 1;
            }
            while (usedIndex.contains(index));
            usedIndex.add(index);
            System.out.println(" ############# INDEX : " + index);
            */

            Iterator<Notification> inIt = part.iterator();
            do {
                rabbitTemplate.convertAndSend("", "notifications", inIt.next()); // "notifications-" + index
                safeCounter.increment(1l);
            } while (inIt.hasNext());
        });
        return safeCounter.getCount();
    }
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

    public void producerBatchInsert() {
        Instant now = Instant.now();
        SafeCounter safeCounter = new SafeCounter();
        Pageable pageable = Pageable.ofSize(50000);
        Page<Notification> paged;
        do {
            paged = notificationRepository.findAll(pageable);
            this.sendSync(paged.getContent(), safeCounter);
            pageable = paged.nextPageable();
        }
        while (paged.hasNext());
        long seconds = Duration.between(now, Instant.now()).getSeconds();
        System.out.println(" Processed : " + safeCounter.getCount() + " --> took " + seconds + " s");
    }

}
