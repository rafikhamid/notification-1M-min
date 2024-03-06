package com.poc.notification.services;

import com.google.common.collect.Lists;
import com.poc.notification.entity.Notification;
import com.poc.notification.repositories.NotificationRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class NotificationService {

    @Autowired
    private NotificationRepository notificationRepository;
    @Autowired
    private EmailService emailService;

    public List<Notification> findAllUnsentNotifications(){
        List<Notification> all = notificationRepository.findAll();
        log.info("#### Found " + all.size() + " unsent notifications");
        return all;
    }

    public void process(){
        int count = 0;
        Instant start = Instant.now();
        List<Notification> all = notificationRepository.findBySentNull();
        System.out.println("#### Start processing : " + all.size() + " notifications");
        Iterator<Notification> iterator = all.iterator();
        do {
            send(iterator.next());
            count++;
        } while (iterator.hasNext() && Duration.between(start, Instant.now()).getSeconds() <= 60) ;
        System.out.println("#### End processing notifications : " + count);
    }

    void send(Notification notification){
        emailService.send(notification);
        notification.setSent(true);
        notificationRepository.save(notification);
    }

    public void processMultiThread(int nbThreads){
        final ExecutorService executor = Executors.newFixedThreadPool(nbThreads);
        SafeCounter counter = new SafeCounter();

        Instant start = Instant.now();
        List<Notification> all = notificationRepository.findBySentNull();
        System.out.println("#### Start processing : " + all.size() + " notifications");

        List<List<Notification>> partitions = Lists.partition(all, 500);
        for (List<Notification> part : partitions){
            executor.submit(new NotificationThread(part, this, counter));
        }
        while (Duration.between(start, Instant.now()).getSeconds() <= 60){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println(" ###### Shut Down Gracefully");
        executor.shutdownNow();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println(" TOTAL PROCESSED : " + counter.getCount());
    }

}
