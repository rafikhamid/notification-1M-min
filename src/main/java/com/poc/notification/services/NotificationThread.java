package com.poc.notification.services;

import com.poc.notification.entity.Notification;

import java.util.Iterator;
import java.util.List;

public class NotificationThread implements Runnable {

    private final List<Notification> part;
    private final NotificationService notificationService;
    private final SafeCounter counter;

    public NotificationThread(List<Notification> part, NotificationService notificationService, SafeCounter counter) {
        this.part = part;
        this.notificationService = notificationService;
        this.counter = counter;
    }

    @Override
    public void run() {
        System.out.println(" ##### Start thread : " + Thread.currentThread().toString() + " processing items : " + part.size());
        long count = 0;
        Iterator<Notification> iterator = part.iterator();
        do {
            if (Thread.currentThread().isInterrupted()){
                System.out.println(" >>>> Thread INTERRUPTED. Processed : " + count);
                counter.increment(count);
                return;
            }
            notificationService.send(iterator.next());
            count++;
        } while (iterator.hasNext()) ;
        System.out.println(" ##### End thread : " + Thread.currentThread().toString() + " Processed : " + count);
        counter.increment(count);
    }
}
