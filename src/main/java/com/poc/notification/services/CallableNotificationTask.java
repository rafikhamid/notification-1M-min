package com.poc.notification.services;

import com.poc.notification.entity.Notification;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

public class CallableNotificationTask implements Callable<Long> {
    private final List<Notification> part;
    private final NotificationService notificationService;

    public CallableNotificationTask(List<Notification> part, NotificationService notificationService){
        this.part = part;
        this.notificationService = notificationService;
    }
    @Override
    public Long call() throws Exception {
        System.out.println("Starting callable ");
        Iterator<Notification> iterator = part.iterator();
        Long count = 0l;
        do {
            if (Thread.currentThread().isInterrupted()){
                System.out.println(" >>>> Thread INTERRUPTED. Processed : " + count);
                return count;
            }
            notificationService.send(iterator.next());
            count++;
        } while (iterator.hasNext()) ;
        System.out.println("Finished callable ");
        return count;
    }
}
