package com.poc.notification.services;

import com.poc.notification.entity.Notification;
import com.poc.notification.repositories.NotificationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

@Service
public class NotificationService {

    @Autowired
    private NotificationRepository notificationRepository;
    @Autowired
    private EmailService emailService;

    public void process(){
        int count = 0;
        Instant start = Instant.now();
        List<Notification> all = notificationRepository.findBySentDateNull();
        System.out.println("#### Start processing : " + all.size() + " notifications");
        Iterator<Notification> iterator = all.iterator();
        do {
            sendAndRemove(iterator.next());
            count++;
        } while (iterator.hasNext() && Duration.between(start, Instant.now()).getSeconds() <= 60) ;
        System.out.println("#### End processing notifications : " + count);
    }

    private void sendAndRemove(Notification notification){
        emailService.send(notification);
        notification.setSentDate(Calendar.getInstance().getTime());
        notificationRepository.save(notification);
    }

}
