package com.poc.notification.services;

import com.poc.notification.entity.Notification;
import org.springframework.stereotype.Service;

@Service
public class EmailService {

    public void send(Notification notification){
        try {
            //System.out.println(">>>> Sending email for : " + notification.getId());
            Thread.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
