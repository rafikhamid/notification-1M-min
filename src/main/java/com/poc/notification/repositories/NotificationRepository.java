package com.poc.notification.repositories;

import com.poc.notification.entity.Notification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface NotificationRepository extends JpaRepository<Notification, Long> {
    //List<Notification> findBySentDateNull();

    List<Notification> findBySentNull();

}
