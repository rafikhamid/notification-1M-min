package com.poc.notification.repositories;

import com.poc.notification.entity.Notification;
import org.springframework.data.jpa.domain.Specification;

public class NotificationSpecification {

    public static Specification<Notification> notSent() {
        return (root, query, builder) -> builder.notEqual(root.get("sentDate"), null);
    }

}
