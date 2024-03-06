package com.poc.notification.entity;

import jakarta.persistence.*;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
@Entity
public class Notification implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    private Long id;

    @Column(name = "message")
    private String message;

    //@Temporal(TemporalType.TIMESTAMP)
    //private Date sentDate;

    @Column(name = "sent")
    private Boolean sent;
}
