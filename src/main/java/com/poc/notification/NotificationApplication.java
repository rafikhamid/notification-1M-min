package com.poc.notification;

import com.poc.notification.services.NotificationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class NotificationApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(NotificationApplication.class, args);
	}

	@Autowired
	private NotificationService notificationService;

	@Override
	public void run(String... args) throws Exception {
		// PART 1 : process 3700 in 1 minute
		// notificationService.process();

		// PART 2 : Multithreading V1 - using SafeCounter
		// Thread = 8 -> process ~30 k in 1 minute
		// Thread = 16 -> process 43~50 k in 1 minute
		notificationService.processMultiThread(16);
	}

}
