package com.poc.notification;

import com.poc.notification.services.NotificationService;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.List;

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
		//notificationService.processMultiThread(16);

		// PART 3 : using RabbitMQ (see NotificationController) : processed 381368 in 1 minute
	}

	@Bean
	public Queue queue() {
		return new Queue("notifications");
	}

	@Bean
	public SimpleMessageConverter converter(){
		SimpleMessageConverter converter = new SimpleMessageConverter();
		converter.setAllowedListPatterns(List.of("com.poc.notification.entity.*",
				"java.util.*",
				"java.sql.Timestamp",
				"java.lang.Boolean"));
		return converter;
	}

	@Autowired
	private ConnectionFactory rabbitConnectionFactory;

	@Bean
	public RabbitTemplate rabbitTemplate(){
		RabbitTemplate template = new RabbitTemplate(rabbitConnectionFactory);
		template.setMessageConverter(converter());
		return template;
	}

	@Bean
	public RabbitListenerContainerFactory<SimpleMessageListenerContainer> prefetchTenRabbitListenerContainerFactory(ConnectionFactory rabbitConnectionFactory) {
		SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
		factory.setConnectionFactory(rabbitConnectionFactory);
		factory.setPrefetchCount(100);
		factory.setMessageConverter(converter());
		return factory;
	}

}
