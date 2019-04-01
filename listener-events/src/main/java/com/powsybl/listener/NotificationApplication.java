package com.powsybl.listener;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import com.powsybl.afs.storage.NodeInfo;

@SpringBootApplication
@EnableDiscoveryClient
@ComponentScan(basePackageClasses = {NodeInfo.class, NotificationApplication.class})
public class NotificationApplication {
    public static void main(String[] args) {
        SpringApplication.run(NotificationApplication.class, args);
    }

    @Bean
    public MessageListenerKfk messageListener() {
        return new MessageListenerKfk();
    }

    @Bean
    public NodeEventWSHandler getNodeEventHandler() {
        return new NodeEventWSHandler();
    }

    @Bean
    public TaskEventHandlerWSTest getTaskEventHandlerWSTest() {
        return new TaskEventHandlerWSTest();
    }
}
