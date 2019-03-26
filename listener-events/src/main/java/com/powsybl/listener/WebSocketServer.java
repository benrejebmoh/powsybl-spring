/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import org.springframework.web.servlet.HandlerMapping;

import org.springframework.web.socket.WebSocketHandler;

import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

import org.springframework.web.socket.server.HandshakeInterceptor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.afs.storage.NodeInfo;

import com.powsybl.afs.storage.events.NodeEvent;
import com.powsybl.afs.storage.events.NodeEventList;
import com.powsybl.commons.json.JsonUtil;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Configuration
@EnableWebSocket
@ComponentScan(basePackageClasses = {NodeInfo.class/*, ListenerApplication.class*/})
public class WebSocketServer implements WebSocketConfigurer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServer.class);

    @Autowired
    private NodeEventWSHandler nodeEventWSHandler;
    @Autowired
    private TaskEventHandlerWSTest taskEventHandlerWSTest;

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry
            .addHandler(nodeEventWSHandler/*new NodeEventWSHandler()*/,  "/messages/afs/v1/node_events/{fileSystemName}")
            //.addHandler(new TaskEventHandler(appDataBean, webSocketContext), "/messages/afs/" + StorageServer.API_VERSION + "/task_events/{fileSystemName}/{projectId}")
            .addHandler(taskEventHandlerWSTest, "/messages/afs/v1/task_events/{fileSystemName}/{projectId}")
            .setAllowedOrigins("*")
            .addInterceptors(new UriTemplateHandshakeInterceptor());
    }

    private class UriTemplateHandshakeInterceptor implements HandshakeInterceptor {
        @Override
        public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler, Map<String, Object> attributes) throws Exception {
            HttpServletRequest origRequest = ((ServletServerHttpRequest) request).getServletRequest();
            /* Retrieve template variables */
            Map<String, String> uriTemplateVars = (Map<String, String>) origRequest.getAttribute(HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE);
            /* Put template variables into WebSocket session attributes */
            if (uriTemplateVars != null) {
                attributes.putAll(uriTemplateVars);
            }
            return true;
        }
        @Override
        public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler, Exception exception) {
        }
    }
}

@Component
class MessageListenerKfk {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageListenerKfk.class);
    private CountDownLatch nodeInfoLatch = new CountDownLatch(1);
    private CountDownLatch nodeEventLatch = new CountDownLatch(1);

    @Autowired
    private WebSocketContext webSocketContext;

    private final ObjectMapper objectMapper = JsonUtil.createObjectMapper();
    @KafkaListener(groupId = "imagrid", topics = {"nodeInfo", "nodeEvent"})
    public void listenTopic(@Payload String message,
            @Header(org.springframework.kafka.support.KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(org.springframework.kafka.support.KafkaHeaders.RECEIVED_TIMESTAMP) long ts,
            @Header(org.springframework.kafka.support.KafkaHeaders.OFFSET) List<Long> offsets,
            @Header("X-FileSystemName") String fileSystemName,
            @Header("X-Event") String event) {

        LOGGER.info("Receive message Topic : {} - Class : {}", topic, event);

        NodeEventList nodeEventList = new NodeEventList();
        try {
            Class myClass = Class.forName(event);
            Object desObject = objectMapper.readValue(new String(message), myClass);
            nodeEventList.addEvent((NodeEvent) myClass.cast(desObject));
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        this.nodeInfoLatch.countDown();
        webSocketContext.getSessions().forEach(ses -> {
            try {
                if (ses.isOpen()) {
                    String eventListEncode = objectMapper.writeValueAsString(nodeEventList);
                    ses.getAsyncRemote().sendText(eventListEncode);
                    LOGGER.info("Send to WS : {}", eventListEncode);
                }
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
    }
}

