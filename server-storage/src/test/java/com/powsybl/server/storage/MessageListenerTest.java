package com.powsybl.server.storage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.afs.storage.events.NodeEvent;
import com.powsybl.afs.storage.events.NodeEventList;
import com.powsybl.commons.json.JsonUtil;

@Component
@Profile("test")
public class MessageListenerTest {
    private CountDownLatch nodeInfoLatch = new CountDownLatch(1);

    @Autowired
    private WebSocketContextTest webSocketContext;

    private final ObjectMapper objectMapper = JsonUtil.createObjectMapper();

    public MessageListenerTest() {
    }

    @KafkaListener(groupId = "imagrid", topics = {"nodeInfo", "nodeEvent"})
    public void listenTopic(@Payload String message,
            @Header(org.springframework.kafka.support.KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(org.springframework.kafka.support.KafkaHeaders.RECEIVED_TIMESTAMP) long ts,
            @Header(org.springframework.kafka.support.KafkaHeaders.OFFSET) List<Long> offsets,
            @Header("X-FileSystemName") String fileSystemName,
            @Header("X-Event") String event) {
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
                } else {
                }
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
    }
}
