package com.powsybl.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.adapter.standard.StandardWebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Component
public class TaskEventHandlerWSTest extends TextWebSocketHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskEventHandlerWSTest.class);

    @Autowired
    private WebSocketContext webSocketContext;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String fileSystemName = session.getAttributes().get("fileSystemName").toString();
        String projectId = session.getAttributes().get("projectId").toString();

        LOGGER.debug("WebSocket session '{}' opened for file system '{}'", session.getId(), fileSystemName);

        webSocketContext.addSession(((StandardWebSocketSession) session).getNativeSession());
    }
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String fileSystemName = (String) session.getAttributes().get("fileSystemName");
        String projectId = session.getAttributes().get("projectId").toString();
        //removeSession(fileSystemName, ((StandardWebSocketSession) session).getNativeSession());
    }
}
