/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.server.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.support.KafkaHeaders;
//import org.springframework.messaging.Message;
//import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.adapter.standard.StandardWebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Profile("test")
@Component
public class NodeEventHandlerTest extends TextWebSocketHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeEventHandlerTest.class);

    @Autowired
    private WebSocketContextTest webSocketContext;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String fileSystemName = session.getAttributes().get("fileSystemName").toString();
        LOGGER.debug("WebSocket session '{}' opened for file system '{}'", session.getId(), fileSystemName);

        webSocketContext.addSession(((StandardWebSocketSession) session).getNativeSession());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String fileSystemName = (String) session.getAttributes().get("fileSystemName");
        //removeSession(fileSystemName, ((StandardWebSocketSession) session).getNativeSession());
    }

/*    private void removeSession(String fileSystemName, Session session) {
        ListenableAppStorage storage = appDataBean.getStorage(fileSystemName);
        AppStorageListener listener = (AppStorageListener) session.getUserProperties().get("listener");
        storage.removeListener(listener);
        webSocketContext.removeSession(session);
    }*/
}
