/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.server.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.TaskEvent;
import com.powsybl.afs.TaskListener;
import com.powsybl.server.commons.AppDataBean;
import com.powsybl.commons.json.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.adapter.standard.StandardWebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import javax.websocket.RemoteEndpoint;
import javax.websocket.Session;
import java.io.IOException;
import java.io.UncheckedIOException;

public class TaskEventHandler extends TextWebSocketHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskEventHandler.class);

    private final AppDataBean appDataBean;
    private final WebSocketContext webSocketContext;

    private final ObjectMapper objectMapper = JsonUtil.createObjectMapper();

    public TaskEventHandler(AppDataBean appDataBean, WebSocketContext webSocketContext) {
        this.appDataBean = appDataBean;
        this.webSocketContext = webSocketContext;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {

        String fileSystemName = session.getAttributes().get("fileSystemName").toString();
        AppFileSystem fileSystem = appDataBean.getFileSystem(fileSystemName);
        String projectId = session.getAttributes().get("projectId").toString();

        LOGGER.debug("WebSocket session '{}' opened for file system '{}'", session.getId(), fileSystemName);

        TaskListener listener = new TaskListener() {

            @Override
            public String getProjectId() {
                return projectId;
            }

            @Override
            public void onEvent(TaskEvent event) {
                if (session.isOpen()) {
                    RemoteEndpoint.Async remote = ((StandardWebSocketSession) session).getNativeSession().getAsyncRemote();
                    remote.setSendTimeout(1000);
                    try {
                        String taskEventEncode = objectMapper.writeValueAsString(event);
                        remote.sendText(taskEventEncode, result -> {
                            if (!result.isOK()) {
                                LOGGER.error(result.getException().toString(), result.getException());
                            }
                        });
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                } else {
                    webSocketContext.removeSession(((StandardWebSocketSession) session).getNativeSession());
                }
            }
        };
        ((StandardWebSocketSession) session).getNativeSession().getUserProperties().put("listener", listener);
        fileSystem.getTaskMonitor().addListener(listener);

        webSocketContext.addSession(((StandardWebSocketSession) session).getNativeSession());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String fileSystemName = (String) session.getAttributes().get("fileSystemName");
        removeSession(fileSystemName, ((StandardWebSocketSession) session).getNativeSession());
    }

    private void removeSession(String fileSystemName, Session session) {
        AppFileSystem fileSystem = appDataBean.getFileSystem(fileSystemName);

        TaskListener listener = (TaskListener) session.getUserProperties().get("listener");
        fileSystem.getTaskMonitor().removeListener(listener);
        webSocketContext.removeSession(session);
    }
}
