package com.powsybl.server.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.afs.storage.ListenableAppStorage;
import com.powsybl.afs.storage.events.AppStorageListener;
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

public class NodeEventHandler extends TextWebSocketHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServer.class);

    private final AppDataBean appDataBean;
    private final WebSocketContext webSocketContext;

    private final ObjectMapper objectMapper = JsonUtil.createObjectMapper();

    public NodeEventHandler(AppDataBean appDataBean, WebSocketContext webSocketContext) {
        this.appDataBean = appDataBean;
        this.webSocketContext = webSocketContext;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String fileSystemName = session.getAttributes().get("fileSystemName").toString();
        LOGGER.debug("WebSocket session '{}' opened for file system '{}'", session.getId(), fileSystemName);

        ListenableAppStorage storage = appDataBean.getStorage(fileSystemName);

        AppStorageListener listener = eventList -> {
            if (session.isOpen()) {
                RemoteEndpoint.Async remote = ((StandardWebSocketSession) session).getNativeSession().getAsyncRemote();
                remote.setSendTimeout(1000);
                try {
                    String eventListEncode = objectMapper.writeValueAsString(eventList);
                    remote.sendText(eventListEncode, result -> {
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
        };
        storage.addListener(listener);
        ((StandardWebSocketSession) session).getNativeSession().getUserProperties().put("listener", listener);
        webSocketContext.addSession(((StandardWebSocketSession) session).getNativeSession());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String fileSystemName = (String) session.getAttributes().get("fileSystemName");
        removeSession(fileSystemName, ((StandardWebSocketSession) session).getNativeSession());
    }

    private void removeSession(String fileSystemName, Session session) {
        ListenableAppStorage storage = appDataBean.getStorage(fileSystemName);
        AppStorageListener listener = (AppStorageListener) session.getUserProperties().get("listener");
        storage.removeListener(listener);
        webSocketContext.removeSession(session);
    }
}
