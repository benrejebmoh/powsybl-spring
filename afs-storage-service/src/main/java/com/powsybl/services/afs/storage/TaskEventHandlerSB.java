package com.powsybl.services.afs.storage;

import javax.websocket.EncodeException;
import javax.websocket.RemoteEndpoint;
import javax.websocket.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.adapter.standard.StandardWebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.TaskEvent;
import com.powsybl.afs.TaskListener;
import com.powsybl.afs.ws.server.utils.sb.AppDataBeanSB;
import com.powsybl.afs.ws.server.utils.sb.TaskEventEncoder;

public class TaskEventHandlerSB extends TextWebSocketHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskEventHandlerSB.class);

    private final AppDataBeanSB appDataBean;
    private final WebSocketContextSB webSocketContext;

    public TaskEventHandlerSB(AppDataBeanSB appDataBean, WebSocketContextSB webSocketContext) {
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
                        String taskEventEncode = new TaskEventEncoder().encode(event);
                        remote.sendText(taskEventEncode, result -> {
                            if (!result.isOK()) {
                                LOGGER.error(result.getException().toString(), result.getException());
                            }
                        });
                    } catch (EncodeException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
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
