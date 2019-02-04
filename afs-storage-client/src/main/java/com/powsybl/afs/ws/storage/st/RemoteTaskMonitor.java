package com.powsybl.afs.ws.storage.st;


import com.powsybl.afs.ProjectFile;
import com.powsybl.afs.TaskListener;
import com.powsybl.afs.TaskMonitor;
import com.powsybl.client.commons.UncheckedDeploymentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import javax.websocket.ContainerProvider;
import javax.websocket.DeploymentException;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.*;

import static com.powsybl.afs.ws.storage.st.RemoteStorage.createClient;
import static com.powsybl.afs.ws.storage.st.RemoteStorage.getWebTarget;
import static com.powsybl.afs.ws.storage.st.RemoteListenableStorage.getWebSocketUri;

public class RemoteTaskMonitor implements TaskMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteTaskMonitor.class);

    public static final String FILE_SYSTEM_NAME = "fileSystemName";

    private final String fileSystemName;
    private final URI restUri;
    private final String token;
    private final Map<TaskListener, Session> sessions = new HashMap<>();
    private final RestTemplate client;
    private final UriComponentsBuilder webTarget;

    public RemoteTaskMonitor(String fileSystemName, URI restUri, String token) {
        this.fileSystemName = Objects.requireNonNull(fileSystemName);
        this.restUri = Objects.requireNonNull(restUri);
        this.token = token;
        client = createClient();
        webTarget = getWebTarget(restUri);
    }

    @Override
    public Task startTask(ProjectFile projectFile) {
        Objects.requireNonNull(projectFile);

        LOGGER.debug("startTask(fileSystemName={}, projectFile={})", fileSystemName, projectFile.getId());

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);

        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/tasks")
                .queryParam("projectFileId", projectFile.getId())
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<Task> response = client.exchange(
                    uri,
                    HttpMethod.PUT,
                    entity,
                    Task.class
                    );
        Task task = response.getBody();
        return task;
    }

    @Override
    public void stopTask(UUID id) {
        LOGGER.debug("stopTask(fileSystemName={}, id={})", fileSystemName, id);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put("taskId", id.toString());

        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/tasks/{taskId}")
                .buildAndExpand(params)
                .toUri();

        ResponseEntity<String> response = client.exchange(
                uri,
                HttpMethod.DELETE,
                entity,
                String.class
                );
    }

    @Override
    public void updateTaskMessage(UUID id, String message) {
        LOGGER.debug("updateTaskMessage(fileSystemName={}, id={})", fileSystemName, id);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(message, headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put("taskId", id.toString());

        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/tasks/{taskId}")
                .buildAndExpand(params)
                .toUri();

        ResponseEntity<String> response = client.exchange(
                uri,
                HttpMethod.POST,
                entity,
                String.class
                );
    }

    @Override
    public Snapshot takeSnapshot(String projectId) {
        LOGGER.debug("takeSnapshot(fileSystemName={}, projectId={})", fileSystemName, projectId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);

        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/tasks/{taskId}")
                .queryParam("projectId", projectId)
                .buildAndExpand(params)
                .toUri();

        ResponseEntity<Snapshot> response = client.exchange(
                uri,
                HttpMethod.POST,
                entity,
                Snapshot.class
                );
        return response.getBody();
    }

    @Override
    public void addListener(TaskListener listener) {
        Objects.requireNonNull(listener);

        URI wsUri = getWebSocketUri(restUri);
        URI endPointUri = URI.create(wsUri + "/messages/afs/" +
                RemoteStorage.API_VERSION + "/task_events/" + fileSystemName + "/" + listener.getProjectId());

        LOGGER.debug("Connecting to task event websocket for file system {} at {}", fileSystemName, endPointUri);

        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        try {
            Session session = container.connectToServer(new TaskEventClient(listener), endPointUri);
            sessions.put(listener, session);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DeploymentException e) {
            throw new UncheckedDeploymentException(e);
        }
    }

    @Override
    public void removeListener(TaskListener listener) {
        Objects.requireNonNull(listener);

        Session session = sessions.remove(listener);
        if (session != null) {
            try {
                session.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void close() {
        for (Session session : sessions.values()) {
            try {
                session.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
