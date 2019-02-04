package com.powsybl.afs.ws.storage.st;

import com.powsybl.afs.storage.ForwardingAppStorage;
import com.powsybl.afs.storage.ListenableAppStorage;
import com.powsybl.afs.storage.events.AppStorageListener;
import com.powsybl.client.commons.UncheckedDeploymentException;
import com.powsybl.commons.exceptions.UncheckedUriSyntaxException;
import com.powsybl.commons.util.WeakListenerList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.websocket.ContainerProvider;
import javax.websocket.DeploymentException;
import javax.websocket.WebSocketContainer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;

public class RemoteListenableStorage extends ForwardingAppStorage implements ListenableAppStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteListenableStorage.class);

    private final WeakListenerList<AppStorageListener> listeners = new WeakListenerList<>();

    public RemoteListenableStorage(RemoteStorage storage, URI restUri) {
        super(storage);

        URI wsUri = getWebSocketUri(restUri);
        URI endPointUri = URI.create(wsUri + "/messages/afs/" +
                RemoteStorage.API_VERSION + "/node_events/" + storage.getFileSystemName());
        LOGGER.debug("Connecting to node event websocket at {}", endPointUri);

        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        try {
            container.connectToServer(new NodeEventClient(storage.getFileSystemName(), listeners), endPointUri);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DeploymentException e) {
            throw new UncheckedDeploymentException(e);
        }
    }

    static URI getWebSocketUri(URI restUri) {
        try {
            String wsScheme;
            switch (restUri.getScheme()) {
                case "http":
                    wsScheme = "ws";
                    break;
                case "https":
                    wsScheme = "wss";
                    break;
                default:
                    throw new AssertionError("Unexpected scheme " + restUri.getScheme());
            }
            return new URI(wsScheme, restUri.getUserInfo(), restUri.getHost(), restUri.getPort(), restUri.getPath(), restUri.getQuery(), null);
        } catch (URISyntaxException e) {
            throw new UncheckedUriSyntaxException(e);
        }
    }

    @Override
    public void addListener(AppStorageListener l) {
        listeners.add(l);
    }

    @Override
    public void removeListener(AppStorageListener l) {
        listeners.remove(l);
    }

    @Override
    public void removeListeners() {
        listeners.removeAll();
    }
}
