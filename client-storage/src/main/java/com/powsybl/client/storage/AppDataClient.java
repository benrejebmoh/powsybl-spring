/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.client.storage;

import com.powsybl.afs.AfsException;
import com.powsybl.afs.AppFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
@Profile("default")
@Component
public class AppDataClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppDataClient.class);

    @Autowired
    private DiscoveryClient eurekaClient;

    private Map<String, AppFileSystem> fileSystems;

    @PostConstruct
    public void init() {
        List<ServiceInstance> instances = eurekaClient.getInstances("STORAGE");
        URI baseUri = instances.get(0).getUri();
        String token = null;
        fileSystems = RemoteStorage.getFileSystemNames(baseUri, token).stream()
                .map(fileSystemName -> {
                    LOGGER.info("Connect to file system '{}'", fileSystemName);
                    RemoteStorage storage = new RemoteStorage(fileSystemName, baseUri, token);
                    RemoteListenableStorage listenableStorage = new RemoteListenableStorage(storage, baseUri);
                    RemoteTaskMonitor taskMonitor = new RemoteTaskMonitor(fileSystemName, baseUri, token);
                    return new AppFileSystem(fileSystemName, true, listenableStorage, taskMonitor);
                })
                .collect(Collectors.toMap(AppFileSystem::getName, fileSystem -> fileSystem));
    }

    public AppFileSystem getFileSystem(String name) {
        AppFileSystem fileSystem = fileSystems.get(name);
        if (fileSystem == null) {
            throw new AfsException("File system '" + name + "' not found");
        }
        return fileSystem;
    }
}
