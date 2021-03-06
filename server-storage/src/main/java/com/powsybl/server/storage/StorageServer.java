/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.server.storage;

import com.google.common.io.ByteStreams;
import com.powsybl.afs.AfsException;
import com.powsybl.afs.AppFileSystem;
import com.powsybl.afs.ProjectFile;
import com.powsybl.afs.TaskMonitor;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.NodeDependency;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.afs.storage.buffer.*;
import com.powsybl.server.commons.AppDataBean;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.TimeSeriesMetadata;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.*;

@RestController
@RequestMapping(value = "/rest/afs/" + StorageServer.API_VERSION)
@Api(value = "/afs", tags = "afs")
@ComponentScan(basePackageClasses = {AppDataBean.class, StorageServer.class})
public class StorageServer {

    public static final String API_VERSION = "v1";

    @Autowired
    private AppDataBean appDataBean;

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems")
    @ApiOperation (value = "Get file system list", response = List.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = "The list of available file systems"), @ApiResponse(code = 404, message = "There is no file system available.")})
    public List<String> getFileSystemNames() {
        return appDataBean.getAppData().getRemotelyAccessibleFileSystemNames();
    }

    @RequestMapping(method = RequestMethod.PUT, value = "fileSystems/{fileSystemName}/rootNode", produces = "application/json")
    @ApiOperation (value = "Get file system root node and create it if not exist", response = NodeInfo.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = "The root node"), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<NodeInfo> createRootNodeIfNotExists(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Root node name") @RequestParam("nodeName") String nodeName,
            @ApiParam(value = "Root node pseudo class") @RequestParam("nodePseudoClass") String nodePseudoClass) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        NodeInfo rootNodeInfo = storage.createRootNodeIfNotExists(nodeName, nodePseudoClass);
        return ResponseEntity.ok().body(rootNodeInfo);
    }

    @RequestMapping(method = RequestMethod.POST, value = "fileSystems/{fileSystemName}/flush", consumes = "application/json")
    @ApiOperation (value = "")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> flush(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                    @ApiParam(value = "Storage Change Set") @RequestBody StorageChangeSet changeSet) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);

        for (StorageChange change : changeSet.getChanges()) {
            switch (change.getType()) {
                case TIME_SERIES_CREATION:
                    TimeSeriesCreation creation = (TimeSeriesCreation) change;
                    storage.createTimeSeries(creation.getNodeId(), creation.getMetadata());
                    break;
                case DOUBLE_TIME_SERIES_CHUNKS_ADDITION:
                    DoubleTimeSeriesChunksAddition doubleAddition = (DoubleTimeSeriesChunksAddition) change;
                    storage.addDoubleTimeSeriesData(doubleAddition.getNodeId(), doubleAddition.getVersion(),
                                                    doubleAddition.getTimeSeriesName(), doubleAddition.getChunks());
                    break;
                case STRING_TIME_SERIES_CHUNKS_ADDITION:
                    StringTimeSeriesChunksAddition stringAddition = (StringTimeSeriesChunksAddition) change;
                    storage.addStringTimeSeriesData(stringAddition.getNodeId(), stringAddition.getVersion(),
                                                    stringAddition.getTimeSeriesName(), stringAddition.getChunks());
                    break;
                default:
                    throw new AssertionError("Unknown change type " + change.getType());
            }
        }
        // propagate flush to underlying storage
        storage.flush();
        return ResponseEntity.ok().build();
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/writable", produces = MediaType.TEXT_PLAIN_VALUE)
    @ApiOperation (value = "", response = Boolean.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> isWritable(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                        @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        boolean writable = storage.isWritable(nodeId);
        return ResponseEntity.ok().body(Boolean.toString(writable));
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/parent", produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "Get Parent Node", response = NodeInfo.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = "Returns the parent node"), @ApiResponse(code = 404, message = "No parent node for nodeId"), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<NodeInfo> getParentNode(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Optional<NodeInfo> parentNodeInfo = storage.getParentNode(nodeId);
        if (parentNodeInfo.isPresent()) {
            return ResponseEntity.ok().body(parentNodeInfo.get());
        } else {
            return ResponseEntity.ok().build(); //noContent().build();
        }
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}",  produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "", response = InputStream.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<NodeInfo> getNodeInfo(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                        @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        NodeInfo nodeInfo = storage.getNodeInfo(nodeId);
        return ResponseEntity.ok().body(nodeInfo);
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/children",  produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "Get child nodes", response = List.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = "The list of chid nodes"), @ApiResponse(code = 404, message = "Thera are no child nodes"), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<List<NodeInfo>> getChildNodes(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        List<NodeInfo> childNodes = storage.getChildNodes(nodeId);
        return ResponseEntity.ok().body(childNodes);
    }

    @RequestMapping(method = RequestMethod.POST, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/children/{childName}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "Create Node", response = NodeInfo.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = "The node is created"), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<NodeInfo> createNode(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
            @ApiParam(value = "Child Name") @PathVariable("childName") String childName,
            @ApiParam(value = "Description") @RequestParam("description") String description,
            @ApiParam(value = "Node Pseudo Class") @RequestParam("nodePseudoClass") String nodePseudoClass,
            @ApiParam(value = "Version") @RequestParam("version") int version,
            @ApiParam(value = "Node Meta Data") @RequestBody NodeGenericMetadata nodeMetadata) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        NodeInfo newNodeInfo =  storage.createNode(nodeId, childName, nodePseudoClass, description, version, nodeMetadata);
        return ResponseEntity.ok().body(newNodeInfo);
    }

    @RequestMapping(method = RequestMethod.PUT, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/name")
    @ApiOperation (value = "Rename Node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "The node is renamed"), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> renameNode(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
            @ApiParam(value = "New node's name") @RequestBody String name) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.renameNode(nodeId, name);
        return ResponseEntity.ok().build();
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/children/{childName}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "Get Child Node", response = NodeInfo.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = "Returns the child node"), @ApiResponse(code = 404, message = "No child node for nodeId"), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<NodeInfo> getChildNode(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
            @ApiParam(value = "Child Name") @PathVariable("childName") String childName) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Optional<NodeInfo> childNodeInfo = storage.getChildNode(nodeId, childName);
        if (childNodeInfo.isPresent()) {
            return ResponseEntity.ok().body(childNodeInfo.get());
        } else {
            return ResponseEntity.ok().build(); //ResponseEntity.noContent().build();
        }
    }

    @RequestMapping(method = RequestMethod.PUT, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/description", consumes = MediaType.TEXT_PLAIN_VALUE)
    @ApiOperation (value = "")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> setDescription(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
            @ApiParam(value = "Description") @RequestBody String description) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.setDescription(nodeId, description);
        return ResponseEntity.ok().build();
    }

    @RequestMapping(method = RequestMethod.PUT, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/modificationTime")
    @ApiOperation (value = "")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> updateModificationTime(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.updateModificationTime(nodeId);
        return ResponseEntity.ok().build();
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies", produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "", response = Set.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<Set<NodeDependency>> getDependencies(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<NodeDependency> dependencies = storage.getDependencies(nodeId);
        return ResponseEntity.ok().body(dependencies);
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/backwardDependencies", produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "", response = Set.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<Set<NodeInfo>> getBackwardDependencies(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<NodeInfo> backwardDependencyNodes = storage.getBackwardDependencies(nodeId);
        return ResponseEntity.ok().body(backwardDependencyNodes);
    }

    @RequestMapping(method = RequestMethod.PUT, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    @ApiOperation (value = "")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> writeBinaryData(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
            @ApiParam(value = "Name") @PathVariable("name") String name,
            @ApiParam(value = "Binary Data") InputStream is) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        try (OutputStream os = storage.writeBinaryData(nodeId, name)) {
            if (os == null) {
                return ResponseEntity.ok().build();
            } else {
                ByteStreams.copy(is, os);
                return ResponseEntity.ok().build();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/data", produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "", response = Set.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<Set<String>> getDataNames(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<String> dataNames = storage.getDataNames(nodeId);
        return ResponseEntity.ok().body(dataNames);
    }

    @RequestMapping(method = RequestMethod.PUT, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}/{toNodeId}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "Add dependency to Node")
    @ApiResponses (value = {@ApiResponse(code = 200, message = "Dependency is added"), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> addDependency(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
            @ApiParam(value = "Name") @PathVariable("name") String name,
            @ApiParam(value = "To Node ID") @PathVariable("toNodeId") String toNodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.addDependency(nodeId, name, toNodeId);
        return ResponseEntity.ok().build();
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "", response = Set.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<Set<NodeInfo>> getDependencies(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                    @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
                                    @ApiParam(value = "Name") @PathVariable("name") String name) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<NodeInfo> dependencies = storage.getDependencies(nodeId, name);
        return ResponseEntity.ok().body(dependencies);
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}/{toNodeId}")
    @ApiOperation (value = "")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> removeDependency(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
            @ApiParam(value = "Name") @PathVariable("name") String name,
            @ApiParam(value = "To Node ID") @PathVariable("toNodeId") String toNodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.removeDependency(nodeId, name, toNodeId);
        return ResponseEntity.ok().build();
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "fileSystems/{fileSystemName}/nodes/{nodeId}", produces = MediaType.TEXT_PLAIN_VALUE)
    @ApiOperation (value = "")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> deleteNode(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        String parentNodeId = storage.deleteNode(nodeId);
        return ResponseEntity.ok().body(parentNodeId);
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    @ApiOperation (value = "", response = InputStream.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<InputStreamResource> readBinaryAttribute(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
            @ApiParam(value = "Name") @PathVariable("name") String name) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Optional<InputStream> is = storage.readBinaryData(nodeId, name);
        if (is.isPresent()) {
            return ResponseEntity.ok().body(new InputStreamResource(is.get()));
        }
        return ResponseEntity.noContent().build();
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}", produces = MediaType.TEXT_PLAIN_VALUE)
    @ApiOperation (value = "", response = InputStream.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> dataExists(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
            @ApiParam(value = "Name") @PathVariable("name") String name) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        boolean exists = storage.dataExists(nodeId, name);
        return ResponseEntity.ok().body(Boolean.toString(exists));
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}", produces = MediaType.TEXT_PLAIN_VALUE)
    @ApiOperation (value = "", response = Boolean.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> removeData(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
            @ApiParam(value = "Data name") @PathVariable("name") String name) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        boolean removed = storage.removeData(nodeId, name);
        return ResponseEntity.ok().body(Boolean.toString(removed));
    }

    @RequestMapping(method = RequestMethod.POST, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String>  createTimeSeries(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
            @ApiParam(value = "Time Series Meta Data") TimeSeriesMetadata metadata) {

        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.createTimeSeries(nodeId, metadata);
        return ResponseEntity.ok().build();
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/name", produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "", response = Set.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<Set<String>> getTimeSeriesNames(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<String> timeSeriesNames = storage.getTimeSeriesNames(nodeId);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_ENCODING, "gzip")
                .body(timeSeriesNames);
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/{timeSeriesName}", produces = MediaType.TEXT_PLAIN_VALUE)
    @ApiOperation (value = "", response = Boolean.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> timeSeriesExists(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                    @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
                                    @ApiParam(value = "Time series name") @PathVariable("timeSeriesName") String timeSeriesName) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        boolean exists = storage.timeSeriesExists(nodeId, timeSeriesName);
        return  ResponseEntity.ok().body(Boolean.toString(exists));
    }

    @RequestMapping(method = RequestMethod.POST, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/metadata", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "", response = List.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<List<TimeSeriesMetadata>> getTimeSeriesMetadata(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
                                            @ApiParam(value = "Time series names") @RequestBody Set<String> timeSeriesNames) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        List<TimeSeriesMetadata> metadataList = storage.getTimeSeriesMetadata(nodeId, timeSeriesNames);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_ENCODING, "gzip")
                .body(metadataList);
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/versions", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Set<Integer>> getTimeSeriesDataVersions(@PathVariable("fileSystemName") String fileSystemName,
                                              @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<Integer> versions = storage.getTimeSeriesDataVersions(nodeId);
        return ResponseEntity.ok().body(versions);
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/{timeSeriesName}/versions", produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "", response = Set.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<Set<Integer>> getTimeSeriesDataVersions(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
                                            @ApiParam(value = "Time series name") @PathVariable("timeSeriesName") String timeSeriesName) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Set<Integer> versions = storage.getTimeSeriesDataVersions(nodeId, timeSeriesName);
        return ResponseEntity.ok().body(versions);
    }

    @RequestMapping(method = RequestMethod.POST, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/double/{version}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "", response = List.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<Map<String, List<DoubleDataChunk>>> getDoubleTimeSeriesData(@PathVariable("fileSystemName") String fileSystemName,
                                            @PathVariable("nodeId") String nodeId,
                                            @PathVariable("version") int version,
                                            @RequestBody Set<String> timeSeriesNames) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Map<String, List<DoubleDataChunk>> timeSeriesData = storage.getDoubleTimeSeriesData(nodeId, timeSeriesNames, version);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_ENCODING, "gzip")
                .body(timeSeriesData);
    }

    @RequestMapping(method = RequestMethod.POST, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/string/{version}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "", response = List.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 404, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<Map<String, List<StringDataChunk>>> getStringTimeSeriesData(@PathVariable("fileSystemName") String fileSystemName,
                                            @PathVariable("nodeId") String nodeId,
                                            @PathVariable("version") int version,
                                            @RequestBody Set<String> timeSeriesNames) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        Map<String, List<StringDataChunk>> timeSeriesData = storage.getStringTimeSeriesData(nodeId, timeSeriesNames, version);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_ENCODING, "gzip")
                .body(timeSeriesData);
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries")
    @ApiOperation (value = "")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> clearTimeSeries(@PathVariable("fileSystemName") String fileSystemName,
                                    @PathVariable("nodeId") String nodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.clearTimeSeries(nodeId);
        return ResponseEntity.ok().build();
    }

    @RequestMapping(method = RequestMethod.PUT, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/parent", consumes = MediaType.TEXT_PLAIN_VALUE)
    @ApiOperation (value = "")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> setParentNode(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                    @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
                                    @ApiParam(value = "New Parent Node ID") @RequestBody String newParentNodeId) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.setParentNode(nodeId, newParentNodeId);
        return ResponseEntity.ok().build();
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/tasks", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TaskMonitor.Snapshot> takeSnapshot(@PathVariable("fileSystemName") String fileSystemName,
            @RequestParam("projectId") String projectId) {
        AppFileSystem fileSystem = appDataBean.getFileSystem(fileSystemName);
        TaskMonitor.Snapshot snapshot = fileSystem.getTaskMonitor().takeSnapshot(projectId);
        return ResponseEntity.ok().body(snapshot);
    }

    @RequestMapping(method = RequestMethod.PUT, value = "fileSystems/{fileSystemName}/tasks", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TaskMonitor.Task> startTask(@PathVariable("fileSystemName") String fileSystemName,
            @RequestParam("projectFileId") String projectFileId) {
        AppFileSystem fileSystem = appDataBean.getFileSystem(fileSystemName);
        ProjectFile projectFile = fileSystem.findProjectFile(projectFileId, ProjectFile.class);
        if (projectFile == null) {
            throw new AfsException("Project file '" + projectFileId + "' not found in file system '" + fileSystemName + "'");
        }
        TaskMonitor.Task task = fileSystem.getTaskMonitor().startTask(projectFile);
        return ResponseEntity.ok().body(task);
    }

    @RequestMapping(method = RequestMethod.POST, value = "fileSystems/{fileSystemName}/tasks/{taskId}", consumes = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> updateTaskMessage(@PathVariable("fileSystemName") String fileSystemName,
                                      @PathVariable("taskId") UUID taskId,
                                      @RequestBody String message) {
        AppFileSystem fileSystem = appDataBean.getFileSystem(fileSystemName);
        fileSystem.getTaskMonitor().updateTaskMessage(taskId, message);
        return ResponseEntity.ok().build();
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "fileSystems/{fileSystemName}/tasks/{taskId}")
    public ResponseEntity<String> stopTask(@PathVariable("fileSystemName") String fileSystemName,
                             @PathVariable("taskId") UUID taskId) {
        AppFileSystem fileSystem = appDataBean.getFileSystem(fileSystemName);
        fileSystem.getTaskMonitor().stopTask(taskId);
        return ResponseEntity.ok().build();
    }

    @RequestMapping(method = RequestMethod.POST, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/double/{version}/{timeSeriesName}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation (value = "")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> addDoubleTimeSeriesData(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
                                            @ApiParam(value = "Version") @PathVariable("version") int version,
                                            @ApiParam(value = "Time series name") @PathVariable("timeSeriesName") String timeSeriesName,
                                            @ApiParam(value = "List double array chunk") @RequestBody List<DoubleDataChunk> chunks) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.addDoubleTimeSeriesData(nodeId, version, timeSeriesName, chunks);
        return ResponseEntity.ok().build();
    }

    @RequestMapping(method = RequestMethod.POST, value = "fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/string/{version}/{timeSeriesName}", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    @ApiOperation (value = "")
    @ApiResponses (value = {@ApiResponse(code = 200, message = ""), @ApiResponse(code = 500, message = "Error")})
    public ResponseEntity<String> addStringTimeSeriesData(@ApiParam(value = "File system name") @PathVariable("fileSystemName") String fileSystemName,
                                            @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
                                            @ApiParam(value = "Version") @PathVariable("version") int version,
                                            @ApiParam(value = "Time Series Name") @PathVariable("timeSeriesName") String timeSeriesName,
                                            @ApiParam(value = "List string array chunkFile system name") @RequestBody List<StringDataChunk> chunks) {
        AppStorage storage = appDataBean.getStorage(fileSystemName);
        storage.addStringTimeSeriesData(nodeId, version, timeSeriesName, chunks);
        return ResponseEntity.ok().build();
    }
}
