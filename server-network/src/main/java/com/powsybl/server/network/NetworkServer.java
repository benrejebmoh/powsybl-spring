/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.server.network;

import com.powsybl.afs.ProjectFile;
import com.powsybl.afs.ext.base.ProjectCase;
import com.powsybl.afs.ext.base.ScriptType;
import com.powsybl.client.storage.StorageService;
import com.powsybl.iidm.network.Network;
import com.powsybl.iidm.xml.NetworkXml;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@RestController
@RequestMapping(value = "/rest/network")
@Api(value = "/rest/network", tags = "network")
@ComponentScan(basePackageClasses = {StorageService.class, NetworkServer.class})
public class NetworkServer {

    public static final String API_VERSION = "v1";

    @Autowired
    private StorageService service;

    private ProjectCase getProjectCase(String fileSystemName, String nodeId) {
        return (ProjectCase) service.getFileSystem(fileSystemName)
                                          .findProjectFile(nodeId, ProjectFile.class);
    }

    @RequestMapping(method = RequestMethod.GET, value = "fileSystems/{fileSystemName}/nodes/{nodeId}", produces = MediaType.APPLICATION_XML_VALUE)
    @ApiOperation (value = "Get Network", response = StreamingResponseBody.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = "The available network"), @ApiResponse(code = 404, message = "No network found.")})
    public ResponseEntity<StreamingResponseBody> getNetwork(@ApiParam(value = "File System Name") @PathVariable("fileSystemName") String fileSystemName,
                        @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId) {
        Network network = getProjectCase(fileSystemName, nodeId).getNetwork();
        StreamingResponseBody streamingOutput = output -> NetworkXml.write(network, output);
        return ResponseEntity.ok().body(streamingOutput);
    }

    @RequestMapping(method = RequestMethod.POST, value = "fileSystems/{fileSystemName}/nodes/{nodeId}", consumes = MediaType.TEXT_PLAIN_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
    @ApiOperation (value = "Query Network", response = String.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = "Response of query"), @ApiResponse(code = 500, message = "Error.")})
    public ResponseEntity<String> queryNetwork(@ApiParam(value = "File System Name") @PathVariable("fileSystemName") String fileSystemName,
                        @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId,
                        @ApiParam(value = "Script Type") @PathVariable("scriptType") ScriptType scriptType,
                        @ApiParam(value = "Script Content") @RequestBody String scriptContent) {
        String resultJson = getProjectCase(fileSystemName, nodeId).queryNetwork(scriptType, scriptContent);
        return ResponseEntity.ok().body(resultJson);
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "fileSystems/{fileSystemName}/nodes/{nodeId}")
    @ApiOperation (value = "Invalidate Cache", response = String.class)
    @ApiResponses (value = {@ApiResponse(code = 200, message = "Cache invalidated"), @ApiResponse(code = 500, message = "Error.")})
    public ResponseEntity<String> invalidateCache(@ApiParam(value = "File System Name") @PathVariable("fileSystemName") String fileSystemName,
                        @ApiParam(value = "Node ID") @PathVariable("nodeId") String nodeId) {
        getProjectCase(fileSystemName, nodeId).invalidateNetworkCache();
        return ResponseEntity.ok().build();
    }
}
