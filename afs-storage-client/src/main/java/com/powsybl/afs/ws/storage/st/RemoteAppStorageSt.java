/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.storage.st;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.powsybl.afs.storage.*;
import com.powsybl.afs.storage.buffer.StorageChangeBuffer;
import com.powsybl.afs.storage.json.AppStorageJsonModule;
import com.powsybl.commons.exceptions.UncheckedInterruptedException;
import com.powsybl.commons.io.ForwardingInputStream;
import com.powsybl.services.utils.AfsRestApi;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.TimeSeriesMetadata;
import com.powsybl.timeseries.TimeSeriesVersions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.Resource;
import org.springframework.http.*;
import org.springframework.http.client.AsyncClientHttpRequest;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.client.*;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.powsybl.client.utils.ClientUtilsSt.checkOk;
import static com.powsybl.client.utils.ClientUtilsSt.readEntityIfOk;



/**
 * @author Ali Tahanout <ali.tahanout at rte-france.com>
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class RemoteAppStorageSt implements AppStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteAppStorageSt.class);

    private static final int BUFFER_MAXIMUM_CHANGE = 1000;
    private static final long BUFFER_MAXIMUM_SIZE = Math.round(Math.pow(2, 20)); // 1Mo
    private static final String FILE_SYSTEM_NAME = "fileSystemName";
    private static final String NODE_ID = "nodeId";
    private static final String VERSION = "version";
    private static final String NODE_DATA_PATH = "fileSystems/{fileSystemName}/nodes/{nodeId}/data/{name}";

    private final RestTemplate client;

    private final UriComponentsBuilder webTarget;

    private final String fileSystemName;

    private final StorageChangeBuffer changeBuffer;

    private String token;

    private boolean closed = false;

    public RemoteAppStorageSt(String fileSystemName, URI baseUri) {
        this(fileSystemName, baseUri, "");
    }
    public RemoteAppStorageSt(String fileSystemName, URI baseUri, String token) {
        this.fileSystemName = Objects.requireNonNull(fileSystemName);
        this.token = token;

        this.webTarget = getWebTarget(baseUri);
        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        this.client = createClient();

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.AUTHORIZATION, token);
        headers.add(HttpHeaders.CONTENT_ENCODING, "gzip");
        //headers.add(HttpHeaders.ACCEPT_ENCODING, "gzip");
        headers.add(HttpHeaders.AUTHORIZATION, token);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);

        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/flush")
                .buildAndExpand(params)
                .toUri();
        changeBuffer = new StorageChangeBuffer(changeSet -> {
            LOGGER.debug("flush(fileSystemName={}, size={})", fileSystemName, changeSet.getChanges().size());
            HttpEntity<com.powsybl.afs.storage.buffer.StorageChangeSet> entity = new HttpEntity<>(changeSet, headers);
            ResponseEntity<String> response = client.exchange(uri,
                    HttpMethod.POST,
                    entity,
                    String.class);

            checkOk(response);
        }, BUFFER_MAXIMUM_CHANGE, BUFFER_MAXIMUM_SIZE);
    }

    static RestTemplate createClient() {
        RestTemplate restTemplate = new RestTemplate() {
            @Override
            protected <T extends Object> T doExecute(URI url, HttpMethod method, final RequestCallback requestCallback, final ResponseExtractor<T> responseExtractor) throws RestClientException {
                return super.doExecute(url, method, new RequestCallback() {
                    @Override
                    public void doWithRequest(ClientHttpRequest chr) throws IOException {
                        if (method.equals(HttpMethod.GET) || method.equals(HttpMethod.DELETE)) {
                            requestCallback.doWithRequest(chr);
                        } else {
                            ZippedClientHttpRequest chr2 = new ZippedClientHttpRequest(chr);
                            if (chr.getHeaders().getFirst(HttpHeaders.CONTENT_ENCODING).equalsIgnoreCase("gzip")) {
                                requestCallback.doWithRequest(chr2);
                                chr2.closeZip();
                            } else {
                                requestCallback.doWithRequest(chr);
                            }
                        }
                    }
                }, new ResponseExtractor<T>() {
                    @Override
                    public T extractData(ClientHttpResponse chr) throws IOException {
                        if (chr.getHeaders().getFirst(HttpHeaders.CONTENT_ENCODING) != null && chr.getHeaders().getFirst(HttpHeaders.CONTENT_ENCODING).equalsIgnoreCase("gzip")) {
                            UnzippedClientHttpResponse chr2 = new UnzippedClientHttpResponse(chr);
                            return responseExtractor.extractData(chr2);
                        } else {
                            return responseExtractor.extractData(chr);
                        }
                    }
                });
            }
        };

        MappingJackson2HttpMessageConverter messageConverter = restTemplate.getMessageConverters().stream()
                .filter(MappingJackson2HttpMessageConverter.class::isInstance)
                .map(MappingJackson2HttpMessageConverter.class::cast)
                .findFirst().orElseThrow(() -> new RuntimeException("MappingJackson2HttpMessageConverter not found"));
        messageConverter.setSupportedMediaTypes(Arrays.asList(MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON_UTF8, MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_STREAM_JSON));
        messageConverter.getObjectMapper().registerModule(new AppStorageJsonModule());

        restTemplate.setMessageConverters(Arrays.asList(messageConverter));
        restTemplate.getMessageConverters().add(0, new StringHttpMessageConverter(Charset.forName("UTF-8")));
        restTemplate.getMessageConverters().add(1, new ByteArrayHttpMessageConverter());
        restTemplate.getMessageConverters().add(2, new ResourceHttpMessageConverter());

        return restTemplate;
    }

    static UriComponentsBuilder getWebTarget(URI baseUri) {
        UriComponentsBuilder ub = UriComponentsBuilder
                .fromUri(baseUri)
                .pathSegment("rest")
                .pathSegment(AfsRestApi.RESOURCE_ROOT)
                .pathSegment(AfsRestApi.VERSION);
        return ub;
    }

    @Override
    public String getFileSystemName() {
        return fileSystemName;
    }
    @Override
    public boolean isRemote() {
        return true;
    }
    public static List<String> getFileSystemNames(URI baseUri, String token) {
        RestTemplate client = createClient();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        UriComponentsBuilder webTargetTemp = getWebTarget(baseUri);
        URI uri = webTargetTemp
                .path("fileSystems")
                .build()
                .toUri();
        ResponseEntity<List<String>> response = client.exchange(
                    uri,
                    HttpMethod.GET,
                    entity,
                    new ParameterizedTypeReference<List<String>>() {});
        return readEntityIfOk(response);
    }

    @Override
    public NodeInfo createRootNodeIfNotExists(String name, String nodePseudoClass) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(nodePseudoClass);
        LOGGER.debug("createRootNodeIfNotExists(fileSystemName={}, name={}, nodePseudoClass={})",
                fileSystemName, name, nodePseudoClass);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.CONTENT_ENCODING, "gzip");
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/rootNode")
                .queryParam("nodeName", name)
                .queryParam("nodePseudoClass", nodePseudoClass)
                .buildAndExpand(params)
                .toUri();

        ResponseEntity<NodeInfo> response = client.exchange(
                    uri,
                    HttpMethod.PUT,
                    entity,
                    NodeInfo.class
                    );
        return readEntityIfOk(response);
    }

    @Override
    public boolean isWritable(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("isWritable(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        headers.setAccept(Collections.singletonList(MediaType.TEXT_PLAIN));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/writable")
                .buildAndExpand(params)
                .toUri();

        ResponseEntity<String> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                String.class
                );
        return Boolean.parseBoolean(readEntityIfOk(response));
    }

    @Override
    public void setDescription(String nodeId, String description) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(description);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("setDescription(fileSystemName={}, nodeId={}, description={})", fileSystemName, nodeId, description);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        //headers.setAccept(Collections.singletonList(MediaType.TEXT_PLAIN));
        headers.add(HttpHeaders.AUTHORIZATION, token);
        headers.add(HttpHeaders.CONTENT_ENCODING, "gzip");
        //headers.add(HttpHeaders.ACCEPT_ENCODING, "gzip");

        HttpEntity<String> entity = new HttpEntity<>(description, headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/description")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<String> response = client.exchange(
                    uri,
                    HttpMethod.PUT,
                    entity,
                    String.class
                    );
        checkOk(response);
    }

    @Override
    public void renameNode(String nodeId, String name) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("renameNode(fileSystemName={}, nodeId={}, name={})", fileSystemName, nodeId, name);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        headers.setAccept(Collections.singletonList(MediaType.TEXT_PLAIN));
        headers.add(HttpHeaders.AUTHORIZATION, token);
        headers.add(HttpHeaders.CONTENT_ENCODING, "gzip");
        //headers.add(HttpHeaders.ACCEPT_ENCODING, "gzip");

        HttpEntity<String> entity = new HttpEntity<>(name, headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);

        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/name")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<String> response = client.exchange(
                    uri,
                    HttpMethod.PUT,
                    entity,
                    String.class
                    );
        checkOk(response);
    }

    @Override
    public void updateModificationTime(String nodeId) {
        Objects.requireNonNull(nodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("updateModificationTime(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        headers.setAccept(Collections.singletonList(MediaType.TEXT_PLAIN));
        headers.add(HttpHeaders.AUTHORIZATION, token);
        headers.add(HttpHeaders.CONTENT_ENCODING, "gzip");
        //headers.add(HttpHeaders.ACCEPT_ENCODING, "gzip");

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);

        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/modificationTime")
                .buildAndExpand(params)
                .toUri();

        ResponseEntity<String> response = client.exchange(
                    uri,
                    HttpMethod.PUT,
                    entity,
                    String.class
                    );
        checkOk(response);
    }

    @Override
    public NodeInfo createNode(String parentNodeId, String name, String nodePseudoClass, String description,
            int version, NodeGenericMetadata genericMetadata) {
        Objects.requireNonNull(parentNodeId);
        Objects.requireNonNull(name);
        Objects.requireNonNull(nodePseudoClass);
        Objects.requireNonNull(description);
        Objects.requireNonNull(genericMetadata);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("createNode(fileSystemName={}, parentNodeId={}, name={}, nodePseudoClass={}, description={}, version={}, genericMetadata={})",
                fileSystemName, parentNodeId, name, nodePseudoClass, description, version, genericMetadata);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);
        headers.add(HttpHeaders.CONTENT_ENCODING, "gzip");
        //headers.add(HttpHeaders.ACCEPT_ENCODING, "gzip");

        HttpEntity<NodeGenericMetadata> entity = new HttpEntity<>(genericMetadata, headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, parentNodeId);
        params.put("childName", name);

        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/children/{childName}")
                .queryParam("nodePseudoClass", nodePseudoClass)
                .queryParam("description", description)
                .queryParam(VERSION, version)
                .buildAndExpand(params)
                .toUri();

        ResponseEntity<NodeInfo> response = client.exchange(
                    uri,
                    HttpMethod.POST,
                    entity,
                    NodeInfo.class
                    );
        return readEntityIfOk(response);
    }

    @Override
    public List<NodeInfo> getChildNodes(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getChildNodes(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/children")
                .buildAndExpand(params)
                .toUri();

        ResponseEntity<List<NodeInfo>> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<List<NodeInfo>>() { }
                );
        return readEntityIfOk(response);
    }

    @Override
    public Optional<NodeInfo> getChildNode(String nodeId, String name) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);

        LOGGER.debug("getChildNode(fileSystemName={}, nodeId={}, name={})", fileSystemName, nodeId, name);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        params.put("childName", name);

        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/children/{childName}")
                .buildAndExpand(params)
                .toUri();

        ResponseEntity<NodeInfo> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                NodeInfo.class
                );
        return Optional.ofNullable(readEntityIfOk(response));
    }

    @Override
    public Optional<NodeInfo> getParentNode(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getParentNode(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/parent")
                .buildAndExpand(params)
                .toUri();

        ResponseEntity<NodeInfo> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                NodeInfo.class
                );
        return Optional.ofNullable(readEntityIfOk(response));
    }

    @Override
    public void setParentNode(String nodeId, String newParentNodeId) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(newParentNodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("setParentNode(fileSystemName={}, nodeId={}, newParentNodeId={})", fileSystemName, nodeId, newParentNodeId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        headers.setAccept(Collections.singletonList(MediaType.TEXT_PLAIN));
        headers.add(HttpHeaders.AUTHORIZATION, token);
        headers.add(HttpHeaders.CONTENT_ENCODING, "gzip");
        //headers.add(HttpHeaders.ACCEPT_ENCODING, "gzip");

        HttpEntity<String> entity = new HttpEntity<>(newParentNodeId, headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/parent")
                .buildAndExpand(params)
                .toUri();

        ResponseEntity<String> response = client.exchange(
                uri,
                HttpMethod.PUT,
                entity,
                String.class
                );
        checkOk(response);
    }

    @Override
    public String deleteNode(String nodeId) {
        Objects.requireNonNull(nodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("deleteNode(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        //headers.setContentType(MediaType.APPLICATION_JSON);
        //headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}")
                .buildAndExpand(params)
                .toUri();

        ResponseEntity<String> response = client.exchange(
                uri,
                HttpMethod.DELETE,
                entity,
                String.class
                );
        return readEntityIfOk(response);
    }

    @Override
    public OutputStream writeBinaryData(String nodeId, String name) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("writeBinaryData(fileSystemName={}, nodeId={}, name={})", fileSystemName, nodeId, name);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_OCTET_STREAM));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        params.put("name", name);

        AsyncRestTemplate aRestTemp = new AsyncRestTemplate();
        MappingJackson2HttpMessageConverter messageConverter = aRestTemp.getMessageConverters().stream()
                .filter(MappingJackson2HttpMessageConverter.class::isInstance)
                .map(MappingJackson2HttpMessageConverter.class::cast)
                .findFirst().orElseThrow(() -> new RuntimeException("MappingJackson2HttpMessageConverter not found"));

        messageConverter.setSupportedMediaTypes(Arrays.asList(MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON_UTF8, MediaType.APPLICATION_JSON,
                MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_STREAM_JSON));
        messageConverter.getObjectMapper().registerModule(new AppStorageJsonModule());

        aRestTemp.setMessageConverters(Arrays.asList(messageConverter));
        aRestTemp.getMessageConverters().add(0, new StringHttpMessageConverter(Charset.forName("UTF-8")));
        aRestTemp.getMessageConverters().add(1, new ByteArrayHttpMessageConverter());
        aRestTemp.getMessageConverters().add(2, new ResourceHttpMessageConverter());

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();
        URI uri = webTargetTemp
                .path(NODE_DATA_PATH)
                .buildAndExpand(params)
                .toUri();
        AsyncClientHttpRequest asyncClientHttpRequest = null;
        try {
            asyncClientHttpRequest = aRestTemp.getAsyncRequestFactory().createAsyncRequest(uri, HttpMethod.PUT);
            asyncClientHttpRequest.getHeaders().addAll(headers);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new OutputStreamPut(asyncClientHttpRequest);
    }

    public static class OutputStreamPut extends OutputStream {
        private ListenableFuture<ClientHttpResponse> response;
        private AsyncClientHttpRequest asyncClientHttpRequest;
        public OutputStreamPut(AsyncClientHttpRequest asyncClientHttpRequest) {
            this.asyncClientHttpRequest = asyncClientHttpRequest;
        }
        @Override
        public void write(byte[] b, int off, int len) {
            try {
                asyncClientHttpRequest.getBody().write(b, off, len);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        @Override
        public void write(byte[] b) throws IOException {
            if (asyncClientHttpRequest != null) {
                asyncClientHttpRequest.getBody().write(b);
            }
        }
        @Override
        public void write(int b) {
            try {
                asyncClientHttpRequest.getBody().write(b);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            super.flush();
            response = asyncClientHttpRequest.executeAsync();
            super.close();
            try {
                checkOk(response.get());
            } catch (ExecutionException e) {
                throw new UncheckedExecutionException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedInterruptedException(e);
            }
        }

        public static void checkOk(ClientHttpResponse response) throws IOException {
            int status = response.getStatusCode().value();
            if (status != HttpStatus.OK.value()) {
                if (status == HttpStatus.INTERNAL_SERVER_ERROR.value()) {
                    throw createServerErrorException(response);
                } else {
                    throw createUnexpectedResponseStatus(response.getStatusCode());
                }
            }
        }
        private static AfsStorageException createServerErrorException(ClientHttpResponse response) throws IOException {
            return new AfsStorageException(response.getStatusText());
        }

        private static AfsStorageException createUnexpectedResponseStatus(HttpStatus status) {
            return new AfsStorageException("Unexpected response status: '" + status + "'");
        }
    }

    @Override
    public Optional<InputStream> readBinaryData(String nodeId, String name) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);

        LOGGER.debug("readBinaryData(fileSystemName={}, nodeId={}, name={})", fileSystemName, nodeId, name);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_OCTET_STREAM));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<String, String>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        params.put("name", name);
        URI uri = webTargetTemp
                .path(NODE_DATA_PATH)
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<Resource> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<Resource>() { }
                );
        try {
            if (response.getStatusCode().value() == HttpStatus.OK.value()) {
                return Optional.of(response.getBody().getInputStream()).map(is -> new ForwardingInputStream<InputStream>(is) {
                    @Override
                    public void close() throws IOException {
                        super.close();
                    }
                });
            } else if (response.getStatusCode().value() == HttpStatus.NO_CONTENT.value()) {
                return Optional.empty();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Optional.empty();
    }

    @Override
    public boolean dataExists(String nodeId, String name) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);

        LOGGER.debug("dataExists(fileSystemName={}, nodeId={}, name={})", fileSystemName, nodeId, name);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        headers.setAccept(Collections.singletonList(MediaType.TEXT_PLAIN));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        params.put("name", name);
        URI uri = webTargetTemp
                .path(NODE_DATA_PATH)
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<Boolean> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<Boolean>() { }
                );
        return readEntityIfOk(response);
    }

    @Override
    public Set<String> getDataNames(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getDataNames(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/data")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<Set<String>> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<Set<String>>() { }
                );
        return readEntityIfOk(response);
    }

    @Override
    public boolean removeData(String nodeId, String name) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);

        LOGGER.debug("removeData(fileSystemName={}, nodeId={}, name={})", fileSystemName, nodeId, name);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        headers.setAccept(Collections.singletonList(MediaType.TEXT_PLAIN));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        params.put("name", name);

        URI uri = webTargetTemp
                .path(NODE_DATA_PATH)
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<Boolean> response = client.exchange(
                uri,
                HttpMethod.DELETE,
                entity,
                Boolean.class
                );
        return readEntityIfOk(response);
    }

    @Override
    public void addDependency(String nodeId, String name, String toNodeId) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);
        Objects.requireNonNull(toNodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("addDependency(fileSystemName={}, nodeId={}, name={}, toNodeId={})", fileSystemName, nodeId, name, toNodeId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);
        headers.add(HttpHeaders.CONTENT_ENCODING, "gzip");

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        params.put("name", name);
        params.put("toNodeId", toNodeId);

        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}/{toNodeId}")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<String> response = client.exchange(
                uri,
                HttpMethod.PUT,
                entity,
                String.class
                );
        checkOk(response);
    }

    @Override
    public Set<NodeInfo> getDependencies(String nodeId, String name) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);

        LOGGER.debug("getDependencies(fileSystemName={}, nodeId={}, name={})", fileSystemName, nodeId, name);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        params.put("name", name);

        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<Set<NodeInfo>> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<Set<NodeInfo>>() { }
                );
        return readEntityIfOk(response);
    }

    @Override
    public Set<NodeDependency> getDependencies(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getDependencies(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies")
                .buildAndExpand(params)
                .toUri();

        ResponseEntity<Set<NodeDependency>> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<Set<NodeDependency>>() { }
                );
        return readEntityIfOk(response);
    }

    @Override
    public Set<NodeInfo> getBackwardDependencies(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getBackwardDependencies(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/backwardDependencies")
                .buildAndExpand(params)
                .toUri();

        ResponseEntity<Set<NodeInfo>> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<Set<NodeInfo>>() { }
                );
        return readEntityIfOk(response);
    }

    @Override
    public void removeDependency(String nodeId, String name, String toNodeId) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(name);
        Objects.requireNonNull(toNodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("removeDependency(fileSystemName={}, nodeId={}, name={}, toNodeId={})", fileSystemName, nodeId, name, toNodeId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        params.put("name", name);
        params.put("toNodeId", toNodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/dependencies/{name}/{toNodeId}")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<String> response = client.exchange(
                uri,
                HttpMethod.DELETE,
                entity,
                String.class
                );
        checkOk(response);
    }

    @Override
    public void createTimeSeries(String nodeId, TimeSeriesMetadata metadata) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(metadata);

        LOGGER.debug("createTimeSeries(fileSystemName={}, nodeId={}, metadata={}) [BUFFERED]", fileSystemName, nodeId, metadata);

        changeBuffer.createTimeSeries(nodeId, metadata);
    }

    @Override
    public Set<String> getTimeSeriesNames(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getTimeSeriesNames(fileSystemName={}, nodeId={})", fileSystemName, nodeId);
        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.ACCEPT_ENCODING, "gzip");
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/name")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<Set<String>> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<Set<String>>() { }
                );
        return readEntityIfOk(response);
    }

    @Override
    public boolean timeSeriesExists(String nodeId, String timeSeriesName) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(timeSeriesName);

        LOGGER.debug("timeSeriesExists(fileSystemName={}, nodeId={}, timeSeriesName={})", fileSystemName, nodeId, timeSeriesName);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        headers.setAccept(Collections.singletonList(MediaType.TEXT_PLAIN));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        params.put("timeSeriesName", timeSeriesName);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/{timeSeriesName}")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<Boolean> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                Boolean.class
                );
        return readEntityIfOk(response);
    }

    @Override
    public List<TimeSeriesMetadata> getTimeSeriesMetadata(String nodeId, Set<String> timeSeriesNames) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(timeSeriesNames);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("getTimeSeriesMetadata(fileSystemName={}, nodeId={}, timeSeriesNames={})", fileSystemName, nodeId, timeSeriesNames);
        }

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.ACCEPT_ENCODING, "gzip");
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<Set<String>> entity = new HttpEntity<>(timeSeriesNames, headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/metadata")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<List<TimeSeriesMetadata>> response = client.exchange(
                uri,
                HttpMethod.POST,
                entity,
                new ParameterizedTypeReference<List<TimeSeriesMetadata>>() { }
                );
        return readEntityIfOk(response);
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getTimeSeriesDataVersions(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/versions")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<Set<Integer>> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<Set<Integer>>() { }
                );
        return readEntityIfOk(response);
    }

    @Override
    public Set<Integer> getTimeSeriesDataVersions(String nodeId, String timeSeriesName) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(timeSeriesName);

        LOGGER.debug("getTimeSeriesDataVersions(fileSystemName={}, nodeId={}, timeSeriesNames={})", fileSystemName, nodeId, timeSeriesName);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        params.put("timeSeriesName", timeSeriesName);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/{timeSeriesName}/versions")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<Set<Integer>> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<Set<Integer>>() { }
                );
        return readEntityIfOk(response);
    }

    @Override
    public void addDoubleTimeSeriesData(String nodeId, int version, String timeSeriesName, List<DoubleDataChunk> chunks) {
        Objects.requireNonNull(nodeId);
        TimeSeriesVersions.check(version);
        Objects.requireNonNull(timeSeriesName);
        Objects.requireNonNull(chunks);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("addDoubleTimeSeriesData(fileSystemName={}, nodeId={}, version={}, timeSeriesName={}, chunks={}) [BUFFERED]",
                    fileSystemName, nodeId, version, timeSeriesName, chunks);
        }

        changeBuffer.addDoubleTimeSeriesData(nodeId, version, timeSeriesName, chunks);
    }

    @Override
    public Map<String, List<DoubleDataChunk>> getDoubleTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(timeSeriesNames);
        TimeSeriesVersions.check(version);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("getDoubleTimeSeriesData(fileSystemName={}, nodeId={}, timeSeriesNames={}, version={})",
                    fileSystemName, nodeId, timeSeriesNames, version);
        }

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.ACCEPT_ENCODING, "gzip");
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<Set<String>> entity = new HttpEntity<>(timeSeriesNames, headers);

        Map<String, Object> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        params.put(VERSION, version);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/double/{version}")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<Map<String, List<DoubleDataChunk>>> response = client.exchange(
                uri,
                HttpMethod.POST,
                entity,
                new ParameterizedTypeReference<Map<String, List<DoubleDataChunk>>>() { }
                );
        return readEntityIfOk(response);
    }

    @Override
    public void addStringTimeSeriesData(String nodeId, int version, String timeSeriesName, List<StringDataChunk> chunks) {
        Objects.requireNonNull(nodeId);
        TimeSeriesVersions.check(version);
        Objects.requireNonNull(timeSeriesName);
        Objects.requireNonNull(chunks);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("addStringTimeSeriesData(fileSystemName={}, nodeId={}, version={}, timeSeriesName={}, chunks={}) [BUFFERED]",
                    fileSystemName, nodeId, version, timeSeriesName, chunks);
        }
        changeBuffer.addStringTimeSeriesData(nodeId, version, timeSeriesName, chunks);
    }

    @Override
    public Map<String, List<StringDataChunk>> getStringTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(timeSeriesNames);
        TimeSeriesVersions.check(version);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("getStringTimeSeriesData(fileSystemName={}, nodeId={}, timeSeriesNames={}, version={})",
                    fileSystemName, nodeId, timeSeriesNames, version);
        }

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.ACCEPT_ENCODING, "gzip");
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<Set<String>> entity = new HttpEntity<>(timeSeriesNames, headers);

        Map<String, Object> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        params.put(VERSION, version);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries/string/{version}")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<Map<String, List<StringDataChunk>>> response = client.exchange(
                uri,
                HttpMethod.POST,
                entity,
                new ParameterizedTypeReference<Map<String, List<StringDataChunk>> >() { }
                );
        return readEntityIfOk(response);
    }

    @Override
    public void clearTimeSeries(String nodeId) {
        Objects.requireNonNull(nodeId);

        // flush buffer to keep change order
        changeBuffer.flush();

        LOGGER.debug("clearTimeSeries(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, Object> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}/timeSeries")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<String> response = client.exchange(
                uri,
                HttpMethod.DELETE,
                entity,
                String.class
                );
        checkOk(response);
    }

    @Override
    public NodeInfo getNodeInfo(String nodeId) {
        Objects.requireNonNull(nodeId);

        LOGGER.debug("getNodeInfo(fileSystemName={}, nodeId={})", fileSystemName, nodeId);

        UriComponentsBuilder webTargetTemp = webTarget.cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.add(HttpHeaders.AUTHORIZATION, token);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        Map<String, String> params = new HashMap<>();
        params.put(FILE_SYSTEM_NAME, fileSystemName);
        params.put(NODE_ID, nodeId);
        URI uri = webTargetTemp
                .path("fileSystems/{fileSystemName}/nodes/{nodeId}")
                .buildAndExpand(params)
                .toUri();
        ResponseEntity<NodeInfo> response = client.exchange(
                uri,
                HttpMethod.GET,
                entity,
                NodeInfo.class
                );
        return readEntityIfOk(response);
    }

    @Override
    public void flush() {
        changeBuffer.flush();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        flush();
        closed = true;
        //client.close();
    }
}
