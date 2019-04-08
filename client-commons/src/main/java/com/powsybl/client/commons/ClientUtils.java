/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.client.commons;

import com.powsybl.afs.storage.AfsStorageException;
import com.powsybl.afs.storage.json.AppStorageJsonModule;
import com.powsybl.commons.net.UserProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

public final class ClientUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientUtils.class);

    private ClientUtils() {
    }

    public static void checkOk(ResponseEntity<?> response) {
        int status = response.getStatusCode().value();
        if (status != HttpStatus.OK.value()) {
            if (status == HttpStatus.INTERNAL_SERVER_ERROR.value()) {
                throw createServerErrorException(response);
            } else {
                throw createUnexpectedResponseStatus(response.getStatusCode());
            }
        }
    }

    private static AfsStorageException createServerErrorException(ResponseEntity response) {
        return new AfsStorageException(Objects.requireNonNull(response.getBody()).toString());
    }

    private static AfsStorageException createUnexpectedResponseStatus(HttpStatus status) {
        return new AfsStorageException("Unexpected response status: '" + status + "'");
    }

    public static <T> T readEntityIfOk(ResponseEntity<T> response) {
        int status = response.getStatusCode().value();
        if (status == HttpStatus.OK.value()) {
            T entity = response.getBody();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("    --> {}", entity);
            }
            return entity;
        } else if (status == HttpStatus.INTERNAL_SERVER_ERROR.value()) {
            throw createServerErrorException(response);
        } else {
            throw createUnexpectedResponseStatus(response.getStatusCode());
        }
    }

    public static UserSession authenticate(URI baseUri, String login, String password) {
        Objects.requireNonNull(baseUri);
        Objects.requireNonNull(login);
        Objects.requireNonNull(password);

        RestTemplate client = createClient();
        UriComponentsBuilder webTargetTemp = getWebTarget(baseUri).cloneBuilder();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("login", login);
        form.add("password", password);

        HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<>(form, headers);

        URI uri = webTargetTemp
                .path("login")
                .build()
                .toUri();

        ResponseEntity<UserProfile> response = client.exchange(
                uri,
                HttpMethod.POST,
                entity,
                UserProfile.class
                );
        UserProfile profile = readEntityIfOk(response);
        String token = response.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        return new UserSession(profile, token);
    }

    static RestTemplate createClient() {
        RestTemplate restTemplate = new RestTemplate();
        MappingJackson2HttpMessageConverter messageConverter = restTemplate.getMessageConverters().stream()
                .filter(MappingJackson2HttpMessageConverter.class::isInstance)
                .map(MappingJackson2HttpMessageConverter.class::cast)
                .findFirst().orElseThrow(() -> new RuntimeException("MappingJackson2HttpMessageConverter not found"));
        messageConverter.setSupportedMediaTypes(Arrays.asList(MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON_UTF8, MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_STREAM_JSON));
        messageConverter.getObjectMapper().registerModule(new AppStorageJsonModule());

        restTemplate.setMessageConverters(Collections.singletonList(messageConverter));
        restTemplate.getMessageConverters().add(0, new StringHttpMessageConverter(Charset.forName("UTF-8")));
        restTemplate.getMessageConverters().add(1, new ByteArrayHttpMessageConverter());
        restTemplate.getMessageConverters().add(2, new ResourceHttpMessageConverter());
        restTemplate.getMessageConverters().add(3, new FormHttpMessageConverter());
        return restTemplate;
    }

    static UriComponentsBuilder getWebTarget(URI baseUri) {
        return UriComponentsBuilder
                .fromUri(baseUri)
                .pathSegment("rest")
                .pathSegment("users");
    }
}
