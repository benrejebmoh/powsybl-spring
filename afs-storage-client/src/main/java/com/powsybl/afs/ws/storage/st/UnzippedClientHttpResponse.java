/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.storage.st;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class UnzippedClientHttpResponse implements ClientHttpResponse {

    private final ClientHttpResponse delegate;

    private GZIPInputStream zip;

    public UnzippedClientHttpResponse(ClientHttpResponse delegate) {
        this.delegate = delegate;
    }

    @Override
    public InputStream getBody() throws IOException {
        this.zip = new GZIPInputStream(this.delegate.getBody());
        return zip;
    }

    @Override
    public HttpHeaders getHeaders() {
        return this.delegate.getHeaders();
    }

    @Override
    public void close() {
        this.delegate.close();
    }

    @Override
    public int getRawStatusCode() throws IOException {
        return this.delegate.getRawStatusCode();
    }

    @Override
    public HttpStatus getStatusCode() throws IOException {
        return this.delegate.getStatusCode();
    }

    @Override
    public String getStatusText() throws IOException {
        return this.delegate.getStatusText();
    }
}
