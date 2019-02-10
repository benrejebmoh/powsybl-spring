/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.client.storage;

import org.springframework.http.HttpHeaders;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.zip.GZIPOutputStream;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class ZippedClientHttpRequest implements ClientHttpRequest {

    private GZIPOutputStream zip;
    private final ClientHttpRequest delegate;

    public ZippedClientHttpRequest(ClientHttpRequest delegate) {
        if (delegate == null) {
            throw new IllegalArgumentException("null delegate");
        }
        this.delegate = delegate;
        this.delegate.getHeaders().add("Content-Encoding", "gzip");
    }

    @Override
    public OutputStream getBody() throws IOException {
        if (zip == null) {
            zip = new GZIPOutputStream(delegate.getBody());
        }
        return zip;
    }

    public void closeZip() throws IOException {
        if (zip != null) {
            zip.flush();
            zip.finish();
            zip.close();
        } else {
            this.delegate.getHeaders().remove("Content-Encoding");
        }
    }

    @Override
    public ClientHttpResponse execute() throws IOException {
        return delegate.execute();
    }

    @Override
    public String getMethodValue() {
        return delegate.getMethodValue();
    }

    @Override
    public URI getURI() {
        return delegate.getURI();
    }

    @Override
    public HttpHeaders getHeaders() {
        return delegate.getHeaders();
    }
}
