/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.server.commons;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.zip.GZIPInputStream;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

public class GzippedInputStreamWrapper extends HttpServletRequestWrapper {

    private final InputStream in;

    public GzippedInputStreamWrapper(final HttpServletRequest request) throws IOException {
        super(request);
        try {
            in = new GZIPInputStream(request.getInputStream());
        } catch (EOFException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public ServletInputStream getInputStream() {
        return new ServletInputStream() {
            @Override
            public int read() throws IOException {
                return in.read();
            }

            @Override
            public void close() throws IOException {
                super.close();
                in.close();
            }

            @Override
            public boolean isFinished() {
                return false;
            }

            @Override
            public boolean isReady() {
                return false;
            }

            @Override
            public void setReadListener(ReadListener listener) {
            }
        };
    }
}
