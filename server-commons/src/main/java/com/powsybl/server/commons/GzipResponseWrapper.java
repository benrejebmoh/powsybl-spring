/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.server.commons;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import org.springframework.http.HttpHeaders;

public class GzipResponseWrapper extends HttpServletResponseWrapper {

    private GzipResponseStream gzipResponse;
    private ServletOutputStream servletOuput;
    private PrintWriter printerWriter;

    public GzipResponseWrapper(HttpServletResponse response) {
        super(response);
    }

    public void finish() throws IOException {
        if (printerWriter != null) {
            printerWriter.close();
        }
        if (servletOuput != null) {
            servletOuput.close();
        }
        if (gzipResponse != null) {
            gzipResponse.close();
        }
    }

    @Override
    public void flushBuffer() throws IOException {
        if (printerWriter != null) {
            printerWriter.flush();
        }
        if (servletOuput != null) {
            servletOuput.flush();
        }
        super.flushBuffer();
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        if (servletOuput == null) {
            if (super.getHeader(HttpHeaders.CONTENT_ENCODING) != null && super.getHeader(HttpHeaders.CONTENT_ENCODING).equals("gzip")) {
                gzipResponse = new GzipResponseStream(new GZIPOutputStream(getResponse().getOutputStream()));
                servletOuput = gzipResponse;
            } else {
                servletOuput = getResponse().getOutputStream();
            }
        }
        return servletOuput;
    }

    @Override
    public PrintWriter getWriter() throws IOException {
        if (printerWriter == null) {
            if (super.getHeader(HttpHeaders.CONTENT_ENCODING) != null && super.getHeader(HttpHeaders.CONTENT_ENCODING).equals("gzip")) {
                gzipResponse = new GzipResponseStream(new GZIPOutputStream(getResponse().getOutputStream()));
                printerWriter = new PrintWriter(new OutputStreamWriter(gzipResponse, getResponse().getCharacterEncoding()));
            } else {
                printerWriter = new PrintWriter(new OutputStreamWriter(getResponse().getOutputStream(), getResponse().getCharacterEncoding()));
            }
        }
        return printerWriter;
    }

    class GzipResponseStream extends ServletOutputStream {

        private GZIPOutputStream gzipOutput;
        private final AtomicBoolean open = new AtomicBoolean(true);

        public GzipResponseStream(GZIPOutputStream zoutput) {
            this.gzipOutput = zoutput;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(WriteListener arg0) {
        }

        @Override
        public void flush() throws IOException {
            gzipOutput.flush();
        }

        @Override
        public void close() throws IOException {
            if (open.compareAndSet(true, false)) {
                gzipOutput.close();
            }
        }

        @Override
        public void write(int b) {
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (!open.get()) {
                throw new IOException("closed");
            }
            gzipOutput.write(b, off, len);
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b, 0, b.length);
        }
    }
}
