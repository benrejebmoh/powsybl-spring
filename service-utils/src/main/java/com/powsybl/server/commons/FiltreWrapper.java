package com.powsybl.server.commons;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;

@Configuration
public class FiltreWrapper implements Filter {

    @Override
    public final void doFilter(final ServletRequest servletRequest,
                               final ServletResponse servletResponse,
                               final FilterChain chain) throws IOException, ServletException {

        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        boolean isGzipped = request.getHeader(HttpHeaders.CONTENT_ENCODING) != null && request.getHeader(HttpHeaders.CONTENT_ENCODING).contains("gzip");

        boolean requestTypeSupported = "POST".equals(request.getMethod()) || "PUT".equals(request.getMethod());
        if (isGzipped && !requestTypeSupported) {
            throw new IllegalStateException(request.getMethod()
                    + " is not supports gzipped body of parameters."
                    + " Only POST requests are currently supported.");
        }
        if (isGzipped && requestTypeSupported) {
            request = new GzippedInputStreamWrapper((HttpServletRequest) servletRequest);
        }
        GzipResponseWrapper gzipResponse = new GzipResponseWrapper(response);
        chain.doFilter(request, gzipResponse);
        gzipResponse.finish();
    }

    @Override
    public void init(FilterConfig filterConfig) {
    }

    @Override
    public void destroy() {
    }
}
