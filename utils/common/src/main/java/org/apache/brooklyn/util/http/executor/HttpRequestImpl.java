package org.apache.brooklyn.util.http.executor;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;

import org.apache.brooklyn.util.http.executor.Credentials.BasicAuth;

import com.google.common.annotations.Beta;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * A (mostly) immutable http request. However, (for efficiency reasons) we do not copy the body.
 */
@Beta
public class HttpRequestImpl implements HttpRequest {

    protected final boolean isHttps;
    protected final URI uri;
    protected final String method;
    protected final byte[] body;
    protected final Multimap<String, String> headers;
    protected final Credentials.BasicAuth credentials;
    protected final HttpConfig config;

    protected HttpRequestImpl(HttpRequest.Builder builder) {
        this.isHttps = builder.isHttps;
        this.uri = checkNotNull(builder.uri, "uri");
        this.method = checkNotNull(builder.method, "method");
        this.body = builder.body;
        this.headers = Multimaps.unmodifiableMultimap(ArrayListMultimap.create(checkNotNull(builder.headers, "headers")));
        this.credentials = builder.credentials;
        this.config = builder.config;
    }
    
    @Override
    public boolean isHttps() {
        return isHttps;
    }
    @Override
    public URI uri() {
        return uri;
    }
    
    @Override
    public String method() {
        return method;
    }
    
    @Override
    public byte[] body() {
        return body;
    }
    
    @Override
    public Multimap<String, String> headers() {
        return headers;
    }
    
    @Override
    public BasicAuth credentials() {
        return credentials;
    }
    
    @Override
    public HttpConfig config() {
        return config;
    }
}
