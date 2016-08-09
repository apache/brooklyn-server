package org.apache.brooklyn.util.http.executor.apacheclient;

import java.io.IOException;

import org.apache.brooklyn.util.http.HttpTool;
import org.apache.brooklyn.util.http.HttpTool.HttpClientBuilder;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.apache.brooklyn.util.http.executor.HttpConfig;
import org.apache.brooklyn.util.http.executor.HttpExecutor;
import org.apache.brooklyn.util.http.executor.HttpRequest;
import org.apache.brooklyn.util.http.executor.HttpResponse;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;

@Beta
public class HttpExecutorImpl implements HttpExecutor {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    
    private static final HttpConfig DEFAULT_CONFIG = HttpConfig.builder()
            .laxRedirect(false)
            .trustAll(false)
            .trustSelfSigned(false)
            .build();

    public static HttpExecutorImpl newInstance() {
        return newInstance(HttpTool.httpClientBuilder());
    }
    
    /**
     * This {@code baseBuilder} is used to construct a new {@link HttpClient} for each call to {@link #execute(HttpRequest)}.
     * Callers are strongly discouraged from modifying the baseBuilder after passing it in!
     */
    public static HttpExecutorImpl newInstance(HttpTool.HttpClientBuilder baseBuilder) {
        return new HttpExecutorImpl(baseBuilder);
    }

    private final HttpClientBuilder baseBuilder;
    
    protected HttpExecutorImpl(HttpClientBuilder baseBuilder) {
        this.baseBuilder = baseBuilder;
    }

    @Override
    public HttpResponse execute(HttpRequest request) throws IOException {
        HttpConfig config = (request.config() != null) ? request.config() : DEFAULT_CONFIG;
        Credentials creds = (request.credentials() != null) ? new UsernamePasswordCredentials(request.credentials().username(), request.credentials().password()) : null;
        HttpClient httpClient = HttpTool.HttpClientBuilder.fromBuilder(baseBuilder)
                .uri(request.uri())
                .https(request.isHttps())
                .credential(Optional.fromNullable(creds))
                .laxRedirect(config.laxRedirect())
                .trustSelfSigned(config.trustSelfSigned())
                .trustAll(config.trustAll())
                .build();
        
        HttpToolResponse response;
        
        switch (request.method().toLowerCase()) {
        case "get":
            response = HttpTool.httpGet(httpClient, request.uri(), request.headers());
            break;
        case "head":
            response = HttpTool.httpHead(httpClient, request.uri(), request.headers());
            break;
        case "post":
            response = HttpTool.httpPost(httpClient, request.uri(), request.headers(), orEmpty(request.body()));
            break;
        case "put":
            response = HttpTool.httpPut(httpClient, request.uri(), request.headers(), orEmpty(request.body()));
            break;
        case "delete":
            response = HttpTool.httpDelete(httpClient, request.uri(), request.headers());
            break;
        default:
            throw new IllegalArgumentException("Unsupported method '"+request.method()+"' for URI "+request.uri());
        }
        return new HttpResponseWrapper(response);
    }
    
    protected byte[] orEmpty(byte[] val) {
        return (val != null) ? val : EMPTY_BYTE_ARRAY;
    }
}
