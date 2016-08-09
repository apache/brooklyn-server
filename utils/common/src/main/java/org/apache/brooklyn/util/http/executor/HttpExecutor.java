package org.apache.brooklyn.util.http.executor;

import java.io.IOException;

/**
 * An abstraction for executing HTTP requests, allowing an appropriate implementation to be chosen.
 */
public interface HttpExecutor {
    
    /**
     * Synchronously send the request, and return its response.
     * 
     * To avoid leaking resources callers must close the response's content (or the response itself).
     *  
     * @throws IOException if a problem occurred talking to the server.
     * @throws RuntimeException (and subclasses) if an unexpected error occurs creating the request.
     */
    HttpResponse execute(HttpRequest request) throws IOException;
}
