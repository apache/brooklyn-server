/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.util.http.executor;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;

import org.apache.brooklyn.util.stream.Streams;

import com.google.common.annotations.Beta;
import com.google.common.collect.Multimap;

@Beta
public class HttpResponseImpl implements HttpResponse {

    private final int code;
    private final String reasonPhrase;
    private final Multimap<String, String> headers;
    private final long contentLength;
    private final InputStream content;

    protected HttpResponseImpl(HttpResponse.Builder builder) {
        code = builder.code;
        reasonPhrase = builder.reasonPhrase;
        headers = checkNotNull(builder.headers, "headers");
        contentLength = builder.contentLength;
        content = builder.content;
    }
    
    @Override
    public void close() throws IOException {
        if (content != null) {
            Streams.closeQuietly(content);
        }
    }

    @Override
    public int code() {
        return code;
    }

    @Override
    public String reasonPhrase() {
        return reasonPhrase;
    }

    @Override
    public Multimap<String, String> headers() {
        return headers;
    }

    @Override
    public long getContentLength() {
        return contentLength;
    }

    @Override
    public InputStream getContent() {
        return content;
    }
}
