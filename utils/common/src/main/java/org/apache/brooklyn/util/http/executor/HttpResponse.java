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

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.annotations.Beta;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * An HTTP response. Instances of this class are not immutable: the response body is a 
 * one-shot value that may be consumed only once and then closed. All other properties 
 * are immutable.
 * 
 * This class implements {@link Closeable}. Closing it simply closes its response streams (if any).
 */
@Beta
public interface HttpResponse extends Closeable {
    
    @Beta
    public static class Builder {
        protected int code;
        protected String reasonPhrase;
        protected Multimap<String, String> headers = ArrayListMultimap.<String, String>create();
        protected long contentLength = -1;
        protected InputStream content;

        public Builder code(int val) {
            code = val;
            return this;
        }
        
        public Builder reasonPhrase(@Nullable String val) {
            reasonPhrase = val;
            return this;
        }

        public Builder headers(Multimap<String, String> val) {
            headers.putAll(val);
            return this;
        }

        public Builder headers(Map<String, String> val) {
            if (checkNotNull(val, "headers").keySet().contains(null)) {
                throw new NullPointerException("Headers must not contain null key");
            }
            for (Map.Entry<String, String> entry : val.entrySet()) {
                header(entry.getKey(), entry.getValue());
            }
            return this;
        }

        public Builder header(String key, String val) {
            headers.put(key, val);
            return this;
        }

        public Builder content(@Nullable InputStream val) {
            if (val != null) {
                contentLength = -1;
                content = val;
            }
            return this;
        }
        
        public Builder content(@Nullable byte[] val) {
            if (val != null) {
                contentLength = val.length;
                content = new ByteArrayInputStream(val);
            }
            return this;
        }
        
        public HttpResponse build() {
            return new HttpResponseImpl(this);
        }
    }

    /**
     * The HTTP status code.
     */
    public int code();

    /**
     * The HTTP reason phrase.
     */
    @Nullable
    public String reasonPhrase();

    public Multimap<String, String> headers();
    
    /**
     * The length of the content, if known.
     *
     * @return  the number of bytes of the content, or a negative number if unknown. If the 
     *          content length is known but exceeds {@link java.lang.Long#MAX_VALUE Long.MAX_VALUE},
     *          a negative number is returned.
     */
    public long getContentLength();

    /**
     * The response body, or null if no body.
     * @return
     */
    public InputStream getContent();
}