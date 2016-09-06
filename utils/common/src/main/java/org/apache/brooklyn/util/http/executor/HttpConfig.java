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

import com.google.common.annotations.Beta;

@Beta
public class HttpConfig {

    @Beta
    public static class Builder {
        private boolean laxRedirect;
        private boolean trustAll;
        private boolean trustSelfSigned;
        
        public Builder laxRedirect(boolean val) {
            laxRedirect = val;
            return this;
        }
        
        public Builder trustAll(boolean val) {
            trustAll = val;
            return this;
        }
        
        public Builder trustSelfSigned(boolean val) {
            trustSelfSigned = val;
            return this;
        }
        
        public HttpConfig build() {
            return new HttpConfig(this);
        }
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    private final boolean laxRedirect;
    private final boolean trustAll;
    private final boolean trustSelfSigned;

    protected HttpConfig(Builder builder) {
        laxRedirect = builder.laxRedirect;
        trustAll = builder.trustAll;
        trustSelfSigned = builder.trustSelfSigned;
    }
    
    public boolean laxRedirect() {
        return laxRedirect;
    }
    
    public boolean trustAll() {
        return trustAll;
    }
    
    public boolean trustSelfSigned() {
        return trustSelfSigned;
    }
}
