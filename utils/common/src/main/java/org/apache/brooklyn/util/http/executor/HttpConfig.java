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
