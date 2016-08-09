package org.apache.brooklyn.util.http.executor;

import com.google.common.annotations.Beta;

@Beta
public class Credentials {

    private Credentials() {}
    
    public static BasicAuth basic(String username, String password) {
        return new BasicAuth(username, password);
    }
    
    public static class BasicAuth {
        private final String username;
        private final String password;

        protected BasicAuth(String username, String password) {
            this.username = username;
            this.password = password;
        }
        
        public String username() {
            return username;
        }
        
        public String password() {
            return password;
        }
    }
}
