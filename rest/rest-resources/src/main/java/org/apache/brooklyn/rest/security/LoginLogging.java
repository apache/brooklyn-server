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
package org.apache.brooklyn.rest.security;

import org.apache.brooklyn.util.collections.MutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpSession;
import java.util.Map;
import java.util.stream.Collectors;

public class LoginLogging {

    private static final Logger log = LoggerFactory.getLogger(LoginLogging.class);

    static final String LOGIN_LOGGED = "brooklyn.login_logged";

    public static void logLoginIfNotLogged(HttpSession session, String user, Map<String,String> values) {
        if (Boolean.TRUE.equals(session.getAttribute(LOGIN_LOGGED))) {
            return;
        }
        session.setAttribute(LOGIN_LOGGED, true);
        log.debug(
                "Login of " +
                (user==null ? "anonymous user" : "user: "+user) +
                        getValuesForLogging(session, values));
    }

    private static String getValuesForLogging(HttpSession session, Map<String, String> values) {
        Map<String, String> v = MutableMap.copyOf(values);
        if (!v.containsKey("session")) v.put("session", session.getId());
        return v.entrySet().stream().filter(k -> k.getValue() != null).map(k -> ", " + k.getKey() + ": " + k.getValue()).collect(Collectors.joining());
    }

    public static void logLogout(HttpSession session, String user, Map<String,String> values) {
        session.setAttribute(LOGIN_LOGGED, false);
        log.debug(
                "Logout of " +
                        (user==null ? "anonymous user" : "user: "+user) +
                        getValuesForLogging(session, values));
    }
}
