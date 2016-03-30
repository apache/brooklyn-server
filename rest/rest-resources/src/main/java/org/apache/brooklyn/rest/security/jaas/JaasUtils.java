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
package org.apache.brooklyn.rest.security.jaas;

import java.net.URL;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaasUtils {
    private static final Logger log = LoggerFactory.getLogger(JaasUtils.class);

    private static final String JAAS_CONFIG = "java.security.auth.login.config";

    public static void init(ManagementContext mgmt) {
        ManagementContextHolder.setManagementContextStatic(mgmt);
        String config = System.getProperty(JAAS_CONFIG);
        if (config == null) {
            URL configUrl = JaasUtils.class.getResource("/jaas.conf");
            if (configUrl != null) {
                log.debug("Using classpath JAAS config from " + configUrl.toExternalForm());
                System.setProperty(JAAS_CONFIG, configUrl.toExternalForm());
            } else {
                log.error("Can't find " + JAAS_CONFIG + " on classpath. Web server authentication will fail.");
            }
        } else {
            log.debug("Using externally configured JAAS at " + config);
        }
    }

}
