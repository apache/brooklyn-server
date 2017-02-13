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
package org.apache.brooklyn.core.config.external;

import java.io.IOException;
import java.util.Map;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.util.core.ResourceUtils;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;


/**
 * This is configured with references to URLs or files. When calling {@link #get(String)} it 
 * retrieves the String contents of that file. See {@link ResourceUtils#getResourceAsString(String)}
 * for details of the type of URL/file references supported.
 * 
 * Example configuration could be:
 * <pre>
 * brooklyn.external.foo = brooklyn.management.config.external.FilesExternalConfigSupplier
 * brooklyn.external.foo.authorized_keys = classpath://authorized_keys
 * brooklyn.external.foo.privateSshKey = /path/to/privateKey
 * brooklyn.external.foo.initScript = https://brooklyn.example.com/config/initScript.sh
 * </pre>
 */
public class UrlsExternalConfigSupplier extends AbstractExternalConfigSupplier {

    // TODO Should we cache (for some short period of time)?

    private final Map<String, String> config;
    private final ResourceUtils resourceUtils;
    
    public UrlsExternalConfigSupplier(ManagementContext managementContext, String name, Map<String, String> config) throws IOException {
        super(managementContext, name);
        this.config = config;
        resourceUtils = ResourceUtils.create(
                managementContext.getCatalogClassLoader(), 
                this, 
                UrlsExternalConfigSupplier.class.getSimpleName()+"("+getName()+")");

        Map<String, String> missing = Maps.newLinkedHashMap();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String target = entry.getValue();
            if (!resourceUtils.doesUrlExist(target)) {
                missing.put(entry.getKey(), entry.getValue());
            }
        }
        if (missing.size() > 0) {
            throw new IllegalStateException("URLs for external config '"+getName()+"' not found: "+missing);
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("name", getName()).toString();
    }
    
    @Override
    public String get(String key) {
        String target = config.get(key);
        if (target == null) {
            throw new IllegalArgumentException("Unknown key '"+key+"' for "+toString());
        }
        return resourceUtils.getResourceAsString(target);
    }
}
