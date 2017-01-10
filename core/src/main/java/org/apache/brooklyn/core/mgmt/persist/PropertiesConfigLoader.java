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
package org.apache.brooklyn.core.mgmt.persist;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.stream.Streams;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesConfigLoader implements ConfigLoader {

    private static final Logger LOG = LoggerFactory.getLogger(PropertiesConfigLoader.class);
    private String propertiesPath;

    protected PropertiesConfigLoader(String propertiesPath){
        this.propertiesPath = propertiesPath;
    }

    @Override
    public Map<String, String> load() {
        try {
            InputStream resource = new ResourceUtils(PropertiesConfigLoader.class).getResourceFromUrl(propertiesPath);

            try {
                Properties props = new Properties();
                props.load(resource);

                Map<String, String> result = Maps.newLinkedHashMap();
                for (Enumeration<?> iter = props.propertyNames(); iter.hasMoreElements(); ) {
                    String key = (String) iter.nextElement();
                    String value = props.getProperty(key);
                    result.put(key, value);
                }
                return result;
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            } finally {
                Streams.closeQuietly(resource);
            }
        } catch (Exception e) {
            LOG.warn("Failed to load properties file from " + propertiesPath + " (continuing)", e);
            return ImmutableMap.<String, String>of();
        }
    }

}
