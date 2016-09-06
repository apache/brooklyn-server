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
package org.apache.brooklyn.util.executor;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.http.executor.HttpExecutor;
import org.apache.brooklyn.util.http.executor.apacheclient.HttpExecutorImpl;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class HttpExecutorFactoryImpl implements HttpExecutorFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HttpExecutorFactoryImpl.class);

    public HttpExecutorFactoryImpl() {
        // no-op
    }

    @Override
    public HttpExecutor getHttpExecutor(Map<?, ?> props) {
        HttpExecutor httpExecutor;

        String httpExecutorClass = (String) props.get(HTTP_EXECUTOR_CLASS_CONFIG);
        if (httpExecutorClass != null) {
            Map<String,Object> httpExecutorProps = MutableMap.of();
            Map<?, ?> executorProps = Maps.filterKeys(props, StringPredicates.isStringStartingWith(HTTP_EXECUTOR_CLASS_CONFIG_PREFIX));
            if (executorProps.size() > 0) {
                for (Entry<?, ?> entry: executorProps.entrySet()) {
                    String keyName = Strings.removeFromStart((String)entry.getKey(), HTTP_EXECUTOR_CLASS_CONFIG_PREFIX);
                    httpExecutorProps.put(keyName, entry.getValue());
                }
            }
            try {
                httpExecutor = (HttpExecutor) new ClassLoaderUtils(getClass()).loadClass(httpExecutorClass).getConstructor(Map.class).newInstance(httpExecutorProps);
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }

        } else {
            LOG.info(HTTP_EXECUTOR_CLASS_CONFIG + " parameter not provided. Using the default implementation " + HttpExecutorImpl.class.getName());
            httpExecutor = HttpExecutorImpl.newInstance();
        }

        return httpExecutor;
    }
}


