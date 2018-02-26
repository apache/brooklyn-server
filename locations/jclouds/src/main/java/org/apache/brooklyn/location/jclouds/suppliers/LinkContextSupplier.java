/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.location.jclouds.suppliers;

import com.google.common.base.MoreObjects;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.location.jclouds.JCloudsPropertiesBuilder;
import org.apache.brooklyn.location.jclouds.JcloudsLocationConfig;
import org.apache.brooklyn.location.jclouds.domain.JcloudsContext;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.jclouds.Context;
import org.jclouds.ContextBuilder;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.sshj.config.SshjSshClientModule;

import java.util.Properties;

public class LinkContextSupplier implements Supplier<Context> {

    private final ConfigBag conf;

    public LinkContextSupplier(ConfigBag conf) {
        this.conf = conf;
    }

    @Override
    public Context get() {
        JcloudsContext jcloudsContext = conf.get(JcloudsLocationConfig.LINK_CONTEXT);
        String identity = MoreObjects.firstNonNull(jcloudsContext.getIdentity(), conf.get(CloudLocationConfig.ACCESS_IDENTITY));
        String credential = MoreObjects.firstNonNull(jcloudsContext.getCredential(), conf.get(CloudLocationConfig.ACCESS_CREDENTIAL));
        return ContextBuilder
                .newBuilder(jcloudsContext.getProviderOrApi())
                .credentials(identity, credential)
                .modules(ImmutableList.of(new SshjSshClientModule(), new SLF4JLoggingModule()))
                .overrides(getProperties(jcloudsContext, conf))
                .build();
    }

    private Properties getProperties(JcloudsContext jcloudsContext, ConfigBag conf) {
        if (jcloudsContext.getProperties() != null && jcloudsContext.getProperties().isEmpty()) {
            Properties properties = new Properties();
            properties.putAll(jcloudsContext.getProperties());
            return properties;
        } else {
            return new JCloudsPropertiesBuilder(conf)
                    .setCommonJcloudsProperties()
                    .setCustomJcloudsProperties()
                    .setEndpointProperty()
                    .build();
        }
    }
}
