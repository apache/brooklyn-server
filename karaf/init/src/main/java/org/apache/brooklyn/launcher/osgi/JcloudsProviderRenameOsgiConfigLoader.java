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
package org.apache.brooklyn.launcher.osgi;

import org.apache.brooklyn.core.mgmt.persist.DeserializingJcloudsRenamesProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JcloudsProviderRenameOsgiConfigLoader extends OsgiConfigLoader {

    private static final Logger LOG = LoggerFactory.getLogger(JcloudsProviderRenameOsgiConfigLoader.class);
    private static final String JCLOUDS_PROVIDER_RENAMES_PROPERTIES = "org.apache.brooklyn.jcloudsrename";

    public JcloudsProviderRenameOsgiConfigLoader(){
        super(JCLOUDS_PROVIDER_RENAMES_PROPERTIES);
    }

    // Called by OSGi
    @Override
    public void init() {
        LOG.trace("DeserializingJcloudsRenamesProvider.OsgiConfigLoader.init: registering loader");
        DeserializingJcloudsRenamesProvider.INSTANCE.getLoaders().add(this);
        DeserializingJcloudsRenamesProvider.INSTANCE.reset();
    }

    // Called by OSGi
    @Override
    public void destroy() {
        LOG.trace("DeserializingJcloudsRenamesProvider.OsgiConfigLoader.destroy: unregistering loader");
        boolean removed = DeserializingJcloudsRenamesProvider.INSTANCE.getLoaders().remove(this);
        if (removed) {
            DeserializingJcloudsRenamesProvider.INSTANCE.reset();
        }
    }

    // Called by OSGi when configuration changes
    @Override
    public void updateProperties(Map properties) {
        LOG.debug("DeserializingJcloudsRenamesProvider.OsgiConfigLoader.updateProperties: clearing cache, so jclouds renames will be reloaded");
        DeserializingJcloudsRenamesProvider.INSTANCE.reset();
    }
}
