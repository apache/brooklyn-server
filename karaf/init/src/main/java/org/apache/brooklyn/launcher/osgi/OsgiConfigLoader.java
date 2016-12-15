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

import java.io.IOException;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.core.mgmt.persist.ConfigLoader;
import org.apache.brooklyn.core.mgmt.persist.DeserializingClassRenamesProvider;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.osgi.framework.Constants;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Loads the class-renames from the OSGi configuration file: {@code org.apache.brooklyn.classrename.cfg}.
 *
 * Only public for OSGi instantiation - treat as an internal class, which may change in
 * future releases.
 *
 * See http://stackoverflow.com/questions/18844987/creating-a-blueprint-bean-from-an-inner-class:
 * we unfortunately need to include {@code !org.apache.brooklyn.core.mgmt.persist.DeserializingClassRenamesProvider}
 * in the Import-Package, as the mvn plugin gets confused due to the use of this inner class
 * within the blueprint.xml.
 *
 * @see {@link #KARAF_DESERIALIZING_CLASS_RENAMES_PROPERTIES}
 */
public class OsgiConfigLoader implements ConfigLoader {
    private static final Logger LOG = LoggerFactory.getLogger(OsgiConfigLoader.class);
    private static final List<String> EXCLUDED_KEYS = ImmutableList.of("service.pid", "felix.fileinstall.filename");
    private static final String KARAF_DESERIALIZING_CLASS_RENAMES_PROPERTIES = "org.apache.brooklyn.classrename";

    private ConfigurationAdmin configAdmin;

    public OsgiConfigLoader() {
        LOG.trace("OsgiConfigLoader instance created");
    }

    // For injection as OSGi bean
    public void setConfigAdmin(ConfigurationAdmin configAdmin) {
        this.configAdmin = configAdmin;
    }

    // Called by OSGi
    public void init() {
        LOG.trace("DeserializingClassRenamesProvider.OsgiConfigLoader.init: registering loader");
        DeserializingClassRenamesProvider.getLoaders().add(this);
        DeserializingClassRenamesProvider.reset();
    }

    // Called by OSGi
    public void destroy() {
        LOG.trace("DeserializingClassRenamesProvider.OsgiConfigLoader.destroy: unregistering loader");
        boolean removed = DeserializingClassRenamesProvider.getLoaders().remove(this);
        if (removed) {
            DeserializingClassRenamesProvider.reset();
        }
    }

    // Called by OSGi when configuration changes
    public void updateProperties(Map properties) {
        LOG.debug("DeserializingClassRenamesProvider.OsgiConfigLoader.updateProperties: clearing cache, so class-renames will be reloaded");
        DeserializingClassRenamesProvider.reset();
    }

    @Override
    public Map<String, String> load() {
        if (configAdmin == null) {
            LOG.warn("No OSGi configuration-admin available - cannot load {}.cfg", KARAF_DESERIALIZING_CLASS_RENAMES_PROPERTIES);
            return ImmutableMap.of();
        }

        String filter = '(' + Constants.SERVICE_PID + '=' + KARAF_DESERIALIZING_CLASS_RENAMES_PROPERTIES + ')';
        Configuration[] configs;

        try {
            configs = configAdmin.listConfigurations(filter);
        } catch (InvalidSyntaxException | IOException e) {
            LOG.info("Cannot list OSGi configurations");
            throw Exceptions.propagate(e);
        }

        final MutableMap<String, String> map = MutableMap.of();
        if (configs != null) {
            for (Configuration config : configs) {
                LOG.debug("Reading OSGi configuration from {}; bundleLocation={}", config.getPid(), config.getBundleLocation());
                map.putAll(dictToMap(config.getProperties()));
            }
        } else {
            LOG.info("No OSGi configuration found for {}.cfg", KARAF_DESERIALIZING_CLASS_RENAMES_PROPERTIES);
        }

        return map;
    }

    private Map<String, String> dictToMap(Dictionary<String, Object> props) {
        Map<String, String> mapProps = MutableMap.of();
        Enumeration<String> keyEnum = props.keys();
        while (keyEnum.hasMoreElements()) {
            String key = keyEnum.nextElement();
            if (!EXCLUDED_KEYS.contains(key)) {
                mapProps.put(key, (String) props.get(key));
            }
        }
        return mapProps;
    }
}
