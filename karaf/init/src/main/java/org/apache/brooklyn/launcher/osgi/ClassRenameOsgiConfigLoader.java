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

import org.apache.brooklyn.core.mgmt.persist.DeserializingClassRenamesProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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
public class ClassRenameOsgiConfigLoader extends OsgiConfigLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ClassRenameOsgiConfigLoader.class);
    private static final String KARAF_DESERIALIZING_CLASS_RENAMES_PROPERTIES = "org.apache.brooklyn.classrename";

    public ClassRenameOsgiConfigLoader(){
        super(KARAF_DESERIALIZING_CLASS_RENAMES_PROPERTIES);
    }

    // Called by OSGi
    @Override
    public void init() {
        LOG.trace("DeserializingClassRenamesProvider.OsgiConfigLoader.init: registering loader");
        DeserializingClassRenamesProvider.INSTANCE.getLoaders().add(this);
        DeserializingClassRenamesProvider.INSTANCE.reset();
    }

    // Called by OSGi
    @Override
    public void destroy() {
        LOG.trace("DeserializingClassRenamesProvider.OsgiConfigLoader.destroy: unregistering loader");
        boolean removed = DeserializingClassRenamesProvider.INSTANCE.getLoaders().remove(this);
        if (removed) {
            DeserializingClassRenamesProvider.INSTANCE.reset();
        }
    }

    // Called by OSGi when configuration changes
    @Override
    public void updateProperties(Map properties) {
        LOG.debug("DeserializingClassRenamesProvider.OsgiConfigLoader.updateProperties: clearing cache, so class-renames will be reloaded");
        DeserializingClassRenamesProvider.INSTANCE.reset();
    }
}
