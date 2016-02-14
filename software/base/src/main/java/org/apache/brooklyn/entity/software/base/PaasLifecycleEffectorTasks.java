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
package org.apache.brooklyn.entity.software.base;


import com.google.common.annotations.Beta;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.entity.software.base.lifecycle.AbstractLifecycleEffectorTasks;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;


@Beta
public class PaasLifecycleEffectorTasks extends AbstractLifecycleEffectorTasks {

    private static final Logger log = LoggerFactory.getLogger(PaasLifecycleEffectorTasks.class);

    @Override
    protected SoftwareProcessImpl entity() {
        return (SoftwareProcessImpl) super.entity();
    }

    @Override
    public void start(Collection<? extends Location> locations) {
        //TODO
    }

    /**
     * Default restart implementation for an entity.
     * <p>
     * Stops processes if possible, then starts the entity again.
     */
    @Override
    public void restart(ConfigBag parameters) {
        //TODO
    }
    
    @Override
    public void stop(ConfigBag parameters) {
        //TODO
    }

    @Override
    public void suspend(ConfigBag parameters) {
        //TODO
    }


}

