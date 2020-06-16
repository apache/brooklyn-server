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
package org.apache.brooklyn.container.entity.helm;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.entity.brooklynnode.BrooklynClusterImpl;
import org.apache.brooklyn.entity.brooklynnode.BrooklynNode;
import org.apache.brooklyn.entity.software.base.SoftwareProcessDriver;
import org.apache.brooklyn.entity.software.base.SoftwareProcessImpl;
import org.apache.brooklyn.feed.function.FunctionFeed;
import org.apache.brooklyn.feed.function.FunctionPollConfig;
import org.apache.brooklyn.util.time.Duration;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;

public class HelmEntityImpl extends SoftwareProcessImpl implements HelmEntity {
    @Override
    public Class getDriverInterface() {
        return HelmDriver.class;
    }

    @Override
    public void init() {
        super.init();
    }

    @Override
    protected void connectSensors() {
        super.connectSensors();
        connectServiceUpIsRunning();

        HelmDriver driver = getDriver();
        Callable status = driver.getCallable("status");
        FunctionPollConfig pollConfig = new FunctionPollConfig<Object, String>(STATUS)
                .callable(status);

        addFeed(FunctionFeed.builder()
                .entity(this)
                .poll(pollConfig)
                .period(Duration.FIVE_SECONDS)
                .build());
    }

    @Override
    protected void disconnectSensors() {
        super.disconnectSensors();
        disconnectServiceUpIsRunning();
    }

    @Override
    public HelmDriver getDriver() {
        return (HelmDriver) super.getDriver();
    }
}
