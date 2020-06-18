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

import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.entity.software.base.SoftwareProcessImpl;
import org.apache.brooklyn.feed.function.FunctionFeed;
import org.apache.brooklyn.feed.function.FunctionPollConfig;
import org.apache.brooklyn.util.time.Duration;

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

        addHelmFeed("status", STATUS);
        addKubernetesFeed();
    }

    private void addKubernetesFeed() {
        HelmDriver driver = getDriver();
        Callable status = driver.getKubeCallable();
        FunctionPollConfig pollConfig = new FunctionPollConfig<String, Boolean>(DEPLOYMENT_READY)
                .callable(status)
                ;

        addFeed(FunctionFeed.builder()
                .entity(this)
                .poll(pollConfig)
                .period(Duration.FIVE_SECONDS)
                .build());
    }

    private void addHelmFeed(String command, AttributeSensor<String> sensor) {
        HelmDriver driver = getDriver();
        Callable status = driver.getCallable(command);
        FunctionPollConfig pollConfig = new FunctionPollConfig<String, String>(sensor)
                .callable(status)
                ;

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
