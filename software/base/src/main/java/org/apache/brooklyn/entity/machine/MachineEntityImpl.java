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
package org.apache.brooklyn.entity.machine;

import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.entity.software.base.AbstractSoftwareProcessSshDriver;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcessDriver;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcessImpl;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.time.Duration;

public class MachineEntityImpl extends EmptySoftwareProcessImpl implements MachineEntity {

    private static final Logger LOG = LoggerFactory.getLogger(MachineEntityImpl.class);

    private transient Feed machineMetrics;

    @Override
    protected void initEnrichers() {
        LOG.info("Adding machine metrics enrichers");
        AddMachineMetrics.addMachineMetricsEnrichers(this);

        super.initEnrichers();
    }

    @Override
    protected void connectSensors() {
        super.connectSensors();

        Maybe<SshMachineLocation> location = Machines.findUniqueMachineLocation(getLocations(), SshMachineLocation.class);
        if (location.isPresent() && location.get().getOsDetails().isLinux()) {
            LOG.info("Adding machine metrics feed");
            machineMetrics = AddMachineMetrics.createMachineMetricsFeed(this);
        } else {
            LOG.warn("Not adding machine metrics feed as no suitable location available on entity");
        }
    }

    @Override
    protected void disconnectSensors() {
        if (machineMetrics != null) machineMetrics.stop();

        super.disconnectSensors();
    }

    @Override
    public Class<?> getDriverInterface() {
        return EmptySoftwareProcessDriver.class;
    }

    public SshMachineLocation getMachine() {
        return Machines.findUniqueMachineLocation(getLocations(), SshMachineLocation.class).get();
    }

    @Override
    public String execCommand(String command) {
        return execCommandTimeout(command, Duration.ONE_MINUTE);
    }

    @Override
    public String execCommandTimeout(String command, Duration timeout) {
        AbstractSoftwareProcessSshDriver driver = (AbstractSoftwareProcessSshDriver) getDriver();
        if (driver == null) {
            throw new NullPointerException("No driver for "+this);
        }
        ProcessTaskWrapper<String> task = SshEffectorTasks.ssh(command)
                .environmentVariables(driver.getShellEnvironment())
                .requiringZeroAndReturningStdout()
                .machine(getMachine())
                .summary(command)
                .newTask();

        try {
            String result = DynamicTasks.queueIfPossible(task)
                    .executionContext(this)
                    .orSubmitAsync()
                    .asTask()
                    .get(timeout);
            return result;
        } catch (TimeoutException te) {
            throw new IllegalStateException("Timed out running command: " + command);
        } catch (Exception e) {
            Integer exitCode = task.getExitCode();
            LOG.warn("Command failed, return code {}: {}", exitCode == null ? -1 : exitCode, task.getStderr());
            throw Exceptions.propagate(e);
        }
    }

}
