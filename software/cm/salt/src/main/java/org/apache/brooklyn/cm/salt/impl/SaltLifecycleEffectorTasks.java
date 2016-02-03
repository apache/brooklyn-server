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
package org.apache.brooklyn.cm.salt.impl;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampConstants;
import org.apache.brooklyn.cm.salt.SaltConfig;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.software.base.SoftwareProcess.StopSoftwareParameters;
import org.apache.brooklyn.entity.software.base.lifecycle.MachineLifecycleEffectorTasks;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.system.ProcessTaskFactory;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.DOTALL;
import static java.util.regex.Pattern.MULTILINE;
import static org.apache.brooklyn.entity.software.base.SoftwareProcess.StopSoftwareParameters.StopMode.ALWAYS;
import static org.apache.brooklyn.entity.software.base.SoftwareProcess.StopSoftwareParameters.StopMode.NEVER;

@Beta
public class SaltLifecycleEffectorTasks extends MachineLifecycleEffectorTasks implements SaltConfig {
    private static final Logger LOG = LoggerFactory.getLogger(SaltLifecycleEffectorTasks.class);

    @Override
    protected String startProcessesAtMachine(Supplier<MachineLocation> machineS) {
        SaltMode mode = detectSaltMode(entity());
        final MachineLocation machine = machineS.get();
        LOG.info("Starting salt in '{}' mode at '{}'", mode, machine.getDisplayName());
        if (mode == SaltMode.MASTERLESS) {
            startWithSshAsync();
        } else {
            // TODO: implement MASTER and MINION
            throw new IllegalStateException("Unknown salt mode: " + mode.name());
        }
        return "salt tasks submitted (" + mode + ")";
    }


    protected static SaltMode detectSaltMode(Entity entity) {
        SaltMode mode = entity.getConfig(SaltConfig.SALT_MODE);
        Preconditions.checkNotNull(mode, "Required config " + SaltConfig.SALT_MODE + " not provided for entity: " + entity);
        return mode;
    }

    protected void startWithSshAsync() {

        final Set<? extends String> startStates = entity().getConfig(SaltConfig.START_STATES);
        final Set<? extends String> formulas = entity().getConfig(SaltConfig.SALT_FORMULAS);
        final Set<? extends String> pillars = entity().getConfig(SaltConfig.SALT_PILLARS);
        final Set<? extends String> pillarUrls = entity().getConfig(SaltConfig.SALT_PILLAR_URLS);
        final String entityId = entity().getConfig(BrooklynCampConstants.PLAN_ID);

        final ProcessTaskWrapper<Integer> installedAlready = queueAndBlock(SaltSshTasks.isSaltInstalled(false));

        if (0 != installedAlready.getExitCode()) {
            DynamicTasks.queue("install", new Runnable() {
                public void run() {
                    DynamicTasks.queue(
                        SaltSshTasks.installSalt(false),
                        SaltSshTasks.installSaltUtilities(false),
                        SaltSshTasks.configureForMasterlessOperation(false),
                        SaltSshTasks.installTopFile(startStates, false));

                    if (Strings.isNonBlank(entityId)) {
                        DynamicTasks.queue(SaltSshTasks.setMinionId(entityId));
                    }
                    installFormulas(formulas);
                    installPillars(pillars, pillarUrls);
                }
            });
        }

        startSalt();

        connectSensors();
    }


    private static final Pattern FAILURES = Pattern.compile(".*^Failed:\\s+(\\d+)$.*", MULTILINE | DOTALL);
    private static final String ZERO = "0";

    private void startSalt() {
        String name = "apply top states";
        final ProcessTaskWrapper<Integer> topStates = queueAndBlock(SaltSshTasks.applyTopStates(false).summary(name));

        // Salt apply returns exit code 0 even upon failure so check the stdout.
        final Matcher failCount = FAILURES.matcher(topStates.getStdout());
        if (!failCount.matches() || !ZERO.equals(failCount.group(1))) {
            LOG.warn("Encountered error in applying Salt top states: {}", topStates.getStdout());
            throw new RuntimeException(
                "Encountered error in applying Salt top states, see '" + name + "' in activities for details");
        }
    }

    private void installFormulas(Set<? extends String> formulas) {
        if (formulas.size() > 0) {
            DynamicTasks.queue(SaltSshTasks.enableFileRoots(false));

            final TaskBuilder<Object> formulaTasks = TaskBuilder.builder().displayName("install formulas");
            for (String url : formulas) {
                formulaTasks.add(SaltSshTasks.installSaltFormula(url, false).newTask());
            }
            DynamicTasks.queue(formulaTasks.build());
        }
    }

    private void installPillars(Set<? extends String> pillars, Set<? extends String> pillarUrls) {
        if (pillarUrls.size() > 0) {
            final TaskBuilder<Object> pillarTasks = TaskBuilder.builder().displayName("install pillars");
            pillarTasks.add(SaltSshTasks.invokeSaltUtility("init_pillar_config", null, false)
                .summary("init pillar config").newTask());
            for (String pillar : pillars) {
                pillarTasks.add(SaltSshTasks.addPillarToTop(pillar, false).newTask());
            }
            for (String url : pillarUrls) {
                pillarTasks.add(SaltSshTasks.installSaltPillar(url, false).newTask());
            }
            DynamicTasks.queue(pillarTasks.build());
        }
    }

    private void connectSensors() {
        final ProcessTaskWrapper<String> retrieveHighstate = SaltSshTasks.retrieveHighstate();
        final ProcessTaskWrapper<String> highstate = DynamicTasks.queue(retrieveHighstate).block();
        String stateDescription = highstate.get();

        SaltHighstate.applyHighstate(stateDescription, entity());
    }


    protected void postStartCustom() {
        // TODO: check for package installed?
        entity().sensors().set(SoftwareProcess.SERVICE_UP, true);
    }


    @Override
    protected String stopProcessesAtMachine() {
        final Set<? extends String> stopStates = entity().getConfig(SaltConfig.STOP_STATES);
        LOG.debug("Executing Salt stopProcessesAtMachine with states {}", stopStates);
        if (stopStates.isEmpty()) {
            stopBasedOnStartStates();
        } else {
            applyStates(stopStates);
        }
        return null;
    }

    private void applyStates(Set<? extends String> states) {
        for (String state : states) {
            DynamicTasks.queue(SaltSshTasks.applyState(state, false).summary("apply state " + state));
        }
    }

    private void stopBasedOnStartStates() {
        final Set<? extends String> startStates = entity().getConfig(SaltConfig.START_STATES);
        final MutableSet<String> stopStates = addSuffix(startStates, ".stop");
        final ProcessTaskWrapper<Integer> checkStops =
            queueAndBlock(SaltSshTasks.verifyStates(stopStates, false).summary("check stop states"));
        if (0 != checkStops.getExitCode()) {
            throw new RuntimeException("No stop_states configured and not all start_states have matching stop states");
        } else {
            applyStates(stopStates);
        }
    }

    public void restart(ConfigBag parameters) {
        ServiceStateLogic.setExpectedState(entity(), Lifecycle.STOPPING);

        try {
            final Set<? extends String> restartStates = entity().getConfig(SaltConfig.RESTART_STATES);
            LOG.debug("Executing Salt restart with states {}", restartStates);
            if (restartStates.isEmpty()) {
                restartBasedOnStartStates();
            } else {
                applyStates(restartStates);
            }
            ServiceStateLogic.setExpectedState(entity(), Lifecycle.RUNNING);
        } catch (Exception e) {
            entity().sensors().set(ServiceStateLogic.SERVICE_NOT_UP_DIAGNOSTICS,
                ImmutableMap.<String, Object>of("restart", e.getMessage()));
            ServiceStateLogic.setExpectedState(entity(), Lifecycle.ON_FIRE);
        }
    }

    private void restartBasedOnStartStates() {
        final Set<? extends String> startStates = entity().getConfig(SaltConfig.START_STATES);
        final MutableSet<String> restartStates = addSuffix(startStates, ".restart");
        final ProcessTaskWrapper<Integer> queued =
            queueAndBlock(SaltSshTasks.findStates(restartStates, false).summary("check restart states"));
        final String stdout = queued.getStdout();
        String[] foundStates = Strings.isNonBlank(stdout) ? stdout.split("\\n") : null;

        if (restartStates.size() > 0 && foundStates != null && (restartStates.size() == foundStates.length)) {
            // each state X listed in start_states has a matching state of the form X.restart;  we apply them.
            LOG.debug("All start_states have matching restart states, applying these");
            applyStates(restartStates);

        } else if (foundStates != null && foundStates.length > 0) {
            // only *some* of the states have a matching restart; we treat this as a fail
            LOG.debug("Only some start_states have matching restart states, treating as restart failure") ;
            throw new RuntimeException("unable to find restart state for all applied states");

        } else {
            // else we apply "stop" effector (with parameters to stop processes not machine) then "start"
            // (and in that effector we'd fail if stop was not well-defined)
            LOG.debug("No stop states available, invoking stop and start effectors");
            invokeEffector(Startable.STOP, ConfigBag.newInstance()
                .configure(StopSoftwareParameters.STOP_PROCESS_MODE, ALWAYS)
                .configure(StopSoftwareParameters.STOP_MACHINE_MODE, NEVER));
            invokeEffector(Startable.START, ConfigBag.EMPTY);
        }
    }

    private ProcessTaskWrapper<Integer> queueAndBlock(ProcessTaskFactory<Integer> taskFactory) {
        final ProcessTaskWrapper<Integer> queued = DynamicTasks.queue(taskFactory);
        queued.asTask().blockUntilEnded();
        return queued;
    }

    private void invokeEffector(Effector<Void> effector, ConfigBag config) {
        final TaskAdaptable<Void> stop = Entities.submit(entity(), Effectors.invocation(entity(), effector, config));
        stop.asTask().blockUntilEnded();
    }

    private MutableSet<String> addSuffix(Set<? extends String> names, String suffix) {
        final MutableSet<String> suffixed = MutableSet.of();
        for (String name : names) {
            suffixed.add(name + suffix);
        }
        return suffixed;
    }
}
