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
package org.apache.brooklyn.entity.software.base.lifecycle;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineManagementMixins;
import org.apache.brooklyn.api.location.MachineManagementMixins.SuspendsMachines;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.internal.AttributesInternal;
import org.apache.brooklyn.core.entity.internal.AttributesInternal.ProvisioningTaskState;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle.Transition;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.entity.trait.StartableMethods;
import org.apache.brooklyn.core.feed.ConfigToAttributes;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.sensor.BasicAttributeSensor;
import org.apache.brooklyn.core.sensor.ReleaseableLatch;
import org.apache.brooklyn.entity.machine.MachineInitTasks;
import org.apache.brooklyn.entity.machine.ProvidesProvisioningFlags;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.software.base.SoftwareProcess.RestartSoftwareParameters;
import org.apache.brooklyn.entity.software.base.SoftwareProcess.RestartSoftwareParameters.RestartMachineMode;
import org.apache.brooklyn.entity.software.base.SoftwareProcess.StopSoftwareParameters;
import org.apache.brooklyn.entity.software.base.SoftwareProcess.StopSoftwareParameters.StopMode;
import org.apache.brooklyn.entity.stock.EffectorStartableImpl.StartParameters;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.location.ssh.CanResolveOnBoxDir;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ValueResolverIterator;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.net.UserAndHostAndPort;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

/**
 * Default skeleton for start/stop/restart tasks on machines.
 * <p>
 * Knows how to provision machines, making use of {@link ProvidesProvisioningFlags#obtainProvisioningFlags(MachineProvisioningLocation)},
 * and provides hooks for injecting behaviour at common places.
 * <p>
 * Methods are designed for overriding, with the convention that *Async methods should queue (and not block).
 * The following methods are commonly overridden (and you can safely queue tasks, block, or return immediately in them):
 * <ul>
 *  <li> {@link #startProcessesAtMachine(Supplier)} (required)
 *  <li> {@link #stopProcessesAtMachine(ConfigBag)} (required, but can be left blank if you assume the VM will be destroyed)
 *  <li> {@link #preStartCustom(MachineLocation, ConfigBag)}
 *  <li> {@link #postStartCustom(ConfigBag)}
 *  <li> {@link #preStopConfirmCustom(ConfigBag)}
 *  <li> {@link #postStopCustom(ConfigBag)}
 * </ul>
 * Note methods at this level typically look after the {@link Attributes#SERVICE_STATE_ACTUAL} sensor.
 *
 * @since 0.6.0
 */
@Beta
public abstract class MachineLifecycleEffectorTasks {

    private static final Logger log = LoggerFactory.getLogger(MachineLifecycleEffectorTasks.class);

    public static final ConfigKey<Boolean> ON_BOX_BASE_DIR_RESOLVED = ConfigKeys.newBooleanConfigKey(
            "onbox.base.dir.resolved",
            "Whether the on-box base directory has been resolved (for internal use)");

    public static final ConfigKey<Collection<? extends Location>> LOCATIONS = StartParameters.LOCATIONS;
    
    public static final ConfigKey<Duration> STOP_PROCESS_TIMEOUT = ConfigKeys.newDurationConfigKey(
            "process.stop.timeout", "How long to wait for the processes to be stopped; use null to mean forever", 
            Duration.TWO_MINUTES);

    @Beta
    public static final ConfigKey<Duration> STOP_WAIT_PROVISIONING_TIMEOUT = ConfigKeys.newDurationConfigKey(
            "stop.wait.provisioning.timeout",
            "If stop is called on an entity while it is still provisioning the machine (such that "
                    + "the provisioning cannot be safely interrupted), this is the length of time "
                    + "to wait for the machine instance to become available so that it can be terminated. "
                    + "If stop aborts before this point, the machine may be left running.", 
            Duration.minutes(10));

    @Beta
    public static final AttributeSensor<MachineLocation> INTERNAL_PROVISIONED_MACHINE = new BasicAttributeSensor<MachineLocation>(
            TypeToken.of(MachineLocation.class), 
            "internal.provisioning.task.machine",
            "Internal transient sensor (do not use) for tracking the machine being provisioned (to better handle aborting)", 
            AttributeSensor.SensorPersistenceMode.NONE);
    
    protected final MachineInitTasks machineInitTasks = new MachineInitTasks();
    
    /** Attaches lifecycle effectors (start, restart, stop) to the given entity post-creation. */
    public void attachLifecycleEffectors(Entity entity) {
        ((EntityInternal) entity).getMutableEntityType().addEffector(newStartEffector());
        ((EntityInternal) entity).getMutableEntityType().addEffector(newRestartEffector());
        ((EntityInternal) entity).getMutableEntityType().addEffector(newStopEffector());
    }

    /**
     * Return an effector suitable for setting in a {@code public static final} or attaching dynamically.
     * <p>
     * The effector overrides the corresponding effector from {@link Startable} with
     * the behaviour in this lifecycle class instance.
     */
    public Effector<Void> newStartEffector() {
        return Effectors.effector(Startable.START).impl(newStartEffectorTask()).build();
    }

    /** @see {@link #newStartEffector()} */
    public Effector<Void> newRestartEffector() {
        return Effectors.effector(Startable.RESTART)
                .parameter(RestartSoftwareParameters.RESTART_CHILDREN)
                .parameter(RestartSoftwareParameters.RESTART_MACHINE)
                .impl(newRestartEffectorTask())
                .build();
    }
    
    /** @see {@link #newStartEffector()} */
    public Effector<Void> newStopEffector() {
        return Effectors.effector(Startable.STOP)
                .parameter(StopSoftwareParameters.STOP_PROCESS_MODE)
                .parameter(StopSoftwareParameters.STOP_MACHINE_MODE)
                .impl(newStopEffectorTask())
                .build();
    }

    /** @see {@link #newStartEffector()} */
    public Effector<Void> newSuspendEffector() {
        return Effectors.effector(Void.class, "suspend")
                .description("Suspend the process/service represented by an entity")
                .parameter(StopSoftwareParameters.STOP_PROCESS_MODE)
                .parameter(StopSoftwareParameters.STOP_MACHINE_MODE)
                .impl(newSuspendEffectorTask())
                .build();
    }

    /**
     * Returns the {@link EffectorBody} which supplies the implementation for the start effector.
     * <p>
     * Calls {@link #start(Collection)} in this class.
     */
    public EffectorBody<Void> newStartEffectorTask() {
        // TODO included anonymous inner class for backwards compatibility with persisted state.
        new EffectorBody<Void>() {
            @Override
            public Void call(ConfigBag parameters) {
                Collection<? extends Location> locations  = null;

                Object locationsRaw = parameters.getStringKey(LOCATIONS.getName());
                locations = Locations.coerceToCollectionOfLocationsManaged(entity().getManagementContext(), locationsRaw);

                if (locations==null) {
                    // null/empty will mean to inherit from parent
                    locations = Collections.emptyList();
                }

                start(locations);
                return null;
            }
        };
        return new StartEffectorBody();
    }

    protected class StartEffectorBody extends EffectorBody<Void> {
        @Override
        public Void call(ConfigBag parameters) {
            Collection<? extends Location> locations = null;

            Object locationsRaw = parameters.getStringKey(LOCATIONS.getName());
            locations = Locations.coerceToCollectionOfLocationsManaged(entity().getManagementContext(), locationsRaw);

            if (locations == null) {
                // null/empty will mean to inherit from parent
                locations = Collections.emptyList();
            }

            start(locations);
            return null;
        }

    }

    /**
     * Calls {@link #restart(ConfigBag)}.
     *
     * @see {@link #newStartEffectorTask()}
     */
    public EffectorBody<Void> newRestartEffectorTask() {
        // TODO included anonymous inner class for backwards compatibility with persisted state.
        new EffectorBody<Void>() {
            @Override
            public Void call(ConfigBag parameters) {
                restart(parameters);
                return null;
            }
        };
        return new RestartEffectorBody();
    }

    protected class RestartEffectorBody extends EffectorBody<Void> {
        @Override
        public Void call(ConfigBag parameters) {
            restart(parameters);
            return null;
        }
    }

    /**
     * Calls {@link #stop(ConfigBag)}.
     *
     * @see {@link #newStartEffectorTask()}
     */
    public EffectorBody<Void> newStopEffectorTask() {
        // TODO included anonymous inner class for backwards compatibility with persisted state.
        new EffectorBody<Void>() {
            @Override
            public Void call(ConfigBag parameters) {
                stop(parameters);
                return null;
            }
        };
        return new StopEffectorBody();
    }

    protected class StopEffectorBody extends EffectorBody<Void> {
        @Override
        public Void call(ConfigBag parameters) {
            stop(parameters);
            return null;
        }
    }

    /**
     * Calls {@link #suspend(ConfigBag)}.
     *
     * @see {@link #newStartEffectorTask()}
     */
    public EffectorBody<Void> newSuspendEffectorTask() {
        return new SuspendEffectorBody();
    }

    protected class SuspendEffectorBody extends EffectorBody<Void> {
        @Override
        public Void call(ConfigBag parameters) {
            suspend(parameters);
            return null;
        }
    }

    protected EntityInternal entity() {
        return (EntityInternal) BrooklynTaskTags.getTargetOrContextEntity(Tasks.current());
    }

    protected Location getLocation(@Nullable Collection<? extends Location> locations) {
        if (locations==null || locations.isEmpty()) locations = entity().getLocations();
        if (locations.isEmpty()) {
            MachineProvisioningLocation<?> provisioner = entity().getAttribute(SoftwareProcess.PROVISIONING_LOCATION);
            if (provisioner!=null) locations = Arrays.<Location>asList(provisioner);
        }
        locations = Locations.getLocationsCheckingAncestors(locations, entity());

        Maybe<MachineLocation> ml = Locations.findUniqueMachineLocation(locations);
        if (ml.isPresent()) return ml.get();

        if (locations.isEmpty())
            throw new IllegalArgumentException("No locations specified when starting "+entity());
        if (locations.size() != 1 || Iterables.getOnlyElement(locations)==null)
            throw new IllegalArgumentException("Ambiguous locations detected when starting "+entity()+": "+locations);
        return Iterables.getOnlyElement(locations);
    }
    
    /** runs the tasks needed to start, wrapped by setting {@link Attributes#SERVICE_STATE_EXPECTED} appropriately */ 
    public void start(Collection<? extends Location> locations) {
        ServiceStateLogic.setExpectedState(entity(), Lifecycle.STARTING);
        try {
            startInLocations(locations, ConfigBag.newInstance());
            DynamicTasks.waitForLast();
            ServiceStateLogic.setExpectedState(entity(), Lifecycle.RUNNING);
        } catch (Throwable t) {
            ServiceStateLogic.setExpectedState(entity(), Lifecycle.ON_FIRE);
            throw Exceptions.propagate(t);
        }
    }

    /** Asserts there is a single location and calls {@link #startInLocation(Location, ConfigBag)} with that location. */
    protected void startInLocations(Collection<? extends Location> locations, ConfigBag parameters) {
        startInLocation(getLocation(locations), parameters);
    }

    /** Dispatches to the appropriate method(s) to start in the given location. */
    protected void startInLocation(final Location location, ConfigBag parameters) {
        Supplier<MachineLocation> locationS = null;
        if (location instanceof MachineProvisioningLocation) {
            Task<MachineLocation> machineTask = provisionAsync((MachineProvisioningLocation<?>)location);
            locationS = Tasks.supplier(machineTask);
        } else if (location instanceof MachineLocation) {
            locationS = Suppliers.ofInstance((MachineLocation)location);
        }
        Preconditions.checkState(locationS != null, "Unsupported location "+location+", when starting "+entity());

        final Supplier<MachineLocation> locationSF = locationS;

        // Opportunity to block startup until other dependent components are available
        try (CloseableLatch latch = waitForCloseableLatch(entity(), SoftwareProcess.START_LATCH)) {
            preStartAtMachineAsync(locationSF, parameters);
            DynamicTasks.queue("start (processes)", new StartProcessesAtMachineTask(locationSF));
            postStartAtMachineAsync(parameters);
        }
    }

    protected class StartProcessesAtMachineTask implements Runnable {
        private final Supplier<MachineLocation> machineSupplier;
        protected StartProcessesAtMachineTask(Supplier<MachineLocation> machineSupplier) {
            this.machineSupplier = machineSupplier;
        }
        @Override
        public void run() {
            startProcessesAtMachine(machineSupplier);
        }
    }

    /**
     * Returns a queued {@link Task} which provisions a machine in the given location
     * and returns that machine. The task can be used as a supplier to subsequent methods.
     */
    protected Task<MachineLocation> provisionAsync(final MachineProvisioningLocation<?> location) {
        return DynamicTasks.queue(Tasks.<MachineLocation>builder().displayName("provisioning (" + location.getDisplayName() + ")").body(
                new ProvisionMachineTask(location)).build());
    }

    protected class ProvisionMachineTask implements Callable<MachineLocation> {
        final MachineProvisioningLocation<?> location;

        protected ProvisionMachineTask(MachineProvisioningLocation<?> location) {
            this.location = location;
        }

        @Override
        public MachineLocation call() throws Exception {
            // Blocks if a latch was configured.
            entity().getConfig(BrooklynConfigKeys.PROVISION_LATCH);
            final Map<String, Object> flags = obtainProvisioningFlags(location);
            if (!(location instanceof LocalhostMachineProvisioningLocation))
                log.info("Starting {}, obtaining a new location instance in {} with ports {}", new Object[]{entity(), location, flags.get("inboundPorts")});
            entity().sensors().set(SoftwareProcess.PROVISIONING_LOCATION, location);
            Transition expectedState = entity().sensors().get(Attributes.SERVICE_STATE_EXPECTED);
            
            // BROOKLYN-263: see corresponding code in doStop()
            if (expectedState != null && (expectedState.getState() == Lifecycle.STOPPING || expectedState.getState() == Lifecycle.STOPPED)) {
                throw new IllegalStateException("Provisioning aborted before even begun for "+entity()+" in "+location+" (presumably by a concurrent call to stop");
            }
            entity().sensors().set(AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE, ProvisioningTaskState.RUNNING);
            
            MachineLocation machine;
            try {
                machine = Tasks.withBlockingDetails("Provisioning machine in " + location, new ObtainLocationTask(location, flags));
                entity().sensors().set(INTERNAL_PROVISIONED_MACHINE, machine);
            } finally {
                entity().sensors().remove(AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE);
            }
            
            if (machine == null) {
                throw new NoMachinesAvailableException("Failed to obtain machine in " + location.toString());
            }
            if (log.isDebugEnabled()) {
                log.debug("While starting {}, obtained new location instance {}", entity(),
                        (machine instanceof SshMachineLocation
                                ? machine + ", details " + ((SshMachineLocation) machine).getUser() + ":" + Sanitizer.sanitize(((SshMachineLocation) machine).config().getLocalBag())
                                : machine));
            }
            return machine;
        }
    }

    protected static class ObtainLocationTask implements Callable<MachineLocation> {
        final MachineProvisioningLocation<?> location;
        final Map<String, Object> flags;

        protected ObtainLocationTask(MachineProvisioningLocation<?> location, Map<String, Object> flags) {
            this.flags = flags;
            this.location = location;
        }

        @Override
        public MachineLocation call() throws NoMachinesAvailableException {
            return location.obtain(flags);
        }
    }

    /**
     * Wraps a call to {@link #preStartCustom(MachineLocation, ConfigBag)}, after setting the hostname and address.
     */
    protected void preStartAtMachineAsync(final Supplier<MachineLocation> machineS, ConfigBag parameters) {
        DynamicTasks.queue("pre-start", new PreStartTask(machineS.get(), parameters));
    }

    protected class PreStartTask implements Runnable {
        final MachineLocation machine;
        final ConfigBag parameters;

        protected PreStartTask(MachineLocation machine) {
            this(machine, null);
        }
        protected PreStartTask(MachineLocation machine, ConfigBag parameters) {
            this.machine = machine;
            this.parameters = parameters;

        }
        @Override
        public void run() {
            log.info("Starting {} on machine {}", entity(), machine);
            Collection<Location> oldLocs = entity().getLocations();
            if (!oldLocs.isEmpty()) {
                List<MachineLocation> oldSshLocs = ImmutableList.copyOf(Iterables.filter(oldLocs, MachineLocation.class));
                if (!oldSshLocs.isEmpty()) {
                    // check if existing locations are compatible
                    log.debug("Entity " + entity() + " had machine locations " + oldSshLocs + " when starting at " + machine + "; checking if they are compatible");
                    for (MachineLocation oldLoc : oldSshLocs) {
                        // machines are deemed compatible if hostname and address are the same, or they are localhost
                        // this allows a machine create by jclouds to then be defined with an ip-based spec
                        if (!"localhost".equals(machine.getConfig(AbstractLocation.ORIGINAL_SPEC))) {
                            checkLocationParametersCompatible(machine, oldLoc, "hostname",
                                    oldLoc.getAddress().getHostName(), machine.getAddress().getHostName());
                            checkLocationParametersCompatible(machine, oldLoc, "address",
                                    oldLoc.getAddress().getHostAddress(), machine.getAddress().getHostAddress());
                        }
                    }
                    log.debug("Entity " + entity() + " old machine locations " + oldSshLocs + " were compatible, removing them to start at " + machine);
                    entity().removeLocations(oldSshLocs);
                }
            }
            entity().addLocations(ImmutableList.of((Location) machine));

            // elsewhere we rely on (public) hostname being set _after_ subnet_hostname
            // (to prevent the tiny possibility of races resulting in hostname being returned
            // simply because subnet is still being looked up)
            Maybe<String> lh = Machines.getSubnetHostname(machine);
            Maybe<String> la = Machines.getSubnetIp(machine);
            if (lh.isPresent() && entity().sensors().get(Attributes.SUBNET_HOSTNAME) == null) {
                entity().sensors().set(Attributes.SUBNET_HOSTNAME, lh.get());
            }
            if (la.isPresent() && entity().sensors().get(Attributes.SUBNET_ADDRESS) == null) {
                entity().sensors().set(Attributes.SUBNET_ADDRESS, la.get());
            }
            if (entity().sensors().get(Attributes.HOSTNAME) == null) {
                entity().sensors().set(Attributes.HOSTNAME, machine.getAddress().getHostName());
            }
            if (entity().sensors().get(Attributes.ADDRESS) == null) {
                entity().sensors().set(Attributes.ADDRESS, machine.getAddress().getHostAddress());
            }
            if (machine instanceof SshMachineLocation) {
                SshMachineLocation sshMachine = (SshMachineLocation) machine;
                UserAndHostAndPort sshAddress = UserAndHostAndPort.fromParts(
                        sshMachine.getUser(), sshMachine.getAddress().getHostName(), sshMachine.getPort());
                // FIXME: Who or what is SSH_ADDRESS intended for? It's not necessarily the address that
                // the SshMachineLocation is using for ssh connections (because it accepts SSH_HOST as an override).
                entity().sensors().set(Attributes.SSH_ADDRESS, sshAddress);
            }

            if (Boolean.TRUE.equals(entity().getConfig(SoftwareProcess.OPEN_IPTABLES))) {
                if (machine instanceof SshMachineLocation) {
                    @SuppressWarnings("unchecked")
                    Iterable<Integer> inboundPorts = (Iterable<Integer>) machine.config().get(CloudLocationConfig.INBOUND_PORTS);
                    machineInitTasks.openIptablesAsync(inboundPorts, (SshMachineLocation)machine);
                } else {
                    log.warn("Ignoring flag OPEN_IPTABLES on non-ssh location {}", machine);
                }
            }
            if (Boolean.TRUE.equals(entity().getConfig(SoftwareProcess.STOP_IPTABLES))) {
                if (machine instanceof SshMachineLocation) {
                    machineInitTasks.stopIptablesAsync((SshMachineLocation)machine);
                } else {
                    log.warn("Ignoring flag STOP_IPTABLES on non-ssh location {}", machine);
                }
            }
            if (Boolean.TRUE.equals(entity().getConfig(SoftwareProcess.DONT_REQUIRE_TTY_FOR_SUDO))) {
                if (machine instanceof SshMachineLocation) {
                    machineInitTasks.dontRequireTtyForSudoAsync((SshMachineLocation)machine);
                } else {
                    log.warn("Ignoring flag DONT_REQUIRE_TTY_FOR_SUDO on non-ssh location {}", machine);
                }
            }
            resolveOnBoxDir(entity(), machine);
            preStartCustom(machine, parameters);

        }
    }

    /**
     * Resolves the on-box dir.
     * <p>
     * Initialize and pre-create the right onbox working dir, if an ssh machine location.
     * Logs a warning if not.
     */
    @SuppressWarnings("deprecation")
    public static String resolveOnBoxDir(EntityInternal entity, MachineLocation machine) {
        String base = entity.getConfig(BrooklynConfigKeys.ONBOX_BASE_DIR);
        if (base==null) base = machine.getConfig(BrooklynConfigKeys.ONBOX_BASE_DIR);
        if (base!=null && Boolean.TRUE.equals(entity.getConfig(ON_BOX_BASE_DIR_RESOLVED))) return base;
        if (base==null) base = entity.getManagementContext().getConfig().getConfig(BrooklynConfigKeys.ONBOX_BASE_DIR);
        if (base==null) base = entity.getConfig(BrooklynConfigKeys.BROOKLYN_DATA_DIR);
        if (base==null) base = machine.getConfig(BrooklynConfigKeys.BROOKLYN_DATA_DIR);
        if (base==null) base = entity.getManagementContext().getConfig().getConfig(BrooklynConfigKeys.BROOKLYN_DATA_DIR);
        if (base==null) base = "~/brooklyn-managed-processes";
        
        if (base.equals("~")) base=".";
        if (base.startsWith("~/")) base = "."+base.substring(1);

        String resolvedBase = null;
        if (entity.getConfig(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION) || machine.getConfig(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION)) {
            if (log.isDebugEnabled()) log.debug("Skipping on-box base dir resolution for "+entity+" at "+machine);
            if (!Os.isAbsolutish(base)) base = "~/"+base;
            resolvedBase = Os.tidyPath(base);
        } else if (machine instanceof CanResolveOnBoxDir) {
            resolvedBase = ((CanResolveOnBoxDir)machine).resolveOnBoxDirFor(entity, base);
        }
        if (resolvedBase==null) {
            if (!Os.isAbsolutish(base)) base = "~/"+base;
            resolvedBase = Os.tidyPath(base);
            log.warn("Could not resolve on-box directory for "+entity+" at "+machine+"; using "+resolvedBase+", though this may not be accurate at the target (and may fail shortly)");
        }
        entity.config().set(BrooklynConfigKeys.ONBOX_BASE_DIR, resolvedBase);
        entity.config().set(ON_BOX_BASE_DIR_RESOLVED, true);
        return resolvedBase;
    }

    protected void checkLocationParametersCompatible(MachineLocation oldLoc, MachineLocation newLoc, String paramSummary,
            Object oldParam, Object newParam) {
        if (oldParam==null || newParam==null || !oldParam.equals(newParam))
            throw new IllegalStateException("Cannot start "+entity()+" in "+newLoc+" as it has already been started with incompatible location "+oldLoc+" " +
                    "("+paramSummary+" not compatible: "+oldParam+" / "+newParam+"); "+newLoc+" may require manual removal.");
    }

    protected void preStartCustom(MachineLocation machine, ConfigBag parameters) {
        ConfigToAttributes.apply(entity());
    }

    protected Map<String, Object> obtainProvisioningFlags(final MachineProvisioningLocation<?> location) {
        if (entity() instanceof ProvidesProvisioningFlags) {
            return ((ProvidesProvisioningFlags)entity()).obtainProvisioningFlags(location).getAllConfig();
        }
        return MutableMap.<String, Object>of();
    }

    protected abstract String startProcessesAtMachine(final Supplier<MachineLocation> machineS);

    protected void postStartAtMachineAsync(ConfigBag parameters) {
        DynamicTasks.queue("post-start", new PostStartTask(parameters));
    }

    protected class PostStartTask implements Runnable {
        protected final ConfigBag parameters;

        public PostStartTask() { this(null); }
        public PostStartTask(ConfigBag parameters) {
            this.parameters = parameters;
        }

        @Override
        public void run() {
            postStartCustom(parameters);
        }
    }

    /**
     * Default post-start hooks.
     * <p>
     * Can be extended by subclasses, and typically will wait for confirmation of start.
     * The service not set to running until after this. Also invoked following a restart.
     */
    protected void postStartCustom(ConfigBag parameters) {
        // nothing by default
    }

    /**
     * whether when 'auto' mode is specified, the machine should be stopped when the restart effector is called
     * <p>
     * with {@link MachineLifecycleEffectorTasks}, a machine will always get created on restart if there wasn't one already
     * (unlike certain subclasses which might attempt a shortcut process-level restart)
     * so there is no reason for default behaviour of restart to throw away a provisioned machine,
     * hence default impl returns <code>false</code>.
     * <p>
     * if it is possible to tell that a machine is unhealthy, or if {@link #restart(ConfigBag)} is overridden,
     * then it might be appropriate to return <code>true</code> here.
     */
    protected boolean getDefaultRestartStopsMachine() {
        return false;
    }

    /**
     * Default restart implementation for an entity.
     * <p>
     * Stops processes if possible, then starts the entity again.
     */
    public void restart(ConfigBag parameters) {
        ServiceStateLogic.setExpectedState(entity(), Lifecycle.STOPPING);

        RestartMachineMode isRestartMachine = parameters.get(RestartSoftwareParameters.RESTART_MACHINE_TYPED);
        if (isRestartMachine==null)
            isRestartMachine=RestartMachineMode.AUTO;
        if (isRestartMachine==RestartMachineMode.AUTO)
            isRestartMachine = getDefaultRestartStopsMachine() ? RestartMachineMode.TRUE : RestartMachineMode.FALSE;

        // Calling preStopCustom without a corresponding postStopCustom invocation
        // doesn't look right so use a separate callback pair; Also depending on the arguments
        // stop() could be called which will call the {pre,post}StopCustom on its own.
        DynamicTasks.queue("pre-restart", new PreRestartTask());

        if (isRestartMachine==RestartMachineMode.FALSE) {
            DynamicTasks.queue("stopping (process)", new StopProcessesAtMachineTask());
        } else {
            Map<String, Object> stopMachineFlags =  MutableMap.of();
            if (Entitlements.getEntitlementContext() != null) {
                stopMachineFlags.put("tags", MutableSet.of(BrooklynTaskTags.tagForEntitlement(Entitlements.getEntitlementContext())));
            }
            Task<String> stopTask = Tasks.<String>builder()
                    .displayName("stopping (machine)")
                    .body(new StopMachineTask())
                    .flags(stopMachineFlags)
                    .build();
            DynamicTasks.queue(stopTask);
        }

        DynamicTasks.queue("starting", new StartInLocationsTask());
        restartChildren(parameters);
        DynamicTasks.queue("post-restart", new PostRestartTask());

        DynamicTasks.waitForLast();
        ServiceStateLogic.setExpectedState(entity(), Lifecycle.RUNNING);
    }

    protected class PreRestartTask implements Runnable {
        protected final ConfigBag parameters;

        public PreRestartTask() { this(null); }
        public PreRestartTask(ConfigBag parameters) {
            this.parameters = parameters;
        }

        @Override
        public void run() {
            preRestartCustom(parameters);
        }
    }
    protected class PostRestartTask implements Runnable {
        protected final ConfigBag parameters;

        public PostRestartTask() { this(null); }
        public PostRestartTask(ConfigBag parameters) {
            this.parameters = parameters;
        }

        @Override
        public void run() {
            postRestartCustom(parameters);
        }
    }
    protected class StartInLocationsTask implements Runnable {
        protected final ConfigBag parameters;

        public StartInLocationsTask() { this(null); }
        public StartInLocationsTask(ConfigBag parameters) {
            this.parameters = parameters;
        }
        @Override
        public void run() {
            // startInLocations will look up the location, and provision a machine if necessary
            // (if it remembered the provisioning location)
            ServiceStateLogic.setExpectedState(entity(), Lifecycle.STARTING);
            startInLocations(null, parameters);
        }
    }

    protected void restartChildren(ConfigBag parameters) {
        // TODO should we consult ChildStartableMode?

        Boolean isRestartChildren = parameters.get(RestartSoftwareParameters.RESTART_CHILDREN);
        if (isRestartChildren==null || !isRestartChildren) {
            return;
        }

        if (isRestartChildren) {
            DynamicTasks.queue(StartableMethods.restartingChildren(entity(), parameters));
            return;
        }

        throw new IllegalArgumentException("Invalid value '"+isRestartChildren+"' for "+RestartSoftwareParameters.RESTART_CHILDREN.getName());
    }

    /**
     * Default stop implementation for an entity.
     * <p>
     * Aborts if already stopped, otherwise sets state {@link Lifecycle#STOPPING} then
     * invokes {@link #preStopCustom(ConfigBag parameters)}, {@link #stopProcessesAtMachine(ConfigBag parameters)}, then finally
     * {@link #stopAnyProvisionedMachines(ConfigBag parameters)} and sets state {@link Lifecycle#STOPPED}.
     * If no errors were encountered call {@link #postStopCustom(ConfigBag parameters)} at the end.
     */
    public void stop(ConfigBag parameters) {
        doStopLatching(parameters, new StopAnyProvisionedMachinesTask(parameters));
    }

    /**
     * As {@link #stop} but calling {@link #suspendAnyProvisionedMachines} rather than
     * {@link #stopAnyProvisionedMachines}.
     */
    public void suspend(ConfigBag parameters) {
        doStopLatching(parameters, new SuspendAnyProvisionedMachinesTask());
    }

    protected void doStopLatching(ConfigBag parameters, Callable<StopMachineDetails<Integer>> stopTask) {
        try (CloseableLatch latch = waitForCloseableLatch(entity(), SoftwareProcess.STOP_LATCH)) {
            doStop(parameters, stopTask);
        }
    }

    protected void doStop(ConfigBag parameters, Callable<StopMachineDetails<Integer>> stopTask) {
        preStopConfirmCustom(parameters);

        log.info("Stopping {} in {}", entity(), entity().getLocations());

        StopMode stopMachineMode = getStopMachineMode(parameters);
        StopMode stopProcessMode = parameters.get(StopSoftwareParameters.STOP_PROCESS_MODE);

        DynamicTasks.queue("pre-stop", new PreStopCustomTask(parameters));

        // BROOKLYN-263:
        // With this change the stop effector will wait for Location to provision so it can terminate
        // the machine, if a provisioning request is in-progress.
        //
        // The ProvisionMachineTask stores transient internal state in PROVISIONING_TASK_STATE and
        // PROVISIONED_MACHINE: it records when the provisioning is running and when done; and it
        // records the final machine. We record the machine in the internal sensor (rather than 
        // just relying on getLocations) because the latter is set much later in the start() 
        // process.
        //
        // This code is a big improvement (previously there was a several-minute window in some 
        // clouds where a call to stop() would leave the machine running). 
        //
        // However, there are still races. If the start() code has not yet reached the call to 
        // location.obtain() then we won't wait, and the start() call won't know to abort. It's 
        // fiddly to get that right, because we need to cope with restart() - so we mustn't leave 
        // any state behind that will interfere with subsequent sequential calls to start().
        // There is some attempt to handle it by ProvisionMachineTask checking if the expectedState
        // is stopping/stopped.
        Maybe<MachineLocation> machine = Machines.findUniqueMachineLocation(entity().getLocations());
        ProvisioningTaskState provisioningState = entity().sensors().get(AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE);

        if (machine.isAbsent() && provisioningState == ProvisioningTaskState.RUNNING) {
            Duration maxWait = entity().config().get(STOP_WAIT_PROVISIONING_TIMEOUT);
            log.info("When stopping {}, waiting for up to {} for the machine to finish provisioning, before terminating it", entity(), maxWait);
            boolean success = Repeater.create("Wait for a machine to appear")
                    .until(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            ProvisioningTaskState state = entity().sensors().get(AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE);
                            return (state != ProvisioningTaskState.RUNNING);
                        }})
                    .backoffTo(Duration.FIVE_SECONDS)
                    .limitTimeTo(maxWait)
                    .run();
            if (!success) {
                log.warn("When stopping {}, timed out after {} waiting for the machine to finish provisioning - machine may we left running", entity(), maxWait);
            }
            machine = Maybe.ofDisallowingNull(entity().sensors().get(INTERNAL_PROVISIONED_MACHINE));
        }
        entity().sensors().remove(AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE);
        entity().sensors().remove(INTERNAL_PROVISIONED_MACHINE);

        Task<List<?>> stoppingProcess = null;
        if (canStop(stopProcessMode, entity())) {
            stoppingProcess = Tasks.parallel("stopping",
                    Tasks.create("stopping (process)", new StopProcessesAtMachineTask(parameters)),
                    Tasks.create("stopping (feeds)", new StopFeedsAtMachineTask(parameters)));
            DynamicTasks.queue(stoppingProcess);
        }

        Task<StopMachineDetails<Integer>> stoppingMachine = null;
        if (canStop(stopMachineMode, machine.isAbsent())) {
            // Release this machine (even if error trying to stop process - we rethrow that after)
            Map<String, Object> stopMachineFlags =  MutableMap.of();
            if (Entitlements.getEntitlementContext() != null) {
                stopMachineFlags.put("tags", MutableSet.of(BrooklynTaskTags.tagForEntitlement(Entitlements.getEntitlementContext())));
            }
            Task<StopMachineDetails<Integer>> stopMachineTask = Tasks.<StopMachineDetails<Integer>>builder()
                    .displayName("stopping (machine)")
                    .body(stopTask).flags(stopMachineFlags)
                    .build();
            stoppingMachine = DynamicTasks.queue(stopMachineTask);

            DynamicTasks.drain(entity().getConfig(STOP_PROCESS_TIMEOUT), false);

            // shutdown the machine if stopping process fails or takes too long
            synchronized (stoppingMachine) {
                // task also used as mutex by DST when it submits it; ensure it only submits once!
                if (!stoppingMachine.isSubmitted()) {
                    // force the stoppingMachine task to run by submitting it here
                    StringBuilder msg = new StringBuilder("Submitting machine stop early in background for ").append(entity());
                    if (stoppingProcess == null) {
                        msg.append(". Process stop skipped, pre-stop not finished?");
                    } else {
                        msg.append(" because process stop has ").append(
                                (stoppingProcess.isDone() ? "finished abnormally" : "not finished"));
                    }
                    log.warn(msg.toString());
                    Entities.submit(entity(), stoppingMachine);
                }
            }
        }

        try {
            // This maintains previous behaviour of silently squashing any errors on the stoppingProcess task if the
            // stoppingMachine exits with a nonzero value
            boolean checkStopProcesses = (stoppingProcess != null && (stoppingMachine == null || stoppingMachine.get().value == 0));

            if (checkStopProcesses) {
                // TODO we should test for destruction above, not merely successful "stop", as things like localhost and ssh won't be destroyed
                DynamicTasks.waitForLast();
                if (machine.isPresent()) {
                    // throw early errors *only if* there is a machine and we have not destroyed it
                    stoppingProcess.get();
                }
            }
        } catch (Throwable e) {
            ServiceStateLogic.setExpectedState(entity(), Lifecycle.ON_FIRE);
            Exceptions.propagate(e);
        }
        entity().sensors().set(SoftwareProcess.SERVICE_UP, false);
        ServiceStateLogic.setExpectedState(entity(), Lifecycle.STOPPED);

        DynamicTasks.queue("post-stop", new PostStopCustomTask());

        if (log.isDebugEnabled()) log.debug("Stopped software process entity "+entity());
    }

    protected class StopAnyProvisionedMachinesTask implements Callable<StopMachineDetails<Integer>> {
        private final ConfigBag parameters;

        public StopAnyProvisionedMachinesTask() { this(ConfigBag.newInstance()); }
        public StopAnyProvisionedMachinesTask(ConfigBag parameters) { this.parameters = parameters;
        }
        @Override
        public StopMachineDetails<Integer> call() {
            return stopAnyProvisionedMachines(parameters);
        }
    }

    protected class SuspendAnyProvisionedMachinesTask implements Callable<StopMachineDetails<Integer>> {
        protected final ConfigBag parameters;

        public SuspendAnyProvisionedMachinesTask() { this(null); }
        public SuspendAnyProvisionedMachinesTask(ConfigBag parameters) {
            this.parameters = parameters;
        }

        @Override
        public StopMachineDetails<Integer> call() {
            return suspendAnyProvisionedMachines(parameters);
        }
    }

    protected class StopProcessesAtMachineTask implements Callable<String> {
        protected final ConfigBag parameters;

        public StopProcessesAtMachineTask() { this(null); }
        public StopProcessesAtMachineTask(ConfigBag parameters) {
            this.parameters = parameters;
        }

        @Override
        public String call() {
            DynamicTasks.markInessential();
            stopProcessesAtMachine(parameters);
            DynamicTasks.waitForLast();
            return "Stop processes completed with no errors.";
        }
    }

    protected class StopFeedsAtMachineTask implements Callable<String> {
        protected final ConfigBag parameters;

        public StopFeedsAtMachineTask() { this(null); }
        public StopFeedsAtMachineTask(ConfigBag parameters) {
            this.parameters = parameters;
        }

        @Override
        public String call() {
            DynamicTasks.markInessential();
            for (Feed feed : entity().feeds().getFeeds()) {
                if (feed.isActivated()) feed.stop();
            }
            DynamicTasks.waitForLast();
            return "Stop feeds completed with no errors.";
        }
    }

    protected class StopMachineTask implements Callable<String> {
        protected final ConfigBag parameters;

        public StopMachineTask() { this(null); }
        public StopMachineTask(ConfigBag parameters) {
            this.parameters = parameters;
        }

        @Override
        public String call() {
            DynamicTasks.markInessential();
            stop(ConfigBag.newInstance().configure(StopSoftwareParameters.STOP_MACHINE_MODE, StopMode.IF_NOT_STOPPED));
            DynamicTasks.waitForLast();
            return "Stop of machine completed with no errors.";
        }
    }

    protected class PreStopCustomTask implements Callable<String> {
        protected final ConfigBag parameters;

        public PreStopCustomTask() { this(null); }
        public PreStopCustomTask(ConfigBag parameters) {
            this.parameters = parameters;
        }

        @Override
        public String call() {
            if (entity().getAttribute(SoftwareProcess.SERVICE_STATE_ACTUAL) == Lifecycle.STOPPED) {
                log.debug("Skipping stop of entity " + entity() + " when already stopped");
                return "Already stopped";
            }
            ServiceStateLogic.setExpectedState(entity(), Lifecycle.STOPPING);
            entity().sensors().set(SoftwareProcess.SERVICE_UP, false);
            preStopCustom(parameters);
            return null;
        }
    }

    protected class PostStopCustomTask implements Callable<Void> {
        protected final ConfigBag parameters;

        public PostStopCustomTask() { this(null); }
        public PostStopCustomTask(ConfigBag parameters) {
            this.parameters = parameters;
        }

        @Override
        public Void call() {
            postStopCustom(parameters);
            return null;
        }
    }

    public static StopMode getStopMachineMode(ConfigBag parameters) {
        final StopMode stopMachineMode = parameters.get(StopSoftwareParameters.STOP_MACHINE_MODE);
        return stopMachineMode;
    }

    public static boolean canStop(StopMode stopMode, Entity entity) {
        boolean isEntityStopped = entity.getAttribute(SoftwareProcess.SERVICE_STATE_ACTUAL)==Lifecycle.STOPPED;
        return canStop(stopMode, isEntityStopped);
    }

    protected static boolean canStop(StopMode stopMode, boolean isStopped) {
        return stopMode == StopMode.ALWAYS ||
                stopMode == StopMode.IF_NOT_STOPPED && !isStopped;
    }

    @Deprecated
    protected void preStopConfirmCustom(ConfigBag parameters) {
    }

    protected void preStopCustom(ConfigBag parameters) {
        // nothing needed here
    }

    protected void postStopCustom(ConfigBag parameters) {
        // nothing needed here
    }

    protected void preRestartCustom(ConfigBag parameters) {
        // nothing needed here
    }

    protected void postRestartCustom(ConfigBag parameters) {
        // nothing needed here
    }

    public static class StopMachineDetails<T> implements Serializable {
        private static final long serialVersionUID = 3256747214315895431L;
        public final String message;
        public final T value;
        public StopMachineDetails(String message, T value) {
            this.message = message;
            this.value = value;
        }
        @Override
        public String toString() {
            return message;
        }
    }

    /**
     * Return string message of result.
     * <p>
     * Can run synchronously or not, caller will submit/queue as needed, and will block on any submitted tasks.
     */
    protected abstract String stopProcessesAtMachine(ConfigBag parameters);

    /**
     * Stop and release the {@link MachineLocation} the entity is provisioned at.
     * <p>
     * Can run synchronously or not, caller will submit/queue as needed, and will block on any submitted tasks.
     */
    protected StopMachineDetails<Integer> stopAnyProvisionedMachines(ConfigBag parameters) {
        MachineProvisioningLocation<MachineLocation> provisioner = entity().getAttribute(SoftwareProcess.PROVISIONING_LOCATION);

        if (Iterables.isEmpty(entity().getLocations())) {
            log.debug("No machine decommissioning necessary for "+entity()+" - no locations");
            return new StopMachineDetails<Integer>("No machine decommissioning necessary - no locations", 0);
        }

        // Only release this machine if we ourselves provisioned it (e.g. it might be running other services)
        if (provisioner==null) {
            log.debug("No machine decommissioning necessary for "+entity()+" - did not provision");
            return new StopMachineDetails<Integer>("No machine decommissioning necessary - did not provision", 0);
        }

        Location machine = getLocation(null);
        if (!(machine instanceof MachineLocation)) {
            log.debug("No decommissioning necessary for "+entity()+" - not a machine location ("+machine+")");
            return new StopMachineDetails<Integer>("No machine decommissioning necessary - not a machine ("+machine+")", 0);
        }

        entity().sensors().set(AttributesInternal.INTERNAL_TERMINATION_TASK_STATE, ProvisioningTaskState.RUNNING);
        try {
            return stopProvisionedMachine(provisioner, machine, parameters);
        } finally {
            // TODO On exception, should we add the machine back to the entity (because it might not really be terminated)?
            //      Do we need a better exception hierarchy for that?
            entity().sensors().remove(AttributesInternal.INTERNAL_TERMINATION_TASK_STATE);
        }
    }

    protected StopMachineDetails<Integer> stopProvisionedMachine(MachineProvisioningLocation<MachineLocation> provisioner, Location machine, ConfigBag parameters) {
        boolean succeeded = false;
        try {
            provisioner.release((MachineLocation) machine);
            clearEntityLocationAttributes(machine, true);
            succeeded = true;
        } finally {
            if (!succeeded) {
                if (!Locations.isManaged(machine)) {
                    log.warn("Stopping "+machine+" failed, will rethrow exception shortly, but as the location is no longer managed it is being cleared on any entities");
                    // there was a failure, but before failing, it proceeded far enough that the location is no longer managed by brooklyn
                    // which means the location will not be usable, so let's clear it
                    clearEntityLocationAttributes(machine, true);
                } else {
                    log.debug("Stopping "+machine+" failed; previously attributes would have been cleared but now they are being kept to facilitate deletion in future");
                    // 2023-01 we no longer remove sensors
                    // clearEntityLocationAttributes(machine, false);
                }
            }
        }
        return new StopMachineDetails<Integer>("Decommissioned "+machine, 1) {};
    }

    /**
     * Suspend the {@link MachineLocation} the entity is provisioned at.
     * <p>
     * Expects the entity's {@link SoftwareProcess#PROVISIONING_LOCATION provisioner} to be capable of
     * {@link SuspendsMachines suspending machines}.
     *
     * @throws java.lang.UnsupportedOperationException if the entity's provisioner cannot suspend machines.
     * @see MachineManagementMixins.SuspendsMachines
     */
    protected StopMachineDetails<Integer> suspendAnyProvisionedMachines(ConfigBag parameters) {
        @SuppressWarnings("unchecked")
        MachineProvisioningLocation<MachineLocation> provisioner = entity().getAttribute(SoftwareProcess.PROVISIONING_LOCATION);

        if (Iterables.isEmpty(entity().getLocations())) {
            log.debug("No machine decommissioning necessary for " + entity() + " - no locations");
            return new StopMachineDetails<>("No machine suspend necessary - no locations", 0);
        }

        // Only release this machine if we ourselves provisioned it (e.g. it might be running other services)
        if (provisioner == null) {
            log.debug("No machine decommissioning necessary for " + entity() + " - did not provision");
            return new StopMachineDetails<>("No machine suspend necessary - did not provision", 0);
        }

        Location machine = getLocation(null);
        if (!(machine instanceof MachineLocation)) {
            log.debug("No decommissioning necessary for " + entity() + " - not a machine location (" + machine + ")");
            return new StopMachineDetails<>("No machine suspend necessary - not a machine (" + machine + ")", 0);
        }

        if (!(provisioner instanceof SuspendsMachines)) {
            log.debug("Location provisioner ({}) cannot suspend machines", provisioner);
            throw new UnsupportedOperationException("Location provisioner cannot suspend machines: " + provisioner);
        }

        clearEntityLocationAttributes(machine, false);
        SuspendsMachines.class.cast(provisioner).suspendMachine(MachineLocation.class.cast(machine));

        return new StopMachineDetails<>("Suspended " + machine, 1);
    }

    /**
     * Nulls the attached entity's hostname, address, subnet hostname and subnet address sensors
     * and removes the given machine from its locations.
     */
    protected void clearEntityLocationAttributes(Location machine) {
        clearEntityLocationAttributes(machine, true);
    }
    protected void clearEntityLocationAttributes(Location machine, boolean removeLocation) {
        if (removeLocation) entity().removeLocations(ImmutableList.of(machine));

        MachineProvisioningLocation oldProvisioningLocation = entity().sensors().get(SoftwareProcess.PROVISIONING_LOCATION);
        if (oldProvisioningLocation!=null && !Locations.isManaged(oldProvisioningLocation)) {
            // clear it if it has been unmanaged, to prevent rebind dangles
            entity().sensors().set(SoftwareProcess.PROVISIONING_LOCATION, null);
        }

        entity().sensors().set(Attributes.HOSTNAME, null);
        entity().sensors().set(Attributes.ADDRESS, null);
        entity().sensors().set(Attributes.SUBNET_HOSTNAME, null);
        entity().sensors().set(Attributes.SUBNET_ADDRESS, null);
    }

    // Removes the checked Exception from the method signature
    public static class CloseableLatch implements AutoCloseable {
        private Entity caller;
        private ReleaseableLatch releaseableLatch;

        public CloseableLatch(Entity caller, ReleaseableLatch releaseableLatch) {
            this.caller = caller;
            this.releaseableLatch = releaseableLatch;
        }

        @Override
        public void close() {
            DynamicTasks.drain(null, false);
            releaseableLatch.release(caller);
        }
    }

    public static CloseableLatch waitForCloseableLatch(Entity entity, ConfigKey<Boolean> configKey) {
        ReleaseableLatch releaseableLatch = waitForLatch((EntityInternal)entity, configKey);
        return new CloseableLatch(entity, releaseableLatch);
    }

    protected static ReleaseableLatch waitForLatch(EntityInternal entity, ConfigKey<Boolean> configKey) {
        Maybe<?> rawValue = entity.config().getRaw(configKey);
        if (rawValue.isAbsent()) {
            return ReleaseableLatch.NOP;
        } else {
            ValueResolverIterator<Boolean> iter = resolveLatchIterator(entity, rawValue.get(), configKey);
            // The iterator is used to prevent coercion; the value should always be the last one, but iter.last() will return a coerced Boolean
            Maybe<ReleaseableLatch> releasableLatchMaybe = iter.next(ReleaseableLatch.class);
            if (releasableLatchMaybe.isPresent()) {
                ReleaseableLatch latch = releasableLatchMaybe.get();
                log.debug("{} finished waiting for {} (value {}); waiting to acquire the latch", new Object[] {entity, configKey, latch});
                Tasks.setBlockingDetails("Acquiring " + configKey + " " + latch);
                try {
                    latch.acquire(entity);
                } finally {
                    Tasks.resetBlockingDetails();
                }
                log.debug("{} Acquired latch {} (value {}); continuing...", new Object[] {entity, configKey, latch});
                return latch;
            } else {
                // If iter.next() above returned absent due to a resolve error next line will throw with the cause
                Boolean val = iter.last().get();
                if (rawValue != null) log.debug("{} finished waiting for {} (value {}); continuing...", new Object[] {entity, configKey, val});
                return ReleaseableLatch.NOP;
            }
        }
    }

    protected static ValueResolverIterator<Boolean> resolveLatchIterator(EntityInternal entity, Object val, ConfigKey<Boolean> key) {
        return Tasks.resolving(val, Boolean.class)
                .context(entity.getExecutionContext())
                .description("config " + key.getName())
                .iterator();
    }

    
}
