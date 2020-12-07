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
package org.apache.brooklyn.core.location;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.brooklyn.api.location.*;
import org.apache.brooklyn.api.location.MachineManagementMixins.GivesMachineMetadata;
import org.apache.brooklyn.api.location.MachineManagementMixins.GivesMetrics;
import org.apache.brooklyn.api.location.MachineManagementMixins.MachineMetadata;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal.ConfigurationSupportInternal;
import org.apache.brooklyn.util.JavaGroovyEquivalents;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MachineLifecycleUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MachineLifecycleUtils.class);

    private final MachineLocation location;

    public static ConfigKey<MachineStatus> STATUS = ConfigKeys.newConfigKey(MachineStatus.class, "status");

    public MachineLifecycleUtils(MachineLocation l) {
        this.location = l;
    }

    public enum MachineStatus {
        /** Either the machine is unknown at the location, or the machine may be known but status unknown or unrecognized,
         * or we are unable to find out. Can use {@link #exists()} to determine the difference between these cases (respectively: false, true, null). */
        UNKNOWN,
        RUNNING,
        SHUTDOWN,
        /** Note some providers report 'shutdown' as 'suspended' if they cannot tell the difference. */
        SUSPENDED,
        TRANSITIONING,
        ERROR
    }

    public interface GivesMachineStatus {
        MachineStatus getStatus();
    }

    /** true if the machine is known at its parent (at the actual provider), false if not known; null if cannot tell */
    @Nullable
    public Boolean exists() {
        if (location.getParent() instanceof MachineManagementMixins.GivesMachineMetadata) {
            MachineMetadata metadata = ((GivesMachineMetadata) location.getParent()).getMachineMetadata(location);
            return (metadata != null);
        }
        if (location.getParent() instanceof MachineManagementMixins.GivesMetrics) {
            ConfigBag metrics = ConfigBag.newInstance(((GivesMetrics) location.getParent()).getMachineMetrics(location));
            return (!metrics.isEmpty());
        }
        return null;
    }

    @Nonnull
    public MachineStatus getStatus() {
        if (location.getParent() instanceof MachineManagementMixins.GivesMetrics) {
            ConfigBag metrics = ConfigBag.newInstance(((GivesMetrics) location.getParent()).getMachineMetrics(location));
            MachineStatus s = metrics.get(STATUS);
            if (s!=null) {
                return s;
            }
        }
        if (location.getParent() instanceof MachineManagementMixins.GivesMachineMetadata) {
            MachineMetadata metadata = ((GivesMachineMetadata) location.getParent()).getMachineMetadata(location);
            if (metadata!=null) {
                if (metadata instanceof GivesMachineStatus) {
                    return ((GivesMachineStatus)metadata).getStatus();
                }
                if (metadata.isRunning()) {
                    return MachineStatus.RUNNING;
                } else {
                    return MachineStatus.UNKNOWN;
                }
            }
        }
        return MachineStatus.UNKNOWN;
    }

    Duration timeout = Duration.minutes(30);
    /** How long to delay on async operations, e.g. resume, including if machine was previuosly transitioning. Defaults 30 minutes. */
    public Duration getTimeout() {
        return timeout;
    }
    public void setTimeout(Duration timeout) {
        this.timeout = Preconditions.checkNotNull(timeout);
    }

    /**
     * Returns a masked error if the machine is already running.
     * Unmasked error if the machine is not resumable and we cannot confirm it is already running.
     * Otherwise returns the resumed machines status.
     * <p>
     * If in a transitional state will wait for the indicated timeout.
     * <p>
     * May return a different machine location than the one known here if critical metadata has changed (e.g. IP address);
     * however the instance/ID will be the same.
     */
    // TODO kill this?
    public ReferenceWithError<MachineLocation> resume() {
        MachineStatus status = getStatus();
        if (MachineStatus.RUNNING.equals(status)) return ReferenceWithError.newInstanceMaskingError(location, new Throwable("Already running"));

        if (location.getParent() instanceof MachineManagementMixins.ResumesMachines) {
            try {
                return ReferenceWithError.newInstanceWithoutError( ((MachineManagementMixins.ResumesMachines) location.getParent()).resumeMachine(getConfigMapWithId()) );
            } catch (Exception e) {
                return ReferenceWithError.newInstanceThrowingError( location, e );
            }
        }

        return ReferenceWithError.newInstanceThrowingError(location, new Throwable("Machine does not support resumption"));
    }

    /** Returns supplied machine if already running; otherwise if suspended, resumes; if shutdown, starts it up and may return same or different object.
     * If can't restart, or can't detect, it returns an absent. */
    public Maybe<MachineLocation> makeRunning() {

        CountdownTimer timer = CountdownTimer.newInstanceStarted(getTimeout());

        MachineStatus status = getStatus();
        Duration sleep = Duration.ONE_SECOND;
        while (MachineStatus.TRANSITIONING.equals(status)) {
            if (timer.isExpired()) {
                return Maybe.absent("Timeout waiting for "+location+" to be stable before running");
            }
            try {
                Duration s = sleep;
                Tasks.withBlockingDetails("waiting on "+location+" to be stable before running", () -> {
                    Time.sleep(Duration.min(s, timer.getDurationRemaining()));
                    return null;
                });
            } catch (Exception e) {
                return Maybe.absent(new IllegalStateException("Error waiting for "+location+" to be stable before running", e));
            }
            status = getStatus();
            sleep = Duration.min(sleep.multiply(1.2), Duration.ONE_MINUTE);
        }

        if (MachineStatus.RUNNING.equals(status)) return Maybe.of(location);

        if (MachineStatus.SUSPENDED.equals(status) && location.getParent() instanceof MachineManagementMixins.ResumesMachines) {
            return Maybe.of( ((MachineManagementMixins.ResumesMachines) location.getParent()).resumeMachine(getConfigMapWithId()) );
        }
        if (MachineStatus.SHUTDOWN.equals(status) && location.getParent() instanceof MachineManagementMixins.ShutsdownMachines) {
            return Maybe.of( ((MachineManagementMixins.ShutsdownMachines) location.getParent()).startupMachine(getConfigMapWithId()) );
        }

        return Maybe.absent("Unable to make "+location+" running from status "+status+"; no methods for doing so are available");
    }

    public MachineLocation makeRunningOrRecreate(Map<String,?> newConfig) throws NoMachinesAvailableException {
        Exception problemRunning = null;
        Maybe<MachineLocation> result = null;
        try {
            result = makeRunning();
            if (result.isPresent()) {
                return result.get();
            }
            problemRunning = Maybe.Absent.getException(result);
            // couldn't resume etc

        } catch (Exception e) {
            problemRunning = e;
        }

        if (problemRunning!=null) {
            LOG.warn("Unable to make existing machine running (" + location + "), will destroy and re-create: " + problemRunning);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Trace for: Unable to make existing machine running (" + location + "), will destroy and re-create: " + problemRunning, problemRunning);
            }

            DynamicTasks.queueIfPossible(Tasks.warning("Could not make existing machine running: " + Exceptions.collapseText(problemRunning), problemRunning));
        }

        if (!(location.getParent() instanceof MachineProvisioningLocation)) {
            throw new IllegalStateException("Cannot destroy/recreate "+location+" because parent is not a provisioning location, and cannot resume due to: "+problemRunning);

        }
        ((MachineProvisioningLocation)location.getParent()).release(location);

        return ((MachineProvisioningLocation)location.getParent()).obtain(newConfig);
    }

    public ConfigBag getConfig() {
        return ((ConfigurationSupportInternal)location.config()).getBag();
    }

    public Map<String,?> getConfigMapWithId() {
        MutableMap<String, Object> result = MutableMap.copyOf(getConfig().getAllConfig());
        if (location.getParent() instanceof MachineManagementMixins.GivesMachineMetadata) {
            MachineMetadata metadata = ((GivesMachineMetadata) location.getParent()).getMachineMetadata(location);
            if (metadata!=null) {
                result.add("id", metadata.getId());
            }
        }
        return result;
    }

    /** If two locations point at the same instance; primarily looking at instance IDs, optionally also looking at addressibility.
     *
     * Locations are "primarily" the same if they have the same instance ID.
     * (Would be nice to confirm that the parents are the same, but that's harder, and not necessary for our use cases.)
     * <p>
     * "Optionally" they can be addressible the same if they have the same public and private IPs and user access (maybe creds, depending on implementations).
     * <p>
     * Null may be returned if we cannot tell.
     */
    public static Boolean isSameInstance(MachineLocation m1, MachineLocation m2, boolean requireSameAccessDetails) {
        Boolean primarilySame = null;
        Location p1 = m1.getParent();
        Location p2 = m2.getParent();
        if (p1 instanceof GivesMachineMetadata || p2 instanceof GivesMachineMetadata) {
            if (p1 instanceof GivesMachineMetadata && p2 instanceof GivesMachineMetadata) {
                String m1id = ((GivesMachineMetadata)p1).getMachineMetadata(m1).getId();
                String m2id = ((GivesMachineMetadata)p2).getMachineMetadata(m2).getId();
                if (Objects.equals(m1id, m2id)) {
                    if (m1id!=null) {
                        primarilySame = true;
                    }  else {
                        // indeterminate
                    }
                } else {
                    primarilySame = false;
                }
            } else {
                primarilySame = false;
            }
        }

        if (Boolean.FALSE.equals(primarilySame)) return false;

        if (requireSameAccessDetails) {
            List<Function<MachineLocation,Object>> reqs = MutableList.of();
            reqs.add(MachineLocation::getUser);
            reqs.add(MachineLocation::getHostname);
            reqs.add(MachineLocation::getAddress);
            reqs.add(MachineLocation::getPrivateAddresses);
            reqs.add(MachineLocation::getPublicAddresses);

            for (Function<MachineLocation,Object> f: reqs) {
                Object v1 = f.apply(m1);
                if (!Objects.equals(v1, f.apply(m2))) return false;
                if (JavaGroovyEquivalents.groovyTruth(v1)) primarilySame = true;
            }
        }

        return primarilySame;
    }

}
