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

package org.apache.brooklyn.location.jclouds;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.brooklyn.util.time.Duration;
import org.jclouds.domain.LoginCredentials;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.net.HostAndPort;

/**
 * Holds parameters to be used by a {@link ConnectivityResolver}.
 */
@Beta
public class ConnectivityResolverOptions {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private boolean isWindows = false;

        private boolean waitForConnectable;
        private boolean pollForReachableAddresses;
        private Predicate<? super HostAndPort> reachableAddressPredicate;
        private Duration reachableAddressTimeout;
        private boolean propagatePollForReachableFailure;

        private LoginCredentials initialCredentials;
        private LoginCredentials userCredentials;

        private int defaultLoginPort;
        private boolean usePortForwarding;
        private HostAndPort portForwardSshOverride;
        private boolean isRebinding;
        private boolean skipJcloudsSshing;

        public Builder pollForReachableAddresses(
                @Nonnull Predicate<? super HostAndPort> reachable,
                @Nonnull Duration timeout,
                boolean propagatePollFailure) {
            this.pollForReachableAddresses = true;
            this.reachableAddressPredicate = reachable;
            this.reachableAddressTimeout = timeout;
            this.propagatePollForReachableFailure = propagatePollFailure;
            return this;
        }

        public Builder noPollForReachableAddresses() {
            this.pollForReachableAddresses = false;
            this.reachableAddressPredicate = null;
            this.reachableAddressTimeout = null;
            this.propagatePollForReachableFailure = false;
            return this;
        }

        public Builder initialCredentials(@Nullable LoginCredentials initialCredentials) {
            this.initialCredentials = initialCredentials;
            return this;
        }

        public Builder userCredentials(@Nullable LoginCredentials userCredentials) {
            this.userCredentials = userCredentials;
            return this;
        }

        public Builder isWindows(boolean windows) {
            isWindows = windows;
            return this;
        }

        /** Indicate the host and port that should be used over all others. Normally used in tandem with a port forwarder. */
        public Builder portForwardSshOverride(@Nullable HostAndPort hostAndPortOverride) {
            this.portForwardSshOverride = hostAndPortOverride;
            return this;
        }

        public Builder isRebinding(boolean isRebinding) {
            this.isRebinding = isRebinding;
            return this;
        }

        public Builder skipJcloudsSshing(boolean skipJcloudsSshing) {
            this.skipJcloudsSshing = skipJcloudsSshing;
            return this;
        }

        public Builder defaultLoginPort(int defaultLoginPort) {
            this.defaultLoginPort = defaultLoginPort;
            return this;
        }

        public Builder usePortForwarding(boolean usePortForwarding) {
            this.usePortForwarding = usePortForwarding;
            return this;
        }

        public Builder waitForConnectable(boolean waitForConnectable) {
            this.waitForConnectable = waitForConnectable;
            return this;
        }

        public ConnectivityResolverOptions build() {
            return new ConnectivityResolverOptions(
                    isWindows, waitForConnectable, pollForReachableAddresses, reachableAddressPredicate,
                    propagatePollForReachableFailure, reachableAddressTimeout, initialCredentials, userCredentials,
                    defaultLoginPort, usePortForwarding, portForwardSshOverride, isRebinding, skipJcloudsSshing);
        }
    }

    private final boolean isWindows;

    /** Wait for Windows machines to be available over WinRM and other machines over SSH */
    // TODO: Merge this with pollForReachable when waitForSshable and waitForWinRmable deleted.
    private final boolean waitForConnectable;

    /** Wait for a machine's ip:port to be available. */
    private final boolean pollForReachableAddresses;
    private final Predicate<? super HostAndPort> reachableAddressPredicate;
    private final boolean propagatePollForReachableFailure;
    private final Duration reachableAddressTimeout;

    private final LoginCredentials initialCredentials;
    private final LoginCredentials userCredentials;
    private final int defaultLoginPort;

    // TODO: Can usePortForwarding and portForwardSshOverride be merged?
    private final boolean usePortForwarding;
    private final HostAndPort portForwardSshOverride;
    private final boolean isRebinding;
    private final boolean skipJcloudsSshing;


    protected ConnectivityResolverOptions(boolean isWindows, boolean waitForConnectable, boolean pollForReachableAddresses, Predicate<? super HostAndPort> reachableAddressPredicate, boolean propagatePollForReachableFailure, Duration reachableAddressTimeout, LoginCredentials initialCredentials, LoginCredentials userCredentials, int defaultLoginPort, boolean usePortForwarding, HostAndPort portForwardSshOverride, boolean isRebinding, boolean skipJcloudsSshing) {
        this.isWindows = isWindows;
        this.waitForConnectable = waitForConnectable;
        this.pollForReachableAddresses = pollForReachableAddresses;
        this.reachableAddressPredicate = reachableAddressPredicate;
        this.propagatePollForReachableFailure = propagatePollForReachableFailure;
        this.reachableAddressTimeout = reachableAddressTimeout;
        this.initialCredentials = initialCredentials;
        this.userCredentials = userCredentials;
        this.defaultLoginPort = defaultLoginPort;
        this.usePortForwarding = usePortForwarding;
        this.portForwardSshOverride = portForwardSshOverride;
        this.isRebinding = isRebinding;
        this.skipJcloudsSshing = skipJcloudsSshing;
    }

    public boolean isWindows() {
        return isWindows;
    }

    public boolean waitForConnectable() {
        return waitForConnectable;
    }

    public boolean pollForReachableAddresses() {
        return pollForReachableAddresses;
    }

    public Predicate<? super HostAndPort> reachableAddressPredicate() {
        return reachableAddressPredicate;
    }

    public Duration reachableAddressTimeout() {
        return reachableAddressTimeout;
    }

    public boolean propagatePollForReachableFailure() {
        return propagatePollForReachableFailure;
    }

    public Optional<LoginCredentials> initialCredentials() {
        return Optional.fromNullable(initialCredentials);
    }

    public Optional<LoginCredentials> userCredentials() {
        return Optional.fromNullable(userCredentials);
    }

    public boolean usePortForwarding() {
        return usePortForwarding;
    }

    public Optional<HostAndPort> portForwardSshOverride() {
        return Optional.fromNullable(portForwardSshOverride);
    }

    public int defaultLoginPort() {
        return defaultLoginPort;
    }

    public boolean skipJcloudsSshing() {
        return skipJcloudsSshing;
    }

    public boolean isRebinding() {
        return isRebinding;
    }

    public Builder toBuilder() {
        Builder builder = builder()
                .isWindows(isWindows)
                .waitForConnectable(waitForConnectable)
                .usePortForwarding(usePortForwarding)
                .portForwardSshOverride(portForwardSshOverride)
                .skipJcloudsSshing(skipJcloudsSshing)
                .initialCredentials(initialCredentials)
                .userCredentials(userCredentials)
                .isRebinding(isRebinding)
                .defaultLoginPort(defaultLoginPort)
                ;
        if (pollForReachableAddresses) {
            builder.pollForReachableAddresses(reachableAddressPredicate, reachableAddressTimeout, propagatePollForReachableFailure);
        } else {
            builder.noPollForReachableAddresses();
        }
        return builder;
    }

}
