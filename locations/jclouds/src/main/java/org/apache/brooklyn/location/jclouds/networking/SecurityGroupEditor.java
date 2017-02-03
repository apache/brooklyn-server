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
package org.apache.brooklyn.location.jclouds.networking;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.jclouds.aws.AWSResponseException;
import org.jclouds.compute.domain.SecurityGroup;
import org.jclouds.compute.extensions.SecurityGroupExtension;
import org.jclouds.domain.Location;
import org.jclouds.net.domain.IpPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utility for manipulating security groups.
 */
public class SecurityGroupEditor {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityGroupEditor.class);
    public static final java.lang.String JCLOUDS_PREFIX_REGEX = "^jclouds[#-]";

    private final Location location;
    private final SecurityGroupExtension securityApi;
    private final Predicate<Exception> isExceptionRetryable;

    /**
     * Constructor for editor that never retries requests if the attempted operation fails.
     * @param location JClouds location where security groups will be managed.
     * @param securityGroupExtension The JClouds security group extension from the compute service for this location.
     */
    public SecurityGroupEditor(Location location, SecurityGroupExtension securityGroupExtension) {
        this.location = checkNotNull(location, "location");
        this.securityApi = checkNotNull(securityGroupExtension, "securityGroupExtension");
        this.isExceptionRetryable = Predicates.alwaysFalse();
    }

    /**
     * Constructor for editor that may retry operations upon exceptions.
     * @deprecated since 0.10.0 Ideally it should be possible to determine a suitable predicate internally to
     * this class by instantiating a type appropriate to the underlying cloud. TODO investigate and implement.
     *
     * @param location JClouds location where security groups will be managed.
     * @param securityGroupExtension The JClouds security group extension from the compute service for this location.
     * @param isExceptionRetryable used to determine for an exception whether to retry the operation that failed.
     */
    @Deprecated
    public SecurityGroupEditor(Location location, SecurityGroupExtension securityGroupExtension,
            Predicate<Exception> isExceptionRetryable) {
        this.location = checkNotNull(location, "location");
        this.securityApi = checkNotNull(securityGroupExtension, "securityGroupExtension");
        this.isExceptionRetryable = isExceptionRetryable;
    }


    /**
     * Get the location in which security groups will be created or searched.
     */
    public Location getLocation() {
        return location;
    }

    public Set<SecurityGroup> getSecurityGroupsForNode(final String nodeId) {
        return securityApi.listSecurityGroupsForNode(nodeId);
    }

    /**
     * Create the security group. As we use jclouds, groups are created with names prefixed
     * with {@link #JCLOUDS_PREFIX}. This method is idempotent.
     * @param name Name of the group to create
     * @return The created group.
     */
    public SecurityGroup createSecurityGroup(final String name) {

        LOG.debug("Creating security group {} in {}", name, location);
        Callable<SecurityGroup> callable = new Callable<SecurityGroup>() {
            @Override
            public SecurityGroup call() throws Exception {
                return securityApi.createSecurityGroup(name, location);
            }

            @Override
            public String toString() {
                return "Create security group " + name;
            }
        };
        return runOperationWithRetry(callable);
    }

    /**
     * Removes a security group and its permissions.
     * @param group The security group.
     * @return true if the group was found and removed.
     */
    public boolean removeSecurityGroup(final SecurityGroup group) {

        LOG.debug("Removing security group {} in {}", group.getName(), location);
        Callable<Boolean> removeIt = new RemoveSecurityGroup(group.getId());
        return runOperationWithRetry(removeIt);
    }
    /**
     * Removes a security group and its permissions.
     * @param groupId The jclouds id (provider id) of the group (including region code)
     * @return true if the group was found and removed.
     */
    public boolean removeSecurityGroup(final String groupId) {

        LOG.debug("Removing security group {} in {}", groupId, location);
        Callable<Boolean> removeIt = new RemoveSecurityGroup(groupId);
        return runOperationWithRetry(removeIt);
    }

    public Set<SecurityGroup> listSecurityGroupsForNode(final String nodeId) {
        return securityApi.listSecurityGroupsForNode(nodeId);
    }

    public Iterable<SecurityGroup> findSecurityGroupsMatching(Predicate predicate) {
        final Set<SecurityGroup> locationGroups = securityApi.listSecurityGroupsInLocation(location);
        return Iterables.filter(locationGroups, predicate);
    }

    /**
     * @see #findSecurityGroupByName(String)
     */
    public static class AmbiguousGroupName extends IllegalArgumentException {
        public AmbiguousGroupName(String s) {
            super(s);
        }
    }

    /**
     * Find a security group with the given name. As we use jclouds, groups are created with names prefixed
     * with {@link #JCLOUDS_PREFIX}. For convenience this method accepts names either with or without the prefix.
     * @param name Name of the group to find.
     * @return An optional of the group.
     * @throws AmbiguousGroupName in the unexpected case that the cloud returns more than one matching group.
     */
    public Optional<SecurityGroup> findSecurityGroupByName(final String name) {
        final Iterable<SecurityGroup> groupsMatching = findSecurityGroupsMatching(new Predicate<SecurityGroup>() {
            final String rawName = name.replaceAll(JCLOUDS_PREFIX_REGEX, "");
            @Override
            public boolean apply(final SecurityGroup input) {
                return input.getName().replaceAll(JCLOUDS_PREFIX_REGEX, "").equals(rawName);
            }
        });
        final ImmutableList<SecurityGroup> matches = ImmutableList.copyOf(groupsMatching);
        if (matches.size() == 0) {
            return Optional.absent();
        } else if (matches.size() == 1) {
            return Optional.of(matches.get(0));
        } else {
            throw new AmbiguousGroupName("Unexpected result of multiple groups matching " + name);
        }
    }

    /**
     * Add permissions to the security group, using {@link #addPermission(SecurityGroup, IpPermission)}.
     * @param group The group to update
     * @param permissions The new permissions
     * @return The updated group with the added permissions.
     */
    public SecurityGroup addPermissions(final SecurityGroup group, final Iterable<IpPermission> permissions) {
        SecurityGroup lastGroup = group;
        for (IpPermission permission : permissions) {
            lastGroup = addPermission(group, permission);
        }
        return lastGroup;
    }

    /**
     * Add a permission to the security group. This operation is idempotent (will return the group unmodified if the
     * permission already exists on it).
     * @param group The group to update
     * @param permissions The new permissions
     * @return The updated group with the added permissions.
     */
    public SecurityGroup addPermission(final SecurityGroup group, final IpPermission permission) {
        LOG.debug("Adding permission to security group {}: {}", group.getName(), permission);
        Callable<SecurityGroup> callable = new Callable<SecurityGroup>() {
            @Override
            public SecurityGroup call() throws Exception {
                try {
                    return securityApi.addIpPermission(permission, group);
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);

                    if (isDuplicate(e)) {
                        return group;
                    }

                    throw Exceptions.propagate(e);
                }
            }

            @Override
            public String toString() {
                return "Add permission " + permission + " to security group " + group;
            }
        };
        return runOperationWithRetry(callable);
    }

    @Deprecated // TODO improve this - shouldn't have AWS specifics in here
    private boolean isDuplicate(Exception e) {
        // Sometimes AWSResponseException is wrapped in an IllegalStateException
        AWSResponseException cause = Exceptions.getFirstThrowableOfType(e, AWSResponseException.class);
        if (cause != null) {
            if ("InvalidPermission.Duplicate".equals(cause.getError().getCode())) {
                return true;
            }
        }

        if (e.toString().contains("already exists")) {
            return true;
        }

        return false;
    }


    public SecurityGroup removePermission(final SecurityGroup group, final IpPermission permission) {
        LOG.debug("Removing permission from security group {}: {}", group.getName(), permission);
        Callable<SecurityGroup> callable = new Callable<SecurityGroup>() {
            @Override
            public SecurityGroup call() throws Exception {
                return securityApi.removeIpPermission(permission, group);
            }

            @Override
            public String toString() {
                return "Remove permission " + permission + " from security group " + group;
            }
        };
        return runOperationWithRetry(callable);
    }

    public SecurityGroup removePermissions(SecurityGroup group, final Iterable<IpPermission> permissions) {
        for (IpPermission permission : permissions) {
            group = removePermission(group, permission);
        }
        return group;
    }

    /**
     * Runs the given callable. Repeats until the operation succeeds or {@link #isExceptionRetryable} indicates
     * that the request cannot be retried.
     */
    protected <T> T runOperationWithRetry(Callable<T> operation) {
        int backoff = 64;
        Exception lastException = null;
        LOG.debug("Running operation {}", operation);
        for (int retries = 0; retries < 12; retries++) { // 12 = keep trying for about 5 minutes
            try {
                return operation.call();
            } catch (Exception e) {
                lastException = e;
                if (isExceptionRetryable.apply(e)) {
                    LOG.debug("Attempt #{} failed to run operation, due to: {}", retries + 1, e.getMessage());
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException e1) {
                        throw Exceptions.propagate(e1);
                    }
                    backoff = backoff << 1;
                } else {
                    break;
                }
            }
        }

        throw new RuntimeException("Unable to run operation '" + operation + "'; repeated errors from provider",
            lastException);
    }

    @Override
    public String toString() {
        return "JcloudsLocationSecurityGroupEditor{" +
            "location=" + location +
            ", securityApi=" + securityApi +
            ", isExceptionRetryable=" + isExceptionRetryable +
            '}';
    }

    private class RemoveSecurityGroup implements Callable<Boolean> {
        private String groupId;
        public RemoveSecurityGroup(final String groupId) {
            this.groupId = groupId;
        }
        @Override
        public Boolean call() throws Exception {
            return securityApi.removeSecurityGroup(groupId);
        }

        @Override
        public String toString() {
            return "Remove security group " + groupId;
        }
    }
}
