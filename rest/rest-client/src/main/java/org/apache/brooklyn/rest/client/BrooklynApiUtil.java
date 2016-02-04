/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.brooklyn.rest.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.rest.api.EffectorApi;
import org.apache.brooklyn.rest.domain.Status;
import org.apache.brooklyn.rest.domain.TaskSummary;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class BrooklynApiUtil {

    private static final Logger LOG = LoggerFactory.getLogger(BrooklynApiUtil.class);
    private static final Duration DEFAULT_POLL_PERIOD = Duration.FIVE_SECONDS;
    private static final Duration DEFAULT_TIMEOUT = Duration.FIVE_MINUTES;

    private BrooklynApiUtil() {}

    /**
     * Deploys the blueprint and returns the task summary.
     * @throws Exception If the response from the server when deploying was {@link #isUnhealthyResponse unhealthy}.
     */
    public static TaskSummary deployBlueprint(BrooklynApi api, String blueprint) throws Exception {
        Response r = api.getApplicationApi().createFromYaml(blueprint);
        if (isUnhealthyResponse(r)) {
            throw new Exception("Unexpected response deploying blueprint to server: " + r.getStatus());
        } else {
            LOG.debug("Server response to deploy blueprint: " + r.getStatus());
        }
        return BrooklynApi.getEntity(r, TaskSummary.class);
    }

    /**
     * Waits for the application with the given ID to be running.
     *
     * @throws IllegalStateException If the application was not running after {@link #DEFAULT_TIMEOUT}.
     */
    public static void waitForRunningAndThrowOtherwise(BrooklynApi api, String applicationId, String taskId) throws IllegalStateException {
        waitForRunningAndThrowOtherwise(api, applicationId, taskId, DEFAULT_TIMEOUT);
    }

    /**
     * Waits for the application with the given ID to be running.
     *
     * @throws IllegalStateException If the application was not running after the given timeout.
     */
    public static void waitForRunningAndThrowOtherwise(BrooklynApi api, String applicationId, String taskId, Duration timeout) throws IllegalStateException {
        Status finalStatus = waitForAppStatus(api, applicationId, Status.RUNNING, timeout, DEFAULT_POLL_PERIOD);
        if (!Status.RUNNING.equals(finalStatus)) {
            LOG.error("Application is not running. Is: " + finalStatus.name().toLowerCase());

            StringBuilder message = new StringBuilder();
            message.append("Application ").append(applicationId)
                    .append(" should be running but is ").append(finalStatus.name().toLowerCase())
                    .append(". ");

            if (Status.ERROR.equals(finalStatus) || Status.UNKNOWN.equals(finalStatus)) {
                String result = getTaskResult(api, taskId);
                message.append("\nThe result of the task on the server was:\n")
                        .append(result);
            }
            throw new IllegalStateException(message.toString());
        }
    }

    /**
     * Polls Brooklyn until the given application has the given status. Quits early if the
     * application's status is {@link org.apache.brooklyn.rest.domain.Status#ERROR} or
     * {@link org.apache.brooklyn.rest.domain.Status#UNKNOWN} and desiredStatus is something else.
     *
     * @return The final polled status.
     */
    public static Status waitForAppStatus(final BrooklynApi api, final String application, final Status desiredStatus,
            Duration timeout, Duration pollPeriod) {
        final AtomicReference<Status> appStatus = new AtomicReference<>(Status.UNKNOWN);
        final boolean shortcutOnError = !Status.ERROR.equals(desiredStatus) && !Status.UNKNOWN.equals(desiredStatus);
        LOG.info("Waiting " + timeout + " from " + new Date() + " for application " + application + " to be " + desiredStatus);
        boolean finalAppStatusKnown = Repeater.create("Waiting for application " + application + " status to be " + desiredStatus)
                .every(pollPeriod)
                .limitTimeTo(timeout)
                .rethrowExceptionImmediately()
                .until(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        Status status = api.getApplicationApi().get(application).getStatus();
                        LOG.debug("Application " + application + " status is: " + status);
                        appStatus.set(status);
                        return desiredStatus.equals(status) || (shortcutOnError &&
                                (Status.ERROR.equals(status) || Status.UNKNOWN.equals(status)));
                    }
                })
                .run();
        if (appStatus.get().equals(desiredStatus)) {
            LOG.info("Application " + application + " is " + desiredStatus.name());
        } else {
            LOG.warn("Application is not " + desiredStatus.name() + " within " + timeout +
                    ". Status is: " + appStatus.get());
        }
        return appStatus.get();
    }

    /**
     * Use the {@link EffectorApi effector API} to invoke the stop effector on the given application.
     */
    public static void attemptStop(BrooklynApi api, String application, Duration timeout) {
        api.getEffectorApi().invoke(application, application, "stop", String.valueOf(timeout.toMilliseconds()),
                ImmutableMap.<String, Object>of());
    }

    /**
     * @return The result of the task with the given id, or "unknown" if it could not be found.
     */
    public static String getTaskResult(BrooklynApi api, String taskId) {
        checkNotNull(taskId, "taskId");
        TaskSummary summary = api.getActivityApi().get(taskId);
        return summary == null || summary.getResult() == null ? "unknown" : summary.getResult().toString();
    }

    /**
     * @return true if response's status code is not between 200 and 299 inclusive.
     */
    public static boolean isUnhealthyResponse(Response response) {
        return response.getStatus() < 200 || response.getStatus() >= 300;
    }

}
