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
package org.apache.brooklyn.rest.resources;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.HasTaskChildren;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags.WrappedStream;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.rest.api.ActivityApi;
import org.apache.brooklyn.rest.domain.TaskSummary;
import org.apache.brooklyn.rest.transform.TaskTransformer;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;

import javax.ws.rs.core.Response;
import java.util.*;

public class ActivityResource extends AbstractBrooklynRestResource implements ActivityApi {

    @Override
    public Response get(String taskId, String timeout, Boolean suppressSecrets) {
        Task<?> t = findTask(taskId);

        try {
            Object result;
            if (timeout == null || timeout.isEmpty() || "always".equalsIgnoreCase(timeout)) {
                timeout = "0";
            } else if ("never".equalsIgnoreCase(timeout)) {
                timeout = "-1";
            }
            Duration timeoutD = Time.parseDuration(timeout);
            if (timeoutD.isNegative()) timeoutD = Duration.PRACTICALLY_FOREVER;

            boolean ended = t.blockUntilEnded(timeoutD);

            return Response.status(ended ? Response.Status.OK : Response.Status.ACCEPTED).entity(TaskTransformer.fromTask(ui.getBaseUriBuilder(), resolving(null), suppressSecrets).apply(t)).build();

        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }

    }

    @Override
    public Map<String, TaskSummary> getAllChildrenAsMap(final String taskId, final int limit, final int maxDepth, Boolean suppressSecrets) {
        final Task<?> parentTask = findTask(taskId);
        return getAllDescendantTasks(parentTask, limit, maxDepth, suppressSecrets);
    }

    protected Task<?> findTask(final String taskId) {
        final Task<?> task = mgmt().getExecutionManager().getTask(taskId);
        if (task == null) {
            throw WebResourceUtils.notFound("Cannot find task '%s' - possibly garbage collected to save memory", taskId);
        }
        checkEntityEntitled(task);
        return task;
    }

    private LinkedHashMap<String, TaskSummary> getAllDescendantTasks(final Task<?> parentTask, int limit, int maxDepth, Boolean suppressSecrets) {
        final LinkedHashMap<String, TaskSummary> result = Maps.newLinkedHashMap();
        if (!(parentTask instanceof HasTaskChildren)) {
            return result;
        }
        Set<Task<?>> nextLayer = MutableSet.copyOf( ((HasTaskChildren) parentTask).getChildren() );
        outer: while (limit!=0 && !nextLayer.isEmpty() && maxDepth-- != 0) {
            Set<Task<?>> thisLayer = nextLayer;
            nextLayer = MutableSet.of();
            for (final Task<?> childTask : thisLayer) {
                TaskSummary wasThere = result.put(childTask.getId(), TaskTransformer.fromTask(ui.getBaseUriBuilder(), resolving(null), suppressSecrets, false).apply(childTask));
                if (wasThere==null) {
                    if (--limit == 0) {
                        break outer;
                    }
                    if (childTask instanceof HasTaskChildren) {
                        Iterables.addAll(nextLayer, ((HasTaskChildren)childTask).getChildren());
                    }
                }
            }
        }
        return result;
    }

    @Override
    public List<TaskSummary> children(String taskId, Boolean includeBackground, int limit, Boolean suppressSecrets) {
        Task<?> t = findTask(taskId);

        List<Task<?>> result = MutableList.copyOf(getSubTaskChildren(t));
        if (Boolean.TRUE.equals(includeBackground)) {
            result.addAll(getBackgroundedChildren(t));
        }
        return TaskTransformer.fromTasks(result, limit, false, null, ui, resolving(null), suppressSecrets);
    }

    private Collection<Task<?>> getBackgroundedChildren(Task<?> t) {
        Entity entity = BrooklynTaskTags.getContextEntity(t);
        List<Task<?>> result = MutableList.of();
        if (entity!=null) {
            Set<Task<?>> tasks = BrooklynTaskTags.getTasksInEntityContext(mgmt().getExecutionManager(), entity);
            for (Task<?> ti: tasks) {
                if (t.equals(ti.getSubmittedByTask())) {
                    result.add(ti);  // TaskTransformer.fromTask(ui.getBaseUriBuilder()).apply(ti));
                }
            }
        }
        return result;
    }

    private Iterable<Task<?>> getSubTaskChildren(Task<?> t) {
        if (!(t instanceof HasTaskChildren)) {
            return Collections.emptyList();
        }
        return ((HasTaskChildren) t).getChildren();
    }

    @Override
    public String stream(String taskId, String streamId) {
        Task<?> t = findTask(taskId);
        checkStreamEntitled(t, streamId);

        WrappedStream stream = BrooklynTaskTags.stream(t, streamId);
        if (stream == null) {
            throw WebResourceUtils.notFound("Cannot find stream '%s' in task '%s'", streamId, taskId);
        }
        return stream.streamContents.get();
    }

    protected void checkEntityEntitled(Task<?> task) {
        Entity entity = BrooklynTaskTags.getContextEntity(task);
        if (entity != null && !Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see activity of entity '%s'",
                    Entitlements.getEntitlementContext().user(), entity);
        }
    }

    protected void checkStreamEntitled(Task<?> task, String streamId) {
        Entity entity = BrooklynTaskTags.getContextEntity(task);
        Entitlements.TaskAndItem<String> item = new Entitlements.TaskAndItem<String>(task, streamId);
        if (entity != null && !Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ACTIVITY_STREAMS, item)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see activity stream of entity '%s'",
                    Entitlements.getEntitlementContext().user(), entity);
        }
    }

    @Override
    public Boolean cancel(String taskId, Boolean noInterrupt) {
        Task<?> t = findTask(taskId);
        return t.cancel(noInterrupt==null || !noInterrupt);
    }

}
