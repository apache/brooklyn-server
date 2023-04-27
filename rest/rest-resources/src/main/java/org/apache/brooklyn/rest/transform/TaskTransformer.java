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
package org.apache.brooklyn.rest.transform;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.HasTaskChildren;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags.WrappedStream;
import org.apache.brooklyn.rest.api.ActivityApi;
import org.apache.brooklyn.rest.api.EntityApi;
import org.apache.brooklyn.rest.domain.LinkWithMetadata;
import org.apache.brooklyn.rest.domain.TaskSummary;
import org.apache.brooklyn.rest.resources.AbstractBrooklynRestResource;
import org.apache.brooklyn.rest.resources.EntityResource.InterestingTasksFirstComparator;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.TaskInternal;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.brooklyn.rest.util.WebResourceUtils.serviceUriBuilder;

public class TaskTransformer {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TaskTransformer.class);

    public static final Function<Task<?>, TaskSummary> fromTask(final UriBuilder ub, AbstractBrooklynRestResource.RestValueResolver resolver, Boolean suppressSecrets) {
        return fromTask(ub,resolver,suppressSecrets,true);
    };
    public static final Function<Task<?>, TaskSummary> fromTask(final UriBuilder ub, AbstractBrooklynRestResource.RestValueResolver resolver, Boolean suppressSecrets, boolean includeDetail) {
        return new Function<Task<?>, TaskSummary>() {
            @Override
            public TaskSummary apply(@Nullable Task<?> input) {
                return taskSummary(input, ub, resolver, suppressSecrets, includeDetail);
            }
        };
    };
    public static TaskSummary taskSummary(Task<?> task, UriBuilder ub, AbstractBrooklynRestResource.RestValueResolver resolver, Boolean suppressSecrets) {
        return taskSummary(task, ub, resolver, suppressSecrets, true);
    }
    public static TaskSummary taskSummary(Task<?> task, UriBuilder ub, AbstractBrooklynRestResource.RestValueResolver resolver, Boolean suppressSecrets, boolean includeDetail) {
      try {
        Preconditions.checkNotNull(task);
        Entity entity = BrooklynTaskTags.getContextEntity(task);
        String entityId;
        String entityDisplayName;
        URI entityLink;
        
        String selfLink = asLink(task, ub).getLink();

        if (entity != null) {
            entityId = entity.getId();
            entityDisplayName = entity.getDisplayName();
            String appId = entity.getApplicationId();
            entityLink = (appId != null) ? serviceUriBuilder(ub, EntityApi.class, "get").build(appId, entityId) : null;
        } else {
            entityId = null;
            entityDisplayName = null;
            entityLink = null;
        }

        List<LinkWithMetadata> children = Collections.emptyList();
        if (task instanceof HasTaskChildren) {
            children = new ArrayList<LinkWithMetadata>();
            for (Task<?> t: ((HasTaskChildren)task).getChildren()) {
                children.add(asLink(t, ub));
            }
        }
        
        Map<String,LinkWithMetadata> streams = new MutableMap<String, LinkWithMetadata>();
        for (WrappedStream stream: BrooklynTaskTags.streams(task)) {
            MutableMap<String, Object> metadata = MutableMap.<String,Object>of("name", stream.streamType);
            if (stream.streamSize.get()!=null) {
                metadata.add("size", stream.streamSize.get());
                metadata.add("sizeText", Strings.makeSizeString(stream.streamSize.get()));
            }
            String link = selfLink + "/stream/" + StringEscapes.escapeHtmlFormUrl(stream.streamType).replaceAll("\\+", "%20");
            streams.put(stream.streamType, new LinkWithMetadata(link, metadata));
        }
        
        Map<String,URI> links = MutableMap.of("self", new URI(selfLink),
                "children", new URI(selfLink+"/"+"children"));
        if (entityLink!=null) links.put("entity", entityLink);

        Collection<Object> tags = (Collection<Object>) resolver.getValueForDisplay(task.getTags(), true, false, suppressSecrets);

        Object result = null;
        String detailedStatus = null;

        if (includeDetail) {
            try {
                detailedStatus = (String) resolver.getValueForDisplay(task.getStatusDetail(true), true, false, suppressSecrets);
                if (task.isDone()) {
                    result = resolver.getValueForDisplay(task.get(), true, false, suppressSecrets);
                } else {
                    result = null;
                }
            } catch (Throwable t) {
                result = Exceptions.collapseTextInContext(t, task);
            }
        }


        return new TaskSummary(task.getId(), task.getDisplayName(), task.getDescription(), entityId, entityDisplayName, 
                tags, ifPositive(task.getSubmitTimeUtc()), ifPositive(task.getStartTimeUtc()), ifPositive(task.getEndTimeUtc()),
                task.getStatusSummary(), result, task.isError(), task.isCancelled(),
                children, asLink(task.getSubmittedByTask(), ub),
                task.isDone() ? null : task instanceof TaskInternal ? asLink(((TaskInternal<?>)task).getBlockingTask(), ub) : null,
                task.isDone() ? null : task instanceof TaskInternal ? ((TaskInternal<?>)task).getBlockingDetails() : null, 
                detailedStatus,
                streams,
                links);
      } catch (URISyntaxException e) {
          // shouldn't happen
          throw Exceptions.propagate(e);
      }
    }

    private static Long ifPositive(Long time) {
        if (time==null || time<=0) return null;
        return time;
    }

    public static LinkWithMetadata asLink(Task<?> t, UriBuilder ub) {
        if (t==null) return null;
        MutableMap<String,Object> data = new MutableMap<String,Object>();
        data.put("id", t.getId());
        if (t.getDisplayName()!=null) data.put("taskName", t.getDisplayName());
        Entity entity = BrooklynTaskTags.getContextEntity(t);
        if (entity!=null) {
            data.put("entityId", entity.getId());
            if (entity.getDisplayName()!=null) data.put("entityDisplayName", entity.getDisplayName());
        }
        URI taskUri = serviceUriBuilder(ub, ActivityApi.class, "get").build(t.getId());
        return new LinkWithMetadata(taskUri.toString(), data);
    }
    public enum TaskSummaryMode{
        ALL, NONE, SUBSET
    }
    public static List<TaskSummary> fromTasks(List<Task<?>> tasksToScan, int limit, Boolean recurse, @Nullable Entity entity, UriInfo ui, AbstractBrooklynRestResource.RestValueResolver resolver, Boolean suppressSecrets) {
        return fromTasks(tasksToScan, limit, recurse, entity, ui, resolver, suppressSecrets, TaskSummaryMode.ALL);
    }
    public static List<TaskSummary> fromTasks(List<Task<?>> tasksToScan, int limit, Boolean recurse, @Nullable Entity entity, UriInfo ui, AbstractBrooklynRestResource.RestValueResolver resolver, Boolean suppressSecrets, TaskSummaryMode taskSummaryMode) {
        int sizeRemaining = limit;
        InterestingTasksFirstComparator comparator = new InterestingTasksFirstComparator(entity);
        if (limit>0 && tasksToScan.size() > limit) {
            Map<String,Task<?>> mostRecentByName = MutableMap.of();
            List<Task<?>> otherTasks = MutableList.of();

            tasksToScan.forEach(t -> {
                String name = t.getDisplayName();
                if (name==null) name = "";
                Task<?> mostRecentOfSameName = mostRecentByName.put(name, t);
                if (mostRecentOfSameName!=null) {
                    if (comparator.compare(mostRecentOfSameName, t) < 0) {
                        // prefer the mostRecentOfSameName; put it back
                        mostRecentByName.put(name, mostRecentOfSameName);
                        otherTasks.add(t);
                    } else {
                        otherTasks.add(mostRecentOfSameName);
                    }
                }
            });

            if (mostRecentByName.size()<=limit) {
                // take at least one of each name
                tasksToScan = MutableList.copyOf(mostRecentByName.values());
                // fill the remaining with the most interesting
                tasksToScan.addAll(Ordering.from(comparator).leastOf(otherTasks, limit - mostRecentByName.size()));

            } else {
                // even taking distinct there are a lot, so apply limit to the distinct ones
                tasksToScan = MutableList.copyOf(Ordering.from(comparator).leastOf(mostRecentByName.values(), limit));
            }
        }

        tasksToScan = MutableList.copyOf(Ordering.from(comparator).sortedCopy(tasksToScan));

        Map<String,Task<?>> tasksLoaded = MutableMap.of();

        boolean isRecurse = Boolean.TRUE.equals(recurse);
        while (!tasksToScan.isEmpty()) {
            Task<?> t = tasksToScan.remove(0);
            if (tasksLoaded.put(t.getId(), t)==null) {
                if (--sizeRemaining==0) {
                    break;
                }
                if (isRecurse) {
                    if (t instanceof HasTaskChildren) {
                        Iterables.addAll(tasksToScan, ((HasTaskChildren) t).getChildren() );
                    }
                }
            }
        }
        List<Task<?>> finalTasksToScan = ImmutableList.copyOf(tasksToScan);
        return tasksLoaded.values().stream().map(task->{
            boolean includeDetail = (taskSummaryMode == TaskSummaryMode.ALL && !isRecurse) || (taskSummaryMode == TaskSummaryMode.SUBSET && finalTasksToScan.contains(task));
            Function<Task<?>, TaskSummary> taskTaskSummaryFunction = TaskTransformer.fromTask(ui.getBaseUriBuilder(), resolver, suppressSecrets, includeDetail);
            return taskTaskSummaryFunction.apply(task);
        }).collect(Collectors.toList());
    }
    public static Object suppressWorkflowOutputs(Object x) {
        if (x instanceof Map) {
            Map y = MutableMap.of();
            ((Map)x).forEach((k,v) -> {
                y.put(k, v!=null && TaskTransformer.IS_OUTPUT.apply(k) ? "(output suppressed)": suppressWorkflowOutputs(v) );
            });
            return y;
        }else if (x instanceof Iterable){
            List y = MutableList.of();
            ((Iterable)x).forEach(xi -> y.add(suppressWorkflowOutputs(xi)));
            return y;
        }else {
            return x;
        }
    }

    public static final Predicate<Object> IS_OUTPUT = new IsOutputPredicate();

    private static class IsOutputPredicate implements Predicate<Object> {
        @Override
        public boolean apply(Object name) {
            if (name == null) return false;
            String lowerName = name.toString().toLowerCase();
            for (String outputFieldName : ImmutableList.of("output", "stdout", "stderr")) {
                if (lowerName.contains(outputFieldName))
                    return true;
            }
            return false;
        }
    }

}
