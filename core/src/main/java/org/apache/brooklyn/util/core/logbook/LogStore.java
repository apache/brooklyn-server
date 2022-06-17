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
package org.apache.brooklyn.util.core.logbook;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.brooklyn.api.mgmt.HasTaskChildren;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.util.collections.MutableSet;

public interface LogStore {
    /**
     * Expects a set of parameters {@link LogBookQueryParams} to query to the implemented logstore.
     *
     * @param query Depending on the implementation some of fields could be mandatory
     * @return List of the log entries modeled
     * @throws IOException
     */
    List<BrooklynLogEntry> query(LogBookQueryParams query) throws IOException;

    /** Breadth-first recursive enumeration of tasks up to {@code maxTasks} */
    default Set<String> enumerateTaskIds(Task<?> parent, int maxTasks) {
        Set<Task<?>> tasks = MutableSet.of(), current = MutableSet.of(parent), children;

        enumerate: do {
            children = MutableSet.of();
            for (Task<?> task : current) {
                if (task instanceof HasTaskChildren) {
                    Iterables.addAll(children, ((HasTaskChildren) task).getChildren());
                    Iterables.addAll(tasks, Iterables.limit(children, maxTasks - tasks.size()));

                    if (tasks.size() == maxTasks) {
                        break enumerate; // Limit reached
                    }
                }
            }
            current = children;
        } while (children.size() > 0);

        // Collect and return only the task ids
        if (tasks.size() > 0) {
            return tasks.stream()
                    .map(Task::getId)
                    .collect(Collectors.toSet());
        } else return ImmutableSet.of();
    }
}
