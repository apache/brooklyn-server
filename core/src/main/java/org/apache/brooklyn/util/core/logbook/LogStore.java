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

import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.mgmt.HasTaskChildren;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.util.collections.MutableSet;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface LogStore {
    /**
     * Expects a set of parameters {@link LogBookQueryParams} to query to the implemented logstore.
     *
     * @param query Depending on the implementation some of fields could be mandatory
     * @return List of the log entries modeled
     * @throws IOException
     */
    List<BrooklynLogEntry> query(LogBookQueryParams query) throws IOException;

    /** Find tasks descended from the given tasks or task IDs, up to {@code maxTasks} */
    Set<String> enumerateTaskIds(Set<?> parents, int maxTasks);

    /** Breadth-first recursive enumeration of tasks up to {@code maxTasks} */
    static Set<String> enumerateTaskIdsDefault(ManagementContext mgmt, Set<?> parentTaskOrIds, int maxTasks) {
        Set<String> tasks = MutableSet.of();
        Set<Task<?>> current = MutableSet.of(), children;

        if (parentTaskOrIds!=null) {
            for (Object t : parentTaskOrIds) {
                if (t instanceof String) { t = mgmt.getExecutionManager().getTask((String) t); }

                if (t instanceof Task) current.add((Task) t);
                else if (t == null) {}
                else throw new IllegalArgumentException("Can only enumerate given task ID or string");
            }
        }

        enumerate: do {
            children = MutableSet.of();
            for (Task<?> task : current) {
                if (task instanceof HasTaskChildren) {
                    Iterables.addAll(children, ((HasTaskChildren) task).getChildren());
                    for (Task<?> child: children) {
                        if (tasks.add(child.getId())) {
                            if (tasks.size() >= maxTasks) {
                                break enumerate; // Limit reached
                            }
                        }
                    }
                }
            }
            current = children;
        } while (children.size() > 0);

        // Collect and return only the task ids
        return tasks;
    }

}
