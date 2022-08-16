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
package org.apache.brooklyn.camp.brooklyn.spi.dsl;

import org.apache.brooklyn.api.entity.Entity;

import java.util.List;
import java.util.function.Predicate;


@Deprecated
/** @deprecated since 1.1 use {@link org.apache.brooklyn.core.mgmt.internal.AppGroupTraverser */
public class AppGroupTraverser {

    /** Progressively expands across {@link org.apache.brooklyn.api.entity.Application} boundaries until one or more matching entities are found. */
    public static List<Entity> findFirstGroupOfMatches(Entity source, Predicate<Entity> test) {
        return org.apache.brooklyn.core.mgmt.internal.AppGroupTraverser.findFirstGroupOfMatches(source, test);
    }

}
