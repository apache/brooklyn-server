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

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.util.collections.MutableSet;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;


// progressively look up and down to application (topology) boundaries

// ie first look subject to application boundaries, ie up to ancestor which is an application, and through descendants which are applications
// then look past all of those to next app boundary

// TODO would be nice to have tests which look at parent, children, grandchildren, older and younger nephews

// TODO ideally would be enhanced to look at the "depth in ancestor" tag, to work based on entity definition boundaries rather than app boundaries

/**
 *
 * Progressively expands groups based on "application" boundaries, returning each distal group.
 * Useful e.g. to find nodes matching an ID included in a particular application definition and not children.
 <code>
 - APP1
   - ENT1
   - APP2
     - ENT2
     - ENT3
   - APP3
     - ENT4
       - ENT5
       - APP4
          - ENT6
       - ENT7
     - APP5
       - ENT8
 </code>

 if this is initialized with ENT4, it will start with that in {@link #visitedThisTime} and {@link #visited}.
 one invocation of {@link #next()} will return an instance where {@link #visitedThisTime} is {APP3,ENT5,APP4,ENT7,APP5}, and {@link #visited} contains that and ENT4;
 it will not go below APP4 or APP5 because those are {@link Application} boundaries, nor will it go above APP3, on that pass.
 an invocation of {@link #next()} on that instance will then return {@link #visitedThisTime} containing {APP1,ENT1,APP2,ENT6,ENT8},
 i.e. the items above APP3 (but not above the next highest ancestor implementing {@link Application},
 and its {@link #visited} will (as it always does) contain those items plus the items previously visited.
 an invocation of {@link #next()} on that instance will then return {@link #visitedThisTime} empty.
 <p>
 see {@link #findFirstGroupOfMatches(Entity, Predicate)}.
*/
public class AppGroupTraverser {

    int depth = -1;
    Set<Entity> visited = MutableSet.of();
    Set<Entity> visitedThisTime = MutableSet.of();
    // _parent_ of the last node we have visited;
    // after first iteration this should have at least (exactly?) one visited child which is a topology template (not a node)
    Entity ancestorBound = null;
    // children whom we have not yet visited, because they are part of a new topology template
    Set<Entity> descendantBounds = MutableSet.of();

    protected AppGroupTraverser() {
    }

    AppGroupTraverser(Entity source) {
        this.visitedThisTime.add(source);
        this.visited.add(source);
        this.ancestorBound = source.getParent();
        this.descendantBounds.addAll(source.getChildren());
    }

    AppGroupTraverser next() {
        AppGroupTraverser result = new AppGroupTraverser();
        result.visited.addAll(visited);
        result.depth = depth + 1;
        descendantBounds.forEach(c -> result.visitDescendants(c, true));
        if (ancestorBound != null) result.visitAncestorsAndTheirDescendants(ancestorBound);
        return result;
    }

    protected void visitAncestorsAndTheirDescendants(Entity ancestor) {
        // go to the top of the containing topology template / application boundary
        Entity appAncestor = ancestor;
        while (!(appAncestor instanceof Application) && appAncestor.getParent() != null)
            appAncestor = appAncestor.getParent();
        visitDescendants(appAncestor, true);
        ancestorBound = appAncestor.getParent() != null ? appAncestor.getParent() : null;
    }

    protected void visitDescendants(Entity node, boolean isFirst) {
        if (!isFirst && !visited.add(node)) {
            // already visited
            return;
        }
        // normal entity
        visitedThisTime.add(node);

        if (!isFirst && node instanceof Application) {
            descendantBounds.add(node);
        } else {
            node.getChildren().forEach(c -> this.visitDescendants(c, false));
        }
    }

    boolean hasNext() {
        return ancestorBound != null || !descendantBounds.isEmpty();
    }

    AppGroupTraverser expandUntilMatchesFound(Predicate<Entity> test) {
        if (visitedThisTime.stream().anyMatch(test) || visitedThisTime.isEmpty()) return this;
        return next().expandUntilMatchesFound(test);
    }

    /** Progressively expands across {@link org.apache.brooklyn.api.entity.Application} boundaries until one or more matching entities are found. */
    public static List<Entity> findFirstGroupOfMatches(Entity source, Predicate<Entity> test) {
        AppGroupTraverser traversed = new AppGroupTraverser(source).expandUntilMatchesFound(test);
        return traversed.visitedThisTime.stream().filter(test).collect(Collectors.toList());
    }

}
