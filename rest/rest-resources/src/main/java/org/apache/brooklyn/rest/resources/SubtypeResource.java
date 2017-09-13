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

import java.util.List;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.rest.api.SubtypeApi;
import org.apache.brooklyn.rest.domain.TypeSummary;
import org.apache.brooklyn.rest.filter.HaHotStateRequired;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

@HaHotStateRequired
public class SubtypeResource extends AbstractBrooklynRestResource implements SubtypeApi {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(SubtypeResource.class);

    @Override
    public List<TypeSummary> list(String supertype, String versions, String regex, String fragment) {
        List<Predicate<RegisteredType>> filters = MutableList.<Predicate<RegisteredType>>of()
            .append(RegisteredTypePredicates.entitledToSee(mgmt()))
            .append(RegisteredTypePredicates.subtypeOf(supertype));
        if (TypeResource.isLatestOnly(versions, true)) {
            // TODO inefficient - does n^2 comparisons where n is sufficient
            // create RegisteredTypes.filterBestVersions to do a list after the initial parse
            // (and javadoc in predicate method below)
            filters.add(RegisteredTypePredicates.isBestVersion(mgmt()));
        }
        if (Strings.isNonEmpty(regex)) {
            filters.add(RegisteredTypePredicates.nameOrAlias(StringPredicates.containsRegex(regex)));
        }
        if (Strings.isNonEmpty(fragment)) {
            filters.add(RegisteredTypePredicates.nameOrAlias(StringPredicates.containsLiteralIgnoreCase(fragment)));
        }
        Predicate<RegisteredType> filter = Predicates.and(filters);
        ImmutableList<RegisteredType> sortedItems =
            FluentIterable.from(brooklyn().getTypeRegistry().getMatching(filter))
                .toSortedList(RegisteredTypes.RegisteredTypeNameThenBestFirstComparator.INSTANCE);
        return TypeResource.toTypeSummary(brooklyn(), sortedItems, ui.getBaseUriBuilder());
    }
    
    @Override public List<TypeSummary> listApplications(String versions, String regex, String fragment) { return list(Application.class.getName(), versions, regex, fragment); }
    @Override public List<TypeSummary> listEntities(String versions, String regex, String fragment) { return list(Entity.class.getName(), versions, regex, fragment); }
    @Override public List<TypeSummary> listPolicies(String versions, String regex, String fragment) { return list(Policy.class.getName(), versions, regex, fragment); }
    @Override public List<TypeSummary> listEnrichers(String versions, String regex, String fragment) { return list(Enricher.class.getName(), versions, regex, fragment); }
    @Override public List<TypeSummary> listLocations(String versions, String regex, String fragment) { return list(Location.class.getName(), versions, regex, fragment); }
    
}

