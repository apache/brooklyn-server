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
package org.apache.brooklyn.rest.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.relations.RelationshipType;
import org.apache.brooklyn.rest.domain.RelationSummary;
import org.apache.brooklyn.rest.domain.RelationType;

import java.util.List;
import java.util.Set;

public class EntityRelationUtils {

    public static List<RelationSummary> getRelations(final Entity entity) {
        List<RelationSummary> entityRelations = Lists.newArrayList();
        for (RelationshipType<?,? extends BrooklynObject> relationship: entity.relations().getRelationshipTypes()) {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                Set relations = entity.relations().getRelations((RelationshipType) relationship);
                Set<String> relationIds = Sets.newLinkedHashSet();
                for (Object r: relations) {
                    relationIds.add(((BrooklynObject) r).getId());
                }
                RelationType relationType = new RelationType(relationship.getRelationshipTypeName(), relationship.getTargetName(), relationship.getSourceName());
                RelationSummary relationSummary = new RelationSummary(relationType, relationIds);
                entityRelations.add(relationSummary);
        }
        return entityRelations;
    }
}
