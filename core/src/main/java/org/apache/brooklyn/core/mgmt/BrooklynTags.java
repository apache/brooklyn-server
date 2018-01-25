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
package org.apache.brooklyn.core.mgmt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.brooklyn.util.collections.MutableList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

/** @since 0.7.0 some strongly typed tags for reference; note these may migrate elsewhere! */
@Beta
public class BrooklynTags {

    public static final String YAML_SPEC_KIND = "yaml_spec";
    public static final String NOTES_KIND = "notes";
    public static final String OWNER_ENTITY_ID = "owner_entity_id";
    public static final String ICON_URL = "icon_url";
    public static final String UPGRADED_FROM = "upgraded_from";
    /** tag on a registered type indicating that an item is intended to be used as a template,
     * and does not have to resolve */
    public static final Object CATALOG_TEMPLATE = "catalog_template";

    public static class NamedStringTag implements Serializable {
        private static final long serialVersionUID = 7932098757009051348L;
        @JsonProperty
        final String kind;
        @JsonProperty final String contents;
        public NamedStringTag(@JsonProperty("kind") String kind, @JsonProperty("contents") String contents) {
            this.kind = kind;
            this.contents = contents;
        }
        @Override
        public String toString() {
            return kind+"["+contents+"]";
        }
        
        public String getKind() {
            return kind;
        }
        public String getContents() {
            return contents;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof NamedStringTag)) {
                return false;
            }
            NamedStringTag o = (NamedStringTag) other;
            return Objects.equal(kind, o.kind) && Objects.equal(contents, o.contents);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(kind, contents);
        }
    }

    public static class ListTag<T> {
        @JsonIgnore
        final List<T> list;

        public ListTag(List<T> list) {
            this.list = list;
        }

        public List<T> getList() {
            return list;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ListTag<?> that = (ListTag<?>) o;
            return list == null ? that.list == null : list.equals(that.list);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(list);
        }
    }

    public static class TraitsTag extends ListTag<String> {
        public TraitsTag(List<Class<?>> interfaces) {
            // The transformed list is a view, meaning that it references
            // the instances list. This means that it will serialize 
            // the list of classes along with the anonymous function which
            // is not what we want. Force eager evaluation instead, using
            // a simple list, supported by {@link CatalogXmlSerializer}.
            super(new ArrayList<String>(
                Lists.transform(interfaces, new Function<Class<?>, String>() {
                    @Override public String apply(Class<?> input) {
                        return input.getName();
                    }
                })));
        }

        @JsonProperty("traits")
        public List<String> getTraits() {
            return super.list;
        }
    }

    public static NamedStringTag newYamlSpecTag(String contents) {
        return new NamedStringTag(YAML_SPEC_KIND, contents);
    }

    public static NamedStringTag newNotesTag(String contents) {
        return new NamedStringTag(NOTES_KIND, contents);
    }
    
    public static NamedStringTag newOwnerEntityTag(String ownerId) {
        return new NamedStringTag(OWNER_ENTITY_ID, ownerId);
    }

    public static NamedStringTag newIconUrlTag(String iconUrl) {
        return new NamedStringTag(ICON_URL, iconUrl);
    }

    public static NamedStringTag newUpgradedFromTag(String oldVersion) {
        return new NamedStringTag(UPGRADED_FROM, oldVersion);
    }

    public static TraitsTag newTraitsTag(List<Class<?>> interfaces) {
        return new TraitsTag(interfaces);
    }

    public static NamedStringTag findFirst(String kind, Iterable<Object> tags) {
        for (Object object: tags) {
            if (object instanceof NamedStringTag && kind.equals(((NamedStringTag)object).kind))
                return (NamedStringTag) object;
        }
        return null;
    }

    public static List<NamedStringTag> findAll(String kind, Iterable<Object> tags) {
        List<NamedStringTag> result = MutableList.of();
        for (Object object: tags) {
            if (object instanceof NamedStringTag && kind.equals(((NamedStringTag)object).kind))
                result.add( (NamedStringTag) object );
        }
        return result;
    }

}
