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

import com.google.common.reflect.TypeToken;
import java.io.Serializable;
import java.util.*;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.util.collections.MutableList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @since 0.7.0 some strongly typed tags for reference; note these may migrate elsewhere! */
@Beta
public class BrooklynTags {

    private static final Logger LOG = LoggerFactory.getLogger(BrooklynTags.class);

    // could deprecate this in favour of spec_hierarchy
    public static final String YAML_SPEC_KIND = "yaml_spec";

    /** used as a single-key map key whose value is a list of {@link SpecSummary} maps, first one being the original source, then going in to conversions/definitions */
    public static final String SPEC_HIERARCHY = "spec_hierarchy";

    /** used as a single-key map key whose value is a number 1 or more, indicating how many ancestors up need to be traversed
     * to find where a particular entity was defined */
    public static final String DEPTH_IN_ANCESTOR = "depth_in_ancestor";

    public static final String NOTES_KIND = "notes";
    public static final String OWNER_ENTITY_ID = "owner_entity_id";
    public static final String ICON_URL = "icon_url";
    public static final String UPGRADED_FROM = "upgraded_from";
    /** tag on a registered type indicating that an item is intended to be used as a template,
     * and does not have to resolve */
    public static final Object CATALOG_TEMPLATE = "catalog_template";

    /** find a tag which is a map of size one whose single key matches the key here, and if found return the value
     * coerced to the indicated type */
    public static <T> T findSingleKeyMapValue(String key, TypeToken<T> type, Iterable<Object> tags) {
        if (tags==null) return null;
        for (Object tag: tags) {
            if (isTagSingleKeyMap(tag, key)) {
                java.lang.Object value = ((Map)tag).get(key);
                return TypeCoercions.coerce(value, type);
            }
        }
        return null;
    }

    private static <Object> boolean isTagSingleKeyMap(Object tag, String key) {
        return tag instanceof Map && ((Map) tag).size() == 1 && Objects.equal(key, ((Map) tag).keySet().iterator().next());
    }

    /** convenience for {@link #findSingleKeyMapValue(String, TypeToken, Iterable)} */
    public static <T> T findSingleKeyMapValue(String key, Class<T> type, Iterable<Object> tags) {
        return findSingleKeyMapValue(key, TypeToken.of(type), tags);
    }

    public static <T> void upsertSingleKeyMapValueTag(AbstractBrooklynObjectSpec<?, ?> spec, String key, T value) {
        MutableList<Object> tags = MutableList.copyOf(spec.getTags());
        AtomicInteger count = new AtomicInteger();
        List<Object> newTags = tags.stream().map(t -> {
            if (isTagSingleKeyMap(t, key)) {
                count.incrementAndGet();
                return MutableMap.of(key, value);
            } else {
                return t;
            }
        }).collect(Collectors.toList());
        if (count.get()>0) {
            spec.tagsReplace(newTags);
        } else {
            spec.tag(MutableMap.of(key, value));
        }
    }

    public static NamedStringTag findFirstNamedStringTag(String kind, Iterable<Object> tags) {
        return findFirstOfKind(kind, NamedStringTag.class, tags);
    }

    public static List<NamedStringTag> findAllNamedStringTags(String kind, Iterable<Object> tags) {
        return findAllOfKind(kind, NamedStringTag.class, tags);
    }

    /** @deprecated since 1.1 use {@link #findFirstNamedStringTag(String, Iterable)} */
    @Deprecated
    public static NamedStringTag findFirst(String kind, Iterable<Object> tags) {
        return findFirstNamedStringTag(kind, tags);
    }

    /** @deprecated since 1.1 use {@link #findAllNamedStringTags(String, Iterable)} */
    @Deprecated
    public static List<NamedStringTag> findAll(String kind, Iterable<Object> tags) {
        return findAllNamedStringTags(kind, tags);
    }

    public interface HasKind {
        public String getKind();
    }

    public static <T extends HasKind> T findFirstOfKind(String kind, Class<T> type, Iterable<Object> tags) {
        for (Object o: tags) {
            if (type.isInstance(o)) {
                if (kind.equals(((T) o).getKind())) {
                    return (T) o;
                }
            } else if (o instanceof Map) {
                Object k2 = ((Map) o).get("kind");
                if (kind.equals(k2)) {
                    try {
                        return BeanWithTypeUtils.newMapper(null, false, null, true).convertValue(o, type);

                    } catch (Exception e) {
                        Exceptions.propagateIfFatal(e);
                        LOG.warn("Tag '"+o+"' declares kind '"+k2+"' but does not convert to "+type+" (ignoring): "+e);
                    }
                }
            }
        }
        return null;
    }

    public static <T extends HasKind> List<T> findAllOfKind(String kind, Class<T> type, Iterable<Object> tags) {
        List<T> result = MutableList.of();

        for (Object o: tags) {
            if (type.isInstance(o)) {
                if (kind.equals(((T) o).getKind())) {
                    result.add( (T)o );
                }
            } else if (o instanceof Map) {
                Object k2 = ((Map) o).get("kind");
                if (kind.equals(k2)) {
                    try {
                        result.add( BeanWithTypeUtils.newMapper(null, false, null, true).convertValue(o, type) );

                    } catch (Exception e) {
                        Exceptions.propagateIfFatal(e);
                        LOG.warn("Tag '"+o+"' declares kind '"+k2+"' but does not convert to "+type+" (ignoring): "+e);
                    }
                }
            }
        }

        return result;
    }

    public static class NamedStringTag implements Serializable, HasKind {
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

    public static class SpecSummary implements Serializable {
        @JsonProperty
        public final String summary;
        @JsonProperty
        public final String format;
        @JsonProperty
        public final Object contents;

        private SpecSummary() { this(null, null, null); }; //for JSON
        public SpecSummary(String summary, String format, Object contents) {
            this.summary = summary;
            this.format = format;
            this.contents = contents;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SpecSummary that = (SpecSummary) o;
            return java.util.Objects.equals(summary, that.summary) && java.util.Objects.equals(format, that.format) && java.util.Objects.equals(contents, that.contents);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(summary, format, contents);
        }

        @Override
        public String toString() {
            return "SpecSummary{" +
                    "summary='" + summary + '\'' +
                    ", format='" + format + '\'' +
                    ", contents=" + contents +
                    '}';
        }

        public static Builder builder() { return new Builder(); }
        public static Builder builder(SpecSummary base) { return new Builder(base); }

        public static class Builder {
            private String summary;
            private String format;
            private Object contents;

            private Builder() {
            }

            private Builder(SpecSummary base) {
                summary = base.summary;
                format = base.format;
                contents = base.contents;
            }

            public Builder summary(final String summary) {
                this.summary = summary;
                return this;
            }

            public Builder format(final String format) {
                this.format = format;
                return this;
            }

            public Builder contents(final Object contents) {
                this.contents = contents;
                return this;
            }

            public SpecSummary build() {
                return new SpecSummary(summary, format, contents);
            }
        }

        public static void pushToList(List<SpecSummary> specList, SpecSummary newFirstSpecTag) {
            specList.add(0, newFirstSpecTag);
        }
        public static void pushToList(List<SpecSummary> specList, List<SpecSummary> newFirstSpecs) {
            if (newFirstSpecs==null || newFirstSpecs.isEmpty()) return;
            if (newFirstSpecs.size()==1) {
                pushToList(specList, newFirstSpecs.iterator().next());
            } else {
                List<SpecSummary> l = MutableList.copyOf(newFirstSpecs);
                Collections.reverse(l);
                l.forEach(li -> pushToList(specList, li));
            }
        }

        public static SpecSummary popFromList(List<SpecSummary> specList) {
            if (specList.isEmpty()) return null;
            return specList.remove(0);
        }

        @Beta @Deprecated /** @deprecated since 1.1 use {@link #modifyHeadSpecSummary(List, java.util.function.Function)} */
        public static boolean modifyHeadSummary(List<SpecSummary> specList, java.util.function.Function<String, String> previousSummaryModification) {
            return modifyHeadSpecSummary(specList, s -> previousSummaryModification.apply(s.summary));
        }
        public static boolean modifyHeadSpecSummary(List<SpecSummary> specList, java.util.function.Function<SpecSummary, String> previousSummaryModification) {
            if (!specList.isEmpty() && previousSummaryModification!=null) {
                SpecSummary oldHead = popFromList(specList);
                SpecSummary newPrevHead = SpecSummary.builder(oldHead).summary(
                        previousSummaryModification.apply(oldHead)).build();
                pushToList(specList, newPrevHead);
                return true;
            }
            return false;
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


    public static List<SpecSummary> findSpecHierarchyTag(Iterable<Object> tags) {
        return findSingleKeyMapValue(SPEC_HIERARCHY, new TypeToken<List<SpecSummary>>() {}, tags);
    }

    public static Integer getDepthInAncestorTag(Set<Object> tags) {
        return BrooklynTags.findSingleKeyMapValue(BrooklynTags.DEPTH_IN_ANCESTOR, Integer.class, tags);
    }

}
