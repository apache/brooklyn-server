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
import java.util.Map;

import com.google.common.base.MoreObjects;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.util.collections.MutableList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @since 0.7.0 some strongly typed tags for reference; note these may migrate elsewhere! */
@Beta
public class BrooklynTags {

    private static final Logger LOG = LoggerFactory.getLogger(BrooklynTags.class);

    public static final String YAML_SPEC_KIND = "yaml_spec";
    public static final String YAML_SPEC_HIERARCHY = "yaml_spec_hierarchy";
    public static final String NOTES_KIND = "notes";
    public static final String OWNER_ENTITY_ID = "owner_entity_id";
    public static final String ICON_URL = "icon_url";
    public static final String UPGRADED_FROM = "upgraded_from";
    /** tag on a registered type indicating that an item is intended to be used as a template,
     * and does not have to resolve */
    public static final Object CATALOG_TEMPLATE = "catalog_template";

    public interface HasKind {
        public String getKind();
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

    public static class SpecHierarchyTag implements Serializable, HasKind {
        private static final long serialVersionUID = 3805124696862755492L;

        public static final String KIND = YAML_SPEC_HIERARCHY;

        public static class SpecSummary implements Serializable {
            @JsonProperty
            public final String summary;
            @JsonProperty
            public final String format;
            @JsonProperty
            public final Object contents;

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
        }

        public static Builder builder() { return new Builder(); }

        public static class Builder {
            private String format;
            private String summary;
            private Object contents;

            private Builder() {}

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


            public SpecSummary buildSpecSummary() {
                return new SpecSummary(summary, format, contents);
            }

            public SpecHierarchyTag buildSpecHierarchyTag() {
                return new SpecHierarchyTag(BrooklynTags.YAML_SPEC_HIERARCHY, MutableList.of(buildSpecSummary()));
            }
        }

        @JsonProperty
        String kind;

        @JsonProperty
        List<SpecSummary> specList;

        // for JSON
        private SpecHierarchyTag() {}

        private SpecHierarchyTag(String kind, List<SpecSummary> specList) {
            this.kind = kind;
            this.specList = specList;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .omitNullValues()
                    .add("kind", kind)
                    .add("specList", specList)
                    .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SpecHierarchyTag specTag = (SpecHierarchyTag) o;
            return Objects.equal(kind, specTag.kind) && Objects.equal(specList, specTag.specList) ;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(kind, specList);
        }

        public String getKind() {
            return kind;
        }

        public List<SpecSummary> getSpecList() {
            return specList;
        }

        public void push(SpecSummary newFirstSpecTag) {
            // usually the list has a single element here, if
            specList.add(0, newFirstSpecTag);
        }
        public void push(SpecHierarchyTag newFirstSpecs) {
            // usually the list has a single element here, if
            newFirstSpecs.getSpecList().forEach(this::push);
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

    public static SpecHierarchyTag findSpecHierarchyTag(Iterable<Object> tags) {
        return findFirstOfKind(SpecHierarchyTag.KIND, SpecHierarchyTag.class, tags);
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

}
