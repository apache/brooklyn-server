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
package org.apache.brooklyn.api.internal;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.annotations.Beta;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.brooklyn.api.mgmt.EntityManager;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.SpecParameter;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Defines a spec for creating a {@link BrooklynObject}.
 * <p>
 * In addition to the contract defined by the code,
 * subclasses should provide a public static <code>create(Class)</code>
 * method to create an instance of the spec for the target type indicated by the argument.
 * <p>
 * The spec is then passed to type-specific methods,
 * e.g. {@link EntityManager#createEntity(org.apache.brooklyn.api.entity.EntitySpec)}
 * to create a managed instance of the target type.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class AbstractBrooklynObjectSpec<T, SpecT extends AbstractBrooklynObjectSpec<T, SpecT>> implements Serializable {

    private static final long serialVersionUID = 3010955277740333030L;

    private static final Logger log = LoggerFactory.getLogger(AbstractBrooklynObjectSpec.class);

    private Class<? extends T> type;
    private String displayName;
    private String catalogItemId;
    private Collection<String> catalogItemIdSearchPath = MutableSet.of();

    private Set<Object> tags = MutableSet.of();
    private List<SpecParameter<?>> parameters = ImmutableList.of();

    protected final Map<String, Object> flags = Maps.newLinkedHashMap();
    protected final Map<ConfigKey<?>, Object> config = Maps.newLinkedHashMap();

    // jackson deserializer only
    protected AbstractBrooklynObjectSpec() {}

    protected AbstractBrooklynObjectSpec(Class<? extends T> type) {
        checkValidType(type);
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    protected SpecT self() {
        return (SpecT) this;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).omitNullValues()
                .add("type", type)
                .add("displayName", displayName)
                .toString();
    }

    protected abstract void checkValidType(Class<? extends T> type);

    public SpecT displayName(String val) {
        displayName = val;
        return self();
    }

    /**
     * Set the catalog item ID that defined this object;
     * since https://issues.apache.org/jira/browse/BROOKLYN-445 this must no longer be used to indicate
     * a caller-context catalog item that should be used for search purposes;
     * instead use {@link #catalogItemIdAndSearchPath}.
     */
    public SpecT catalogItemId(String val) {
        catalogItemId = val;
        return self();
    }

    /**
     * Set the immediate catalog item ID of this object, and the search path of other catalog items used to define it.
     */
    public synchronized SpecT catalogItemIdAndSearchPath(String catalogItemId, Collection<String> searchPath) {
        if (catalogItemId != null) {
            catalogItemId(catalogItemId);
        }
        if (searchPath!=null) {
            synchronized (catalogItemIdSearchPath) {
                catalogItemIdSearchPath.clear();
                if (searchPath!=null) {
                    catalogItemIdSearchPath.addAll(searchPath);
                }
            }
        }
        return self();
    }

    public synchronized SpecT addSearchPath(List<String> searchPath) {
        if (searchPath!=null) {
            synchronized (catalogItemIdSearchPath) {
                catalogItemIdSearchPath.addAll(searchPath);
            }
        }
        return self();
    }

    public synchronized SpecT addSearchPathAtStart(List<String> searchPath) {
        if (searchPath!=null) {
            synchronized (catalogItemIdSearchPath) {
                Set<String> newPath = MutableSet.copyOf(searchPath).putAll(catalogItemIdSearchPath);
                catalogItemIdSearchPath.clear();
                catalogItemIdSearchPath.addAll(newPath);
            }
        }
        return self();
    }

    /**
     * @deprecated since 0.11.0, most callers would want {@link #stackCatalogItemId(String)} instead, though semantics are different
     */
    @Deprecated
    @Beta
    public SpecT catalogItemIdIfNotNull(String val) {
        if (val!=null) {
            catalogItemId(val);
        }
        return self();
    }

    protected Object readResolve() {
        if (catalogItemIdSearchPath == null) {
            catalogItemIdSearchPath = MutableList.of();
        }
        return this;
    }

    /**
     * Adds (stacks) the catalog item id of a wrapping specification.
     * Does nothing if the value is null.
     * If the value is not null, and is not the same as the current
     * catalogItemId, then the *current* catalogItemId will
     * be moved to the start of the search path, and the value supplied
     * as the parameter here will be set as the new catalogItemId.
     * <p>
     * Used when we want to collect IDs of items that extend other items, so that *all* can be searched.
     * e.g. if R3 extends R2 which extends R1 any one of these might supply config keys
     * referencing resources or types in their local bundles.
     */
    @Beta
    public SpecT stackCatalogItemId(String val) {
        if (null != val) {
            if (null != catalogItemId && !catalogItemId.equals(val)) {
                if (!catalogItemIdSearchPath.contains(catalogItemId)) {
                    addSearchPathAtStart(Collections.singletonList(catalogItemId));
                }
            }
            catalogItemId(val);
        }
        return self();
    }


    public SpecT tag(Object tag) {
        tags.add(tag);
        return self();
    }

    /**
     * adds the given tags
     */
    public SpecT tags(Iterable<? extends Object> tagsToAdd) {
        return tagsAdd(tagsToAdd);
    }

    /**
     * adds the given tags
     */
    @JsonSetter("brooklyn.tags")
    public SpecT tagsAdd(Iterable<? extends Object> tagsToAdd) {
        Iterables.addAll(this.tags, tagsToAdd);
        return self();
    }

    public SpecT tagsAddAtStart(Iterable<? extends Object> tagsToAdd) {
        MutableSet<Object> oldTags = MutableSet.copyOf(this.tags);
        tags.clear();
        Iterables.addAll(this.tags, tagsToAdd);
        Iterables.addAll(this.tags, oldTags);
        return self();
    }

    /**
     * replaces tags with the given
     */
    public SpecT tagsReplace(Iterable<? extends Object> tagsToReplace) {
        this.tags.clear();
        Iterables.addAll(this.tags, tagsToReplace);
        return self();
    }

    // TODO which semantics are correct? replace has been the behaviour;
    // add breaks tests and adds unwanted parameters,
    // but replacing will cause some desired parameters to be lost.
    // i (AH) think ideally the caller should remove any parameters which
    // have been defined as config keys, and then add the others;
    // or actually we should always add, since this is really defining the config keys,
    // and maybe extend the SpecParameter object to be able to advertise whether
    // it is a CatalogConfig or merely a config key, maybe introducing displayable, or even priority 
    // (but note part of the reason for CatalogConfig.priority is that java reflection doesn't preserve field order) .
    // see also comments on the camp SpecParameterResolver.

    // probably the thing to do is deprecate the ambiguous method in favour of an explicit
    @Beta
    public SpecT parameters(Iterable<? extends SpecParameter<?>> parameters) {
        return parametersReplace(parameters);
    }

    /**
     * adds the given parameters, new ones first so they dominate subsequent ones
     */
    @Beta
    public SpecT parametersAdd(Iterable<? extends SpecParameter<?>> parameters) {
        // parameters follows immutable pattern, unlike the other fields
        Set<SpecParameter<?>> params = MutableSet.<SpecParameter<?>>copyOf(parameters);
        Set<SpecParameter<?>> current = MutableSet.<SpecParameter<?>>copyOf(this.parameters);
        current.removeAll(params);

        return parametersReplace(ImmutableList.<SpecParameter<?>>builder()
                .addAll(params)
                .addAll(current)
                .build());
    }

    /**
     * replaces parameters with the given
     */
    @Beta
    public SpecT parametersReplace(Iterable<? extends SpecParameter<?>> parameters) {
        this.parameters = ImmutableList.copyOf(checkNotNull(parameters, "parameters"));
        return self();
    }

    /**
     * @return The type (often an interface) this spec represents and which will be instantiated from it
     */
    public Class<? extends T> getType() {
        return type;
    }

    /**
     * @return The display name of the object
     */
    public final String getDisplayName() {
        return displayName;
    }

    /** Same as {@link BrooklynObject#getCatalogItemId()}. */
    public final String getCatalogItemId() {
        return catalogItemId;
    }

    /** Same as {@link BrooklynObject#getCatalogItemIdSearchPath()}. */
    public final List<String> getCatalogItemIdSearchPath() {
        synchronized (catalogItemIdSearchPath) {
            return ImmutableList.copyOf(catalogItemIdSearchPath);
        }
    }

    public final Set<Object> getTags() {
        return ImmutableSet.copyOf(tags);
    }

    public final Object getTag(Predicate<Object> predicate) {
        return tags.stream().filter(predicate::apply).findAny().orElse(null);
    }

    /**
     * A list of configuration options that the entity supports.
     */
    public final List<SpecParameter<?>> getParameters() {
        //Could be null after rebind
        if (parameters != null) {
            return ImmutableList.copyOf(parameters);
        } else {
            return ImmutableList.of();
        }
    }

    // TODO Duplicates method in BasicEntityTypeRegistry and InternalEntityFactory.isNewStyleEntity
    protected final void checkIsNewStyleImplementation(Class<?> implClazz) {
        try {
            implClazz.getConstructor(new Class[0]);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Implementation " + implClazz + " must have a no-argument constructor");
        } catch (SecurityException e) {
            throw Exceptions.propagate(e);
        }

        if (implClazz.isInterface())
            throw new IllegalStateException("Implementation " + implClazz + " is an interface, but must be a non-abstract class");
        if (Modifier.isAbstract(implClazz.getModifiers()))
            throw new IllegalStateException("Implementation " + implClazz + " is abstract, but must be a non-abstract class");
    }

    // TODO Duplicates method in BasicEntityTypeRegistry
    protected final void checkIsImplementation(Class<?> val, Class<? super T> requiredInterface) {
        if (!requiredInterface.isAssignableFrom(val))
            throw new IllegalStateException("Implementation " + val + " does not implement " + requiredInterface.getName());
        if (val.isInterface())
            throw new IllegalStateException("Implementation " + val + " is an interface, but must be a non-abstract class");
        if (Modifier.isAbstract(val.getModifiers()))
            throw new IllegalStateException("Implementation " + val + " is abstract, but must be a non-abstract class");
    }

    protected SpecT copyFrom(SpecT otherSpec) {
        return displayName(otherSpec.getDisplayName())
            .configure(otherSpec.getConfig())
            .configure(otherSpec.getFlags())
            .tags(otherSpec.getTags())
            .catalogItemIdAndSearchPath(otherSpec.getCatalogItemId(), otherSpec.getCatalogItemIdSearchPath())
            .parameters(otherSpec.getParameters());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (!obj.getClass().equals(getClass())) return false;
        AbstractBrooklynObjectSpec<?,?> other = (AbstractBrooklynObjectSpec<?,?>) obj;
        if (!Objects.equal(getDisplayName(), other.getDisplayName())) return false;
        if (!Objects.equal(getCatalogItemId(), other.getCatalogItemId())) return false;
        if (!Objects.equal(getCatalogItemIdSearchPath(), other.getCatalogItemIdSearchPath())) return false;
        if (!Objects.equal(getType(), other.getType())) return false;
        if (!Objects.equal(getTags(), other.getTags())) return false;
        if (!Objects.equal(getConfig(), other.getConfig())) return false;
        if (!Objects.equal(getFlags(), other.getFlags())) return false;
        if (!Objects.equal(getParameters(), other.getParameters())) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getCatalogItemIdSearchPath(), getDisplayName(), getType(), getTags());
    }

    /**
     * strings inserted as flags, config keys inserted as config keys;
     * if you want to force one or the other, create a ConfigBag and convert to the appropriate map type
     */
    public SpecT configure(Map<?, ?> val) {
        if (val == null) {
            log.warn("Null supplied when configuring " + this);
            log.debug("Source for null supplied when configuring " + this, new Throwable("Source for null supplied when configuring " + this));
            return self();
        }
        for (Map.Entry<?, ?> entry : val.entrySet()) {
            if (entry.getKey() == null) throw new NullPointerException("Null key not permitted");
            if (entry.getKey() instanceof CharSequence)
                flags.put(entry.getKey().toString(), entry.getValue());
            else if (entry.getKey() instanceof ConfigKey<?>)
                config.put((ConfigKey<?>) entry.getKey(), entry.getValue());
            else if (entry.getKey() instanceof HasConfigKey<?>)
                config.put(((HasConfigKey<?>) entry.getKey()).getConfigKey(), entry.getValue());
            else {
                log.warn("Spec " + this + " ignoring unknown config key " + entry.getKey());
            }
        }
        return self();
    }

    public SpecT configure(CharSequence key, Object val) {
        flags.put(checkNotNull(key, "key").toString(), val);
        return self();
    }

    public <V> SpecT configure(ConfigKey<V> key, V val) {
        config.put(checkNotNull(key, "key"), val);
        return self();
    }

    public <V> SpecT configureIfNotNull(ConfigKey<V> key, V val) {
        return (val != null) ? configure(key, val) : self();
    }

    public <V> SpecT configure(ConfigKey<V> key, Task<? extends V> val) {
        config.put(checkNotNull(key, "key"), val);
        return self();
    }

    public <V> SpecT configure(HasConfigKey<V> key, V val) {
        config.put(checkNotNull(key, "key").getConfigKey(), val);
        return self();
    }

    public <V> SpecT configure(HasConfigKey<V> key, Task<? extends V> val) {
        config.put(checkNotNull(key, "key").getConfigKey(), val);
        return self();
    }

    public <V> SpecT removeConfig(ConfigKey<V> key) {
        config.remove(checkNotNull(key, "key"));
        return self();
    }

    public <V> SpecT removeFlag(String key) {
        flags.remove(checkNotNull(key, "key"));
        return self();
    }

    /**
     * Clears the config map, removing any config previously set.
     */
    public void clearConfig() {
        config.clear();
    }

    /**
     * @return Read-only construction flags
     * @see SetFromFlag declarations on the policy type
     */
    public Map<String, ?> getFlags() {
        return Collections.unmodifiableMap(flags);
    }

    /**
     * @return Read-only configuration values
     */
    public Map<ConfigKey<?>, Object> getConfig() {
        return Collections.unmodifiableMap(config);
    }

    @JsonSetter("name")
    private void jsonSetName(String val) {
        displayName(val);
    }

    @JsonSetter("brooklyn.config")
    private void jsonSetConfig(Map<String,Object> val) {
        configure(val);
    }

    @JsonAnySetter
    private void jsonSetConfig(String flag, Object value) {
        configure(flag, value);
    }

}
