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
package org.apache.brooklyn.core.config;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.core.config.internal.AbstractCollectionConfigKey;
import org.apache.brooklyn.core.internal.storage.impl.ConcurrentMapAcceptingNullVals;
import org.apache.brooklyn.util.collections.MutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.TypeToken;

/** A config key representing a list of values. 
 * If a value is set on this key, it is _added_ to the list.
 * (With a warning is issued if a collection is passed in.)
 * If a value is set against an equivalent *untyped* key which *is* a collection,
 * it will be treated as a list upon discovery and used as a base to which subkey values are appended.
 * If a value is discovered against this key which is not a map or collection,
 * it is ignored.
 * <p>
 * To add all items in a collection, to add a collection as a single element, 
 * to clear the list, or to set a collection (clearing first), 
 * use the relevant {@link ListModification} in {@link ListModifications}.
 * <p>  
 * Specific values can be added in a replaceable way by referring to a subkey.
 * 
 * @deprecated since 0.6; use SetConfigKey. 
 * The ListConfigKey does not guarantee order when subkeys are used,
 * due to distribution and the use of the {@link ConcurrentMapAcceptingNullVals} 
 * as a backing store.
 * However the class will likely be kept around with tests for the time being
 * as we would like to repair this.
 */
//TODO Create interface
@Deprecated
public class ListConfigKey<V> extends AbstractCollectionConfigKey<List<V>,List<Object>,V> {

    private static final long serialVersionUID = 751024268729803210L;
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ListConfigKey.class);
    
    public static class Builder<V> extends BasicConfigKey.Builder<List<V>,Builder<V>> {
        protected Class<V> subType;
        
        @SuppressWarnings("unchecked")
        public Builder(TypeToken<V> subType, String name) {
            super(typeTokenListWithSubtype((Class<V>)subType.getRawType()), name);
            this.subType = (Class<V>) subType.getRawType();
        }
        public Builder(Class<V> subType, String name) {
            super(typeTokenListWithSubtype(subType), name);
            this.subType = checkNotNull(subType, "subType");
        }
        public Builder(ListConfigKey<V> key) {
            this(key.getName(), key);
        }
        public Builder(String newName, ListConfigKey<V> key) {
            super(newName, key);
            subType = key.subType;
        }
        @Override
        public Builder<V> self() {
            return this;
        }
        @Override
        @Deprecated
        public Builder<V> name(String val) {
            throw new UnsupportedOperationException("Builder must be constructed with name");
        }
        @Override
        @Deprecated
        public Builder<V> type(Class<List<V>> val) {
            throw new UnsupportedOperationException("Builder must be constructed with type");
        }
        @Override
        @Deprecated
        public Builder<V> type(TypeToken<List<V>> val) {
            throw new UnsupportedOperationException("Builder must be constructed with type");
        }
        @Override
        public ListConfigKey<V> build() {
            return new ListConfigKey<V>(this);
        }
    }

    public ListConfigKey(Builder<V> builder) {
        super(builder, builder.subType);
    }

    public ListConfigKey(Class<V> subType, String name) {
        this(subType, name, name, null);
    }

    public ListConfigKey(Class<V> subType, String name, String description) {
        this(subType, name, description, null);
    }

    @SuppressWarnings("unchecked")
    public ListConfigKey(Class<V> subType, String name, String description, List<? extends V> defaultValue) {
        super(typeTokenListWithSubtype(subType), subType, name, description, (List<V>) defaultValue);
    }

    @SuppressWarnings("unchecked")
    private static <X> TypeToken<List<X>> typeTokenListWithSubtype(final Class<X> subType) {
        return (TypeToken<List<X>>) TypeToken.of(new ParameterizedType() {
            @Override
            public Type getRawType() {
                return List.class;
            }
            
            @Override
            public Type getOwnerType() {
                return null;
            }
            
            @Override
            public Type[] getActualTypeArguments() {
                return new Type[] { subType };
            }
        });
    }
    
    @Override
    public String toString() {
        return String.format("%s[ListConfigKey:%s]", name, getTypeName());
    }

    @Override
    protected List<Object> merge(boolean unmodifiable, Iterable<?>... sets) {
        MutableList<Object> result = MutableList.of();
        for (Iterable<?> set: sets) result.addAll(set);
        if (unmodifiable) return result.asUnmodifiable();
        return result;
    }

    public interface ListModification<T> extends StructuredModification<ListConfigKey<T>>, List<T> {
    }
    
    public static class ListModifications extends StructuredModifications {
        /** when passed as a value to a ListConfigKey, causes each of these items to be added.
         * if you have just one, no need to wrap in a mod. */
        // to prevent confusion (e.g. if a list is passed) we require two objects here.
        public static final <T> ListModification<T> add(final T o1, final T o2, @SuppressWarnings("unchecked") final T ...oo) {
            List<T> l = new ArrayList<T>();
            l.add(o1); l.add(o2);
            for (T o: oo) l.add(o);
            return new ListModificationBase<T>(l, false);
        }
        /** when passed as a value to a ListConfigKey, causes each of these items to be added */
        public static final <T> ListModification<T> addAll(final Collection<T> items) { 
            return new ListModificationBase<T>(items, false);
        }
        /** when passed as a value to a ListConfigKey, causes the items to be added as a single element in the list */
        public static final <T> ListModification<T> addItem(final T item) {
            return new ListModificationBase<T>(Collections.singletonList(item), false);
        }
        /** when passed as a value to a ListConfigKey, causes the list to be cleared and these items added */
        public static final <T> ListModification<T> set(final Collection<T> items) { 
            return new ListModificationBase<T>(items, true);
        }
    }

    public static class ListModificationBase<T> extends ArrayList<T> implements ListModification<T> {
        private static final long serialVersionUID = 7131812294560446235L;
        private final boolean clearFirst;
        public ListModificationBase(Collection<T> delegate, boolean clearFirst) {
            super(delegate);
            this.clearFirst = clearFirst;
        }
        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public Object applyToKeyInMap(ListConfigKey<T> key, Map target) {
            if (clearFirst) {
                StructuredModification<StructuredConfigKey> clearing = StructuredModifications.clearing();
                clearing.applyToKeyInMap(key, target);
            }
            for (T o: this) target.put(key.subKey(), o);
            return null;
        }
    }
}
