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
package org.apache.brooklyn.entity.group;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.entity.stock.DelegateEntity;
import org.apache.brooklyn.util.concurrent.Locks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;


/**
 * Represents a group of entities - sub-classes can support dynamically changing membership,
 * ad hoc groupings, etc.
 * <p>
 * Synchronization model. When changing and reading the group membership, this class uses internal
 * synchronization to ensure atomic operations and the "happens-before" relationship for reads/updates
 * from different threads. Sub-classes should not use this same synchronization mutex when doing
 * expensive operations - e.g. if resizing a cluster, don't block everyone else from asking for the
 * current number of members.
 */
public abstract class AbstractGroupImpl extends AbstractEntity implements AbstractGroup {
    private static final Logger log = LoggerFactory.getLogger(AbstractGroupImpl.class);

    private Set<Entity> members = Sets.newLinkedHashSet();

    @Override
    public void setManagementContext(ManagementContextInternal managementContext) {
        super.setManagementContext(managementContext);
    }

    @Override
    public void init() {
        super.init();
        sensors().set(GROUP_SIZE, 0);
        sensors().set(GROUP_MEMBERS, ImmutableList.<Entity>of());
    }

    @Override
    protected void initEnrichers() {
        super.initEnrichers();
        
        // check states and upness separately so they can be individually replaced if desired
        // problem if any children or members are on fire
        enrichers().add(ServiceStateLogic.newEnricherFromChildrenState()
                .checkChildrenAndMembers()
                .requireRunningChildren(getConfig(RUNNING_QUORUM_CHECK))
                .suppressDuplicates(true));
        // defaults to requiring at least one member or child who is up
        enrichers().add(ServiceStateLogic.newEnricherFromChildrenUp()
                .checkChildrenAndMembers()
                .requireUpChildren(getConfig(UP_QUORUM_CHECK))
                .suppressDuplicates(true));
    }

    /**
     * Adds the given entity as a member of this group <em>and</em> this group as one of the groups of the child
     */
    @Override
    public boolean addMember(Entity member) {
        return Locks.withLock(getLockInternal(), () -> {
            synchronized (members) {
                if (Entities.isNoLongerManaged(member)) {
                    // Don't add dead entities, as they could never be removed (because addMember could be called in
                    // concurrent thread as removeMember triggered by the unmanage).
                    // Not using Entities.isManaged here, as could be called in entity.init()
                    log.debug("Group {} ignoring new member {}, because it is no longer managed", this, member);
                    return false;
                }
    
                // FIXME do not set sensors on members; possibly we don't need FIRST at all, just look at the first in MEMBERS, and take care to guarantee order there
                Entity first = getAttribute(FIRST);
                if (first == null) {
                    sensors().set(FIRST, member);
                }
    
                ((EntityInternal)member).groups().add((Group)getProxyIfAvailable());
                boolean changed = addMemberInternal(member);
                if (changed) {
                    log.debug("Group {} got new member {}", this, member);
                    sensors().set(GROUP_SIZE, getCurrentSize());
                    sensors().set(GROUP_MEMBERS, getMembers());
                    // emit after the above so listeners can use getMembers() and getCurrentSize()
                    sensors().emit(MEMBER_ADDED, member);
    
                    if (Boolean.TRUE.equals(getConfig(MEMBER_DELEGATE_CHILDREN))) {
                        log.warn("Use of deprecated ConfigKey {} in {} (as of 0.9.0)", MEMBER_DELEGATE_CHILDREN.getName(), this);
                        Optional<Entity> result = Iterables.tryFind(getChildren(), Predicates.equalTo(member));
                        if (!result.isPresent()) {
                            String nameFormat = Optional.fromNullable(getConfig(MEMBER_DELEGATE_NAME_FORMAT)).or("%s");
                            DelegateEntity child = addChild(EntitySpec.create(DelegateEntity.class)
                                    .configure(DelegateEntity.DELEGATE_ENTITY, member)
                                    .displayName(String.format(nameFormat, member.getDisplayName())));
                        }
                    }
    
                    getManagementSupport().getEntityChangeListener().onMembersChanged();
                }
                return changed;
            }
        });
    }

    // visible for rebind
    public boolean addMemberInternal(Entity member) {
        synchronized (members) {
            return members.add(member);
        }
    }

    /**
     * Returns {@code true} if the group was changed as a result of the call.
     */
    @Override
    public boolean removeMember(final Entity member) {
        return Locks.withLock(getLockInternal(), () -> {
            synchronized (members) {
                boolean changed = (member != null && members.remove(member));
                if (changed) {
                    log.debug("Group {} lost member {}", this, member);
                    // TODO ideally the following are all synched
                    sensors().set(GROUP_SIZE, getCurrentSize());
                    Collection<Entity> membersNow = getMembers();
                    sensors().set(GROUP_MEMBERS, membersNow);
                    if (member.equals(getAttribute(FIRST))) {
                        sensors().set(FIRST, membersNow.isEmpty() ? null : membersNow.iterator().next());
                    }
                    // emit after the above so listeners can use getMembers() and getCurrentSize()
                    sensors().emit(MEMBER_REMOVED, member);
    
                    if (Boolean.TRUE.equals(getConfig(MEMBER_DELEGATE_CHILDREN))) {
                        Optional<Entity> result = Iterables.tryFind(getChildren(), new Predicate<Entity>() {
                            @Override
                            public boolean apply(Entity input) {
                                Entity delegate = input.getConfig(DelegateEntity.DELEGATE_ENTITY);
                                if (delegate == null) return false;
                                return delegate.equals(member);
                            }
                        });
                        if (result.isPresent()) {
                            Entity child = result.get();
                            removeChild(child);
                            Entities.unmanage(child);
                        }
                    }
    
                }
                
                Exception errorRemoving = null;
                // suppress errors if member is being unmanaged in parallel
                try {
                    ((EntityInternal)member).groups().remove((Group)getProxyIfAvailable());
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    errorRemoving = e;
                }
                
                getManagementSupport().getEntityChangeListener().onMembersChanged();
                
                if (errorRemoving!=null) {
                    if (Entities.isNoLongerManaged(member)) {
                        log.debug("Ignoring error when telling group "+this+" unmanaged member "+member+" is is removed: "+errorRemoving);
                    } else {
                        Exceptions.propagate(errorRemoving);
                    }
                }
    
                return changed;
            }
        });
    }

    @Override
    public void setMembers(Collection<Entity> m) {
        setMembers(m, null);
    }

    @Override
    public void setMembers(Collection<Entity> mm, Predicate<Entity> filter) {
        Locks.withLock(getLockInternal(), () -> {
            synchronized (members) {
                log.debug("Group {} members set explicitly to {} (of which some possibly filtered)", this, members);
                List<Entity> mmo = new ArrayList<Entity>(getMembers());
                for (Entity m: mmo) {
                    if (!(mm.contains(m) && (filter==null || filter.apply(m))))
                        // remove, unless already present, being set, and not filtered out
                        removeMember(m);
                }
                for (Entity m: mm) {
                    if ((!mmo.contains(m)) && (filter==null || filter.apply(m))) {
                        // add if not alrady contained, and not filtered out
                        addMember(m);
                    }
                }
    
                getManagementSupport().getEntityChangeListener().onMembersChanged();
            }
        });
    }

    @Override
    public Collection<Entity> getMembers() {
        // TODO use this instead; see issue and email thread where this comment was introduced
//        relations().getRelations(EntityRelations.GROUP_CONTAINS);
        synchronized (members) {
            return ImmutableSet.<Entity>copyOf(members);
        }
    }

    @Override
    public boolean hasMember(Entity e) {
        synchronized (members) {
            return members.contains(e);
        }
    }

    @Override
    public Integer getCurrentSize() {
        synchronized (members) {
            return members.size();
        }
    }

    @Override
    public <T extends Entity> T addMemberChild(EntitySpec<T> spec) {
        T child = addChild(spec);
        addMember(child);
        return child;
    }

    @Override
    public <T extends Entity> T addMemberChild(T child) {
        child = addChild(child);
        addMember(child);
        return child;
    }

}
