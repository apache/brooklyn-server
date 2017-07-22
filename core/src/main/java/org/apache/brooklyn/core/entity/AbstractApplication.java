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
package org.apache.brooklyn.core.entity;

import java.util.Collection;
import java.util.Map;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ServiceProblemsLogic;
import org.apache.brooklyn.core.entity.trait.StartableMethods;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

/**
 * Users can extend this to define the entities in their application, and the relationships between
 * those entities. Users should override the {@link #init()} method, and in there should create 
 * their entities.
 */
public abstract class AbstractApplication extends AbstractEntity implements StartableApplication {

    /*
     * Note that DEFAULT_DISPLAY_NAME is particularly important for apps.
     * It gives the default name to use for this app, if not explicitly overridden by the top-level app.
     * Necessary to avoid the app being wrapped in another layer of "BasicApplication" on deployment.
     * Previously, the catalog item gave an explicit name (rathe rthan this defaultDisplayName), which
     * meant that if the user chose a different name then AMP would automatically wrap this app so
     * that both names would be presented.
     */

    private static final Logger log = LoggerFactory.getLogger(AbstractApplication.class);
    
    private volatile Application application;

    public AbstractApplication() {
    }

    @Override
    public void init() { 
        super.init();
        initApp();
    }
    
    protected void initApp() {}
    
    /**
     * 
     * @deprecated since 0.6; use EntitySpec so no-arg constructor
     */
    @Deprecated
    public AbstractApplication(Map properties) {
        super(properties);
    }

    /** 
     * Constructor for when application is nested inside another application
     * 
     * @deprecated Nesting applications is not currently supported
     */
    @Deprecated
    public AbstractApplication(Map properties, Entity parent) {
        super(properties, parent);
    }

    @Override
    public Application getApplication() {
        if (application!=null) {
            if (application.getId().equals(getId()))
                return (Application) getProxyIfAvailable();
            return application;
        }
        if (getParent()==null) return (Application)getProxyIfAvailable();
        return getParent().getApplication();
    }
    
    @Override
    protected synchronized void setApplication(Application app) {
        if (app.getId().equals(getId())) {
            application = getProxy()!=null ? (Application)getProxy() : app;
        } else {
            application = app;

            // Alex, Mar 2013: added some checks; 
            // i *think* these conditions should not happen, 
            // and so should throw but don't want to break things (yet)
            if (getParent()==null) {
                log.warn("Setting application of "+this+" to "+app+", but "+this+" is not parented");
            } else if (getParent().getApplicationId().equals(app.getParent())) {
                log.warn("Setting application of "+this+" to "+app+", but parent "+getParent()+" has different app "+getParent().getApplication());
            }
        }
        super.setApplication(app);
    }
    
    @Override
    public AbstractApplication setParent(Entity parent) {
        super.setParent(parent);
        return this;
    }
    
    /** as {@link AbstractEntity#initEnrichers()} but also adding default service not-up and problem indicators from children */
    @Override
    protected void initEnrichers() {
        super.initEnrichers();

        // default app logic; easily overridable by adding a different enricher with the same tag
        ServiceStateLogic.newEnricherFromChildren().checkChildrenAndMembers()
                .configure(ServiceStateLogic.ComputeServiceIndicatorsFromChildrenAndMembers.UP_QUORUM_CHECK, config().get(UP_QUORUM_CHECK))
                .configure(ServiceStateLogic.ComputeServiceIndicatorsFromChildrenAndMembers.RUNNING_QUORUM_CHECK, config().get(RUNNING_QUORUM_CHECK))
                .addTo(this);
        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(this, Attributes.SERVICE_STATE_ACTUAL, "Application created but not yet started, at "+Time.makeDateString());
    }

    @Override
    public void onManagementStarted() {
        super.onManagementStarted();
        if (!isRebinding()) {
            recordApplicationEvent(Lifecycle.CREATED);
        }
    }

    /**
     * Default start will start all Startable children (child.start(Collection<? extends Location>)),
     * calling preStart(locations) first and postStart(locations) afterwards.
     */
    @Override
    public void start(Collection<? extends Location> locations) {
        this.addLocations(locations);
        // 2016-01: only pass locations passed to us, as per ML discussion
        Collection<? extends Location> locationsToUse = locations==null ? ImmutableSet.<Location>of() : locations;
        ServiceProblemsLogic.clearProblemsIndicator(this, START);
        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(this, Attributes.SERVICE_STATE_ACTUAL, "Application starting");
        ServiceStateLogic.ServiceNotUpLogic.clearNotUpIndicator(this, START.getName());
        setExpectedStateAndRecordLifecycleEvent(Lifecycle.STARTING);
        try {
            try {
                
                preStart(locationsToUse);
                
                // Opportunity to block startup until other dependent components are available
                Object val = config().get(START_LATCH);
                if (val != null) log.debug("{} finished waiting for start-latch; continuing...", this);
                
                doStart(locationsToUse);
                postStart(locationsToUse);
                
            } catch (ProblemStartingChildrenException e) {
                throw Exceptions.propagate(e);
            } catch (Exception e) {
                // should remember problems, apart from those that happened starting children
                // fixed bug introduced by the fix in dacf18b831e1e5e1383d662a873643a3c3cabac6
                // where failures in special code at application root don't cause app to go on fire 
                ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(this, START.getName(), Exceptions.collapseText(e));
                throw Exceptions.propagate(e);
            }
            
        } catch (Exception e) {
            recordApplicationEvent(Lifecycle.ON_FIRE);
            ServiceStateLogic.setExpectedStateRunningWithErrors(this);
            
            // no need to log here; the effector invocation should do that
            throw Exceptions.propagate(e);
            
        } finally {
            ServiceStateLogic.ServiceNotUpLogic.clearNotUpIndicator(this, Attributes.SERVICE_STATE_ACTUAL);
        }
        
        ServiceStateLogic.setExpectedState(this, Lifecycle.RUNNING);
        setExpectedStateAndRecordLifecycleEvent(Lifecycle.RUNNING);

        logApplicationLifecycle("Started");
    }

    protected void logApplicationLifecycle(String message) {
        log.info(message+" application " + this);
    }
    
    protected void doStart(Collection<? extends Location> locations) {
        doStartChildren(locations);        
    }
    
    protected void doStartChildren(Collection<? extends Location> locations) {
        try {
            StartableMethods.start(this, locations);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            throw new ProblemStartingChildrenException(e);
        }
    }

    protected static class ProblemStartingChildrenException extends RuntimeException {
        private static final long serialVersionUID = 7710856289284536803L;
        private ProblemStartingChildrenException(Exception cause) { super(cause); }
    }
    
    /**
     * Default is no-op. Subclasses can override.
     * */
    public void preStart(Collection<? extends Location> locations) {
        //no-op
    }

    /**
     * Default is no-op. Subclasses can override.
     * */
    public void postStart(Collection<? extends Location> locations) {
        //no-op
    }

    /**
     * Default stop will stop all Startable children
     */
    @Override
    public void stop() {
        logApplicationLifecycle("Stopping");

        setExpectedStateAndRecordLifecycleEvent(Lifecycle.STOPPING);
        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(this, Attributes.SERVICE_STATE_ACTUAL, "Application stopping");
        sensors().set(SERVICE_UP, false);
        try {
            doStop();
        } catch (Exception e) {
            setExpectedStateAndRecordLifecycleEvent(Lifecycle.ON_FIRE);
            log.warn("Error stopping application " + this + " (rethrowing): "+e);
            throw Exceptions.propagate(e);
        }
        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(this, Attributes.SERVICE_STATE_ACTUAL, "Application stopped");
        setExpectedStateAndRecordLifecycleEvent(Lifecycle.STOPPED);

        logApplicationLifecycle("Stopped");
        
        if (getParent()==null && Boolean.TRUE.equals(getConfig(DESTROY_ON_STOP))) {
            synchronized (this) {
                //TODO review mgmt destroy lifecycle
                //  we don't necessarily want to forget all about the app on stop, 
                //since operator may be interested in things recently stopped;
                //but that could be handled by the impl at management
                //(keeping recently unmanaged things)  
                //  however unmanaging must be done last, _after_ we stop children and set attributes 
                getEntityManager().unmanage(this);
            }
        }
    }

    protected void doStop() {
        StartableMethods.stop(this);
    }

    /** default impl invokes restart on all children simultaneously */
    @Override
    public void restart() {
        StartableMethods.restart(this);
    }

    @Override
    public void onManagementStopped() {
        super.onManagementStopped();
        if (getManagementContext().isRunning()) {
            recordApplicationEvent(Lifecycle.DESTROYED);
        }
    }

    protected void setExpectedStateAndRecordLifecycleEvent(Lifecycle state) {
        ServiceStateLogic.setExpectedState(this, state);
        recordApplicationEvent(state);
    }

    protected void recordApplicationEvent(Lifecycle state) {
        try {
            ((ManagementContextInternal)getManagementContext()).getUsageManager().recordApplicationEvent(this, state);
        } catch (RuntimeInterruptedException e) {
            throw e;
        } catch (RuntimeException e) {
            if (getManagementContext().isRunning()) {
                log.warn("Problem recording application event '"+state+"' for "+this, e);
            }
        }
    }
}
