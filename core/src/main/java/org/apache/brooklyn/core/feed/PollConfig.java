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
package org.apache.brooklyn.core.feed;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.time.Duration;

/**
 * Configuration for polling, which is being added to a feed (e.g. to poll a given URL over http).
 * 
 * @author aled
 */
public class PollConfig<V, T, F extends PollConfig<V, T, F>> extends FeedConfig<V, T, F> {

    private Boolean skipInitialRun = null;  // null default is false
    private long period = -1;
    private Object otherTriggers;
    private String description;
    private Supplier<DslPredicates.DslPredicate> condition;

    public PollConfig(AttributeSensor<T> sensor) {
        super(sensor);
    }

    public PollConfig(PollConfig<V,T,F> other) {
        super(other);
        this.period = other.period;
        this.otherTriggers = other.otherTriggers;
        this.condition = other.condition;
        this.description = other.description;
    }

    public Boolean getSkipInitialRun() { return skipInitialRun; }
    public F skipInitialRun(Boolean val) { this.skipInitialRun = val; return self(); }

    public long getPeriod() {
        return period;
    }
    
    public F period(Duration val) {
        if (val==null) {
            this.period = -1;
        } else {
            checkArgument(val.toMilliseconds() >= 0, "period must be greater than or equal to zero");
            this.period = val.toMilliseconds();
        }
        return self();
    }
    
    public F period(long millis) {
        checkArgument(millis >= 0, "period must be greater than or equal to zero");
        this.period = millis; return self();
    }
    
    public F period(long val, TimeUnit units) {
        checkArgument(val >= 0, "period must be greater than or equal to zero");
        return period(units.toMillis(val));
    }
    
    public F description(String description) {
        this.description = description;
        return self();
    }

    public F otherTriggers(Object otherTriggers) {
        this.otherTriggers = otherTriggers;
        return self();
    }

    public Object getOtherTriggers() {
        return otherTriggers;
    }

    public F condition(Supplier<DslPredicates.DslPredicate> condition) {
        this.condition = condition;
        return self();
    }

    public Supplier<DslPredicates.DslPredicate> getCondition() {
        return condition;
    }

    public String getDescription() {
        return description;
    }
    
    @Override protected MutableList<Object> toStringOtherFields() {
        MutableList<Object> result = super.toStringOtherFields().appendIfNotNull(description);
        if (period>0 && period < Duration.PRACTICALLY_FOREVER.toMilliseconds()) result.append("period: "+Duration.of(period));
        if (otherTriggers!=null) result.append("triggers: "+otherTriggers);
        return result;
    }

    @Override
    protected MutableList<Object> equalsFields() {
        return super.equalsFields().appendIfNotNull(period);
    }
}
