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
package org.apache.brooklyn.util.yoml.internal;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;

import com.google.common.base.Objects;

public abstract class YomlContext {

    final YomlContext parent;
    final String jsonPath;
    final String expectedType;
    Object javaObject;
    Object yamlObject;
    Map<Object,Object> blackboard;
    
    String phaseCurrent = null;
    int phaseCurrentStep = -1;
    Set<String> phasesFollowing = MutableSet.of(StandardPhases.MANIPULATING, StandardPhases.HANDLING_TYPE, StandardPhases.HANDLING_TYPE, StandardPhases.HANDLING_FIELDS);
    List<String> phasesPreceding = MutableList.of();
    
    public static interface StandardPhases {
        String MANIPULATING = "manipulating";
        String HANDLING_TYPE = "handling-type";
        String HANDLING_FIELDS = "handling-fields";
    }
    
    public YomlContext(String jsonPath, String expectedType, YomlContext parent) {
        this.jsonPath = jsonPath;
        this.expectedType = expectedType;
        this.parent = parent;
    }
    
    public YomlContext getParent() {
        return parent;
    }
    public String getJsonPath() {
        return jsonPath;
    }
    public String getExpectedType() {
        return expectedType;
    }
    public Object getJavaObject() {
        return javaObject;
    }
    public void setJavaObject(Object javaObject) {
        this.javaObject = javaObject;
    }
    
    public Object getYamlObject() {
        return yamlObject;
    }
    public void setYamlObject(Object yamlObject) {
        this.yamlObject = yamlObject;
    }
    
    public boolean isPhase(String phase) { return Objects.equal(phase, phaseCurrent); }
    public boolean seenPhase(String phase) { return phasesPreceding.contains(phase); }
    public boolean willDoPhase(String phase) { return phasesFollowing.contains(phase); }
    public String phaseCurrent() { return phaseCurrent; }
    public int phaseCurrentStep() { return phaseCurrentStep; }
    public int phaseStepAdvance() { 
        if (phaseCurrentStep() < Integer.MAX_VALUE) phaseCurrentStep++;
        return phaseCurrentStep();
    }
    public boolean phaseAdvance() {
        if (phaseCurrent!=null) phasesPreceding.add(phaseCurrent);
        Iterator<String> fi = phasesFollowing.iterator();
        if (!fi.hasNext()) {
            phaseCurrent = null;
            phaseCurrentStep = Integer.MAX_VALUE;
            return false;
        }
        phaseCurrent = fi.next();
        phasesFollowing = MutableSet.copyOf(fi);
        phaseCurrentStep = -1;
        return true;
    }
    public void phaseRestart() { phaseCurrentStep = -1; }
    public void phaseInsert(String nextPhase, String ...otherNextPhases) { 
        phasesFollowing = MutableSet.of(nextPhase).putAll(Arrays.asList(otherNextPhases)).putAll(phasesFollowing); 
    }
    public void phasesFinished() {
        if (phaseCurrent!=null) phasesPreceding.add(phaseCurrent);
        phasesFollowing = MutableSet.of(); phaseAdvance(); 
    }

    @Override
    public String toString() {
        return super.toString()+"["+getJsonPath()+"]";
    }

    public Map<Object, Object> getBlackboard() {
        if (blackboard==null) blackboard = MutableMap.of();
        return blackboard; 
    }
    
}
