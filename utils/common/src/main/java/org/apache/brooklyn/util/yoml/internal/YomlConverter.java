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

import java.util.Map;
import java.util.Objects;

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.YomlConfig;
import org.apache.brooklyn.util.yoml.YomlRequirement;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.serializers.InstantiateTypeFromRegistry;
import org.apache.brooklyn.util.yoml.serializers.ReadingTypeOnBlackboard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

public class YomlConverter {

    private static final Logger log = LoggerFactory.getLogger(YomlConverter.class);
    private final YomlConfig config;
    
    /** easy way at dev time to get trace logging to stdout info level */
    private static boolean FORCE_SHOW_TRACE_LOGGING = false;
    
    public YomlConverter(YomlConfig config) {
        this.config = config;
    }

    /**
     * returns object of type expectedType
     * makes shallow copy of the object, then goes through serializers modifying it or creating/setting result,
     * until result is done
     */ 
    public Object read(YomlContextForRead context) {
        loopOverSerializers(context);
        return context.getJavaObject();
    }

    /**
     * returns jsonable object (map, list, primitive) 
     */   
    public Object write(final YomlContextForWrite context) {
        loopOverSerializers(context);
        return context.getYamlObject();
    }

    protected boolean isTraceDetailWanted() {
        return log.isTraceEnabled() || FORCE_SHOW_TRACE_LOGGING;
    }
    protected void logTrace(String message) {
        if (FORCE_SHOW_TRACE_LOGGING) {
            log.info(message);
        } else {
            log.trace(message);
        }
    }
    
    protected void loopOverSerializers(YomlContext context) {
        // TODO refactor further so we always pass the context
        Map<Object, Object> blackboard = context.getBlackboard();
        
        // find the serializers known so far; store on blackboard so they could be edited
        SerializersOnBlackboard serializers = SerializersOnBlackboard.getOrCreate(blackboard);
        if (context.getExpectedType()!=null) {
            serializers.addExpectedTypeSerializers(config.getTypeRegistry().getSerializersForType(context.getExpectedType(), context));
        }
        serializers.addPostSerializers(config.getSerializersPost());
        
        if (context instanceof YomlContextForRead) {
            // read needs instantiated so that these errors display before manipulating errors and others
            ReadingTypeOnBlackboard.get(blackboard);
        }
        
        String lastYamlObject = ""+context.getYamlObject();
        String lastJavaObject = ""+context.getJavaObject();
        Map<String,String> lastBlackboardOutput = MutableMap.of();
        if (isTraceDetailWanted()) {
            logTrace("YOML "+contextMode(context)+" "+contextSummary(context)+" (expecting "+context.getExpectedType()+")");
            showBlackboard(blackboard, lastBlackboardOutput, false);
        }
        
        while (context.phaseAdvance()) {
            while (context.phaseStepAdvance()<Iterables.size(serializers.getSerializers())) {
                if (context.phaseCurrentStep()==0) {
                    if (isTraceDetailWanted()) { 
                        logTrace("yoml "+contextMode(context)+" "+contextSummary(context)+" entering phase "+context.phaseCurrent()+", blackboard size "+blackboard.size());
                    }
                }
                YomlSerializer s = Iterables.get(serializers.getSerializers(), context.phaseCurrentStep());
                if (isTraceDetailWanted()) logTrace("yoml "+contextMode(context)+" "+context.phaseCurrent()+" "+context.phaseCurrentStep()+": "+cleanName(s));
                if (context instanceof YomlContextForRead) {
                    s.read((YomlContextForRead)context, this);
                } else {
                    s.write((YomlContextForWrite)context, this);
                }
                
                if (isTraceDetailWanted()) {
                    String nowYamlObject = ""+context.getYamlObject();
                    if (!Objects.equals(lastYamlObject, nowYamlObject)) {
                        logTrace("  yaml obj now: "+nowYamlObject);
                        lastYamlObject = nowYamlObject;
                    }
                    String nowJavaObject = ""+context.getJavaObject();
                    if (!Objects.equals(lastJavaObject, nowJavaObject)) {
                        logTrace("  java obj now: "+nowJavaObject);
                        lastJavaObject = nowJavaObject;
                    }
                    showBlackboard(blackboard, lastBlackboardOutput, true);
                }
            }
        }
        
        if (isTraceDetailWanted()) logTrace("YOML done looking at "+context.getJsonPath()+"/ = "+context.getJavaObject()+" <-> "+context.getYamlObject());
        checkCompletion(context);
    }

    protected String contextMode(YomlContext context) {
        return context instanceof YomlContextForWrite ? "write" : "read";
    }

    private void showBlackboard(Map<Object, Object> blackboard, Map<String, String> lastBlackboardOutput, boolean justDelta) {
        Map<String, String> newBlackboardOutput = MutableMap.copyOf(lastBlackboardOutput);
        if (!justDelta) lastBlackboardOutput.clear();
        
        for (Map.Entry<Object, Object> bb: blackboard.entrySet()) {
            String k = cleanName(bb.getKey());
            String v = cleanName(bb.getValue());
            newBlackboardOutput.put(k, v);
            String last = lastBlackboardOutput.remove(k);
            if (!justDelta) logTrace("  "+k+": "+v);
            else if (!Objects.equals(last, v)) logTrace("  "+k+" "+(last==null ? "added" : "now")+": "+v);
        }

        for (String k: lastBlackboardOutput.keySet()) {
            logTrace("  "+k+" removed");
        }
        
        lastBlackboardOutput.putAll(newBlackboardOutput);
    }

    protected String cleanName(Object s) {
        String out = Strings.toString(s);
        out = Strings.removeFromStart(out, YomlConverter.class.getPackage().getName());
        out = Strings.removeFromStart(out, InstantiateTypeFromRegistry.class.getPackage().getName());
        out = Strings.removeFromStart(out, ".");
        return out;
    }

    protected String contextSummary(YomlContext context) {
        return (Strings.isBlank(context.getJsonPath()) ? "/" : context.getJsonPath()) + " = " +
            (context instanceof YomlContextForWrite ? context.getJavaObject() : context.getYamlObject());
    }

    protected void checkCompletion(YomlContext context) {
        for (Object bo: context.getBlackboard().values()) {
            if (bo instanceof YomlRequirement) {
                ((YomlRequirement)bo).checkCompletion(context);
            }
        }
    }


    /**
     * generates human-readable schema for a type
     */
    public String document(String type) {
        // TODO
        return null;
    }

    public YomlConfig getConfig() {
        return config;
    }

}
