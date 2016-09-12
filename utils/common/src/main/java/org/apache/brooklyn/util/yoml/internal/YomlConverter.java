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

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.yoml.YomlContext;
import org.apache.brooklyn.util.yoml.YomlContextForRead;
import org.apache.brooklyn.util.yoml.YomlContextForWrite;
import org.apache.brooklyn.util.yoml.YomlRequirement;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.serializers.JavaFieldsOnBlackboard;
import org.apache.brooklyn.util.yoml.serializers.ReadingTypeOnBlackboard;
import org.apache.brooklyn.util.yoml.serializers.YamlKeysOnBlackboard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

public class YomlConverter {

    private static final Logger log = LoggerFactory.getLogger(YomlConverter.class);
    private final YomlConfig config;
    
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

    protected void loopOverSerializers(YomlContext context) {
        Map<Object,Object> blackboard = MutableMap.of();
        
        // find the serializers known so far; store on blackboard so they could be edited
        SerializersOnBlackboard serializers = SerializersOnBlackboard.create(blackboard);
        if (context.getExpectedType()!=null) {
            serializers.addExpectedTypeSerializers(config.getTypeRegistry().getSerializersForType(context.getExpectedType()));
        }
        serializers.addPostSerializers(config.getSerializersPost());
        
        if (context instanceof YomlContextForRead) {
            // read needs instantiated so that these errors display before manipulating errors and others
            ReadingTypeOnBlackboard.get(blackboard);
        }
        
        if (log.isTraceEnabled()) log.trace("YOML now looking at "+context.getJsonPath()+"/ = "+context.getJavaObject()+" <-> "+context.getYamlObject()+" ("+context.getExpectedType()+")");
        while (context.phaseAdvance()) {
            if (log.isTraceEnabled()) log.trace("read "+context.getJsonPath()+"/ = "+context.getJavaObject()+" entering phase "+context.phaseCurrent()+": "+YamlKeysOnBlackboard.peek(blackboard)+" / "+JavaFieldsOnBlackboard.peek(blackboard)+" / "+JavaFieldsOnBlackboard.peek(blackboard, "config"));
            while (context.phaseStepAdvance()<Iterables.size(serializers.getSerializers())) {
                YomlSerializer s = Iterables.get(serializers.getSerializers(), context.phaseCurrentStep());
                if (context instanceof YomlContextForRead) {
                    if (log.isTraceEnabled()) log.trace("read "+context.getJsonPath()+"/ = "+context.getJavaObject()+" serializer "+s+" starting ("+context.phaseCurrent()+"."+context.phaseCurrentStep()+") ");
                    s.read((YomlContextForRead)context, this, blackboard);
                    if (log.isTraceEnabled()) log.trace("read "+context.getJsonPath()+"/ = "+context.getJavaObject()+" serializer "+s+" ended: "+context.getYamlObject());
                } else {
                    if (log.isTraceEnabled()) log.trace("write "+context.getJsonPath()+"/ = "+context.getJavaObject()+" serializer "+s+" starting ("+context.phaseCurrent()+"."+context.phaseCurrentStep()+") ");
                    s.write((YomlContextForWrite)context, this, blackboard);
                    if (log.isDebugEnabled())
                        log.debug("write "+context.getJsonPath()+"/ = "+context.getJavaObject()+" serializer "+s+" ended: "+context.getYamlObject());
                    if (log.isTraceEnabled()) log.trace("write "+context.getJsonPath()+"/ = "+context.getJavaObject()+" serializer "+s+" ended: "+context.getYamlObject());
                }
            }
        }
        
        if (log.isTraceEnabled()) log.trace("YOML done looking at "+context.getJsonPath()+"/ = "+context.getJavaObject()+" <-> "+context.getYamlObject());
        checkCompletion(context, blackboard);
    }

    protected void checkCompletion(YomlContext context, Map<Object, Object> blackboard) {
        for (Object bo: blackboard.values()) {
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
