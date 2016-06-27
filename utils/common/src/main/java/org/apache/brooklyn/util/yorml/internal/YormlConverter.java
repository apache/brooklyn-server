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
package org.apache.brooklyn.util.yorml.internal;

import java.util.Map;

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.yorml.YormlContext;
import org.apache.brooklyn.util.yorml.YormlContextForRead;
import org.apache.brooklyn.util.yorml.YormlContextForWrite;
import org.apache.brooklyn.util.yorml.YormlRequirement;
import org.apache.brooklyn.util.yorml.YormlSerializer;
import org.apache.brooklyn.util.yorml.serializers.ReadingTypeOnBlackboard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

public class YormlConverter {

    private static final Logger log = LoggerFactory.getLogger(YormlConverter.class);
    
    private final YormlConfig config;
    
    public YormlConverter(YormlConfig config) {
        this.config = config;
    }

    /**
     * returns object of type expectedType
     * makes shallow copy of the object, then goes through serializers modifying it or creating/setting result,
     * until result is done
     */ 
    public Object read(YormlContextForRead context) {
        loopOverSerializers(context);
        return context.getJavaObject();
    }

    /**
     * returns jsonable object (map, list, primitive) 
     */   
    public Object write(final YormlContextForWrite context) {
        loopOverSerializers(context);
        return context.getYamlObject();
    }

    protected void loopOverSerializers(YormlContext context) {
        Map<Object,Object> blackboard = MutableMap.of();
        
        // find the serializers known so far; store on blackboard so they could be edited
        SerializersOnBlackboard serializers = SerializersOnBlackboard.create(blackboard);
        if (context.getExpectedType()!=null) {
            config.typeRegistry.collectSerializers(context.getExpectedType(), serializers.expectedTypeSerializers, MutableSet.<String>of());
        }
        serializers.postSerializers.addAll(config.serializersPost);
        
        if (context instanceof YormlContextForRead) {
            // read needs instantiated so that these errors display before manipulating errors and others
            ReadingTypeOnBlackboard.get(blackboard);
        }
        
        while (context.phaseAdvance()) {
            while (context.phaseStepAdvance()<Iterables.size(serializers.getSerializers())) {
                YormlSerializer s = Iterables.get(serializers.getSerializers(), context.phaseCurrentStep());
                if (context instanceof YormlContextForRead) {
                    s.read((YormlContextForRead)context, this, blackboard);
                } else {
                    if (log.isDebugEnabled())
                        log.debug("write "+context.getJsonPath()+"/ = "+context.getJavaObject()+" serializer "+s+" starting ("+context.phaseCurrent()+"."+context.phaseCurrentStep()+") ");
                    s.write((YormlContextForWrite)context, this, blackboard);
                    if (log.isDebugEnabled())
                        log.debug("write "+context.getJsonPath()+"/ = "+context.getJavaObject()+" serializer "+s+" ended: "+context.getYamlObject());
                }
            }
        }
        
        checkCompletion(context, blackboard);
    }

    protected void checkCompletion(YormlContext context, Map<Object, Object> blackboard) {
        for (Object bo: blackboard.values()) {
            if (bo instanceof YormlRequirement) {
                ((YormlRequirement)bo).checkCompletion(context);
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

    public YormlConfig getConfig() {
        return config;
    }

}
