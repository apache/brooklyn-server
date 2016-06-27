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
package org.apache.brooklyn.util.yorml;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.yorml.YormlInternals.YormlContinuation;
import org.apache.brooklyn.util.yorml.serializers.ReadingTypeOnBlackboard;

public class YormlConverter {

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
        List<YormlSerializer> serializers = getSerializers(context);
        int i=0;
        Map<Object,Object> blackboard = MutableMap.of();
        ReadingTypeOnBlackboard.get(blackboard);
        
        while (i<serializers.size()) {
            YormlSerializer s = serializers.get(i);
            YormlContinuation next = s.read(context, this, blackboard);
            if (next == YormlContinuation.FINISHED) break;
            else if (next == YormlContinuation.RESTART) i=0;
            else i++;
        }
        checkCompletion(context, blackboard);
        return context.getJavaObject();
    }

    private List<YormlSerializer> getSerializers(YormlContext context) {
        MutableList<YormlSerializer> serializers = MutableList.<YormlSerializer>of();
        if (context.getExpectedType()!=null) {
            serializers.appendAll(config.typeRegistry.getAllSerializers(context.getExpectedType()));
        }
        serializers.appendAll(config.serializersPost);
        return serializers;
    }

    protected void checkCompletion(YormlContext context, Map<Object, Object> blackboard) {
        for (Object bo: blackboard.values()) {
            if (bo instanceof YormlRequirement) {
                ((YormlRequirement)bo).checkCompletion(context);
            }
        }
    }

    /**
     * returns jsonable object (map, list, primitive) 
     */   
    public Object write(YormlContextForWrite context) {
        List<YormlSerializer> serializers = getSerializers(context);
        int i=0;
        Map<Object,Object> blackboard = MutableMap.of();
        while (i<serializers.size()) {
            YormlSerializer s = serializers.get(i);
            System.out.println("write "+context.getJsonPath()+"/ = "+context.getJavaObject()+" serializer "+i+" "+s+" starting");
            YormlContinuation next = s.write(context, this, blackboard);
            System.out.println("write "+context.getJsonPath()+"/ = "+context.getJavaObject()+" serializer "+i+" "+s+" ended: "+context.getYamlObject());
            if (next == YormlContinuation.FINISHED) break;
            else if (next == YormlContinuation.RESTART) i=0;
            else i++;
        }
        checkCompletion(context, blackboard);
        return context.getYamlObject();
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
