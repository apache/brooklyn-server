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
package org.apache.brooklyn.util.yoml.serializers;

import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.annotations.YomlAnnotations;
import org.apache.brooklyn.util.yoml.internal.SerializersOnBlackboard;

/* Adds ExplicitField instances for all fields declared on the type */
@Alias("all-fields-explicit")
public class AllFieldsExplicit extends YomlSerializerComposition {

    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }
    
    /** marker on blackboard indicating that we have run */ 
    static class DoneAllFieldsExplicit {}
    
    public class Worker extends YomlSerializerWorker {
        
        public void read() { run(); }
        public void write() { run(); }
        
        protected void run() {
            if (!hasJavaObject()) return;
            if (blackboard.containsKey(DoneAllFieldsExplicit.class.getName())) return;
            
            // mark done
            blackboard.put(DoneAllFieldsExplicit.class.getName(), new DoneAllFieldsExplicit());
            
            SerializersOnBlackboard.get(blackboard).addInstantiatedTypeSerializers(
                new YomlAnnotations().findExplicitFieldSerializers(getJavaObject().getClass(), false));

            context.phaseRestart();
        }
    }
    
}
