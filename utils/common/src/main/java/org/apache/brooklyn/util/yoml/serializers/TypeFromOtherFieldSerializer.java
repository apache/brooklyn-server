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
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsTopLevel;
import org.apache.brooklyn.util.yoml.annotations.YomlTypeFromOtherField;
import org.apache.brooklyn.util.yoml.internal.YomlContext.StandardPhases;

/** Populates a blackboard recording the fact, for use by {@link FieldsInMapUnderFields} */
@YomlAllFieldsTopLevel
@Alias("type-from-other-field")
public class TypeFromOtherFieldSerializer extends YomlSerializerComposition {

    public TypeFromOtherFieldSerializer() {}
    public TypeFromOtherFieldSerializer(String fieldName, YomlTypeFromOtherField otherFieldInfo) {
        this(fieldName, otherFieldInfo.value(), otherFieldInfo.real());
    }
    public TypeFromOtherFieldSerializer(String fieldNameToDecorate, String fieldNameContainingType, boolean isFieldReal) {
        this.field = fieldNameToDecorate;
        this.typeField = fieldNameContainingType;
        this.typeFieldReal = isFieldReal;
    }

    String field;
    String typeField;
    boolean typeFieldReal;
    
    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }
    
    public class Worker extends YomlSerializerWorker {
        
        public void go() {
            // probably runs too often but optimize that later
            TypeFromOtherFieldBlackboard.get(blackboard).setTypeConstraint(field, typeField, typeFieldReal);
        }
        
        public void read() { 
            if (!context.isPhase(StandardPhases.MANIPULATING)) return;
            if (!isYamlMap()) return;
            go();
        }
        
        public void write() { 
            if (!context.isPhase(StandardPhases.HANDLING_TYPE)) return;
            go(); 
        }

    }

    @Override
    public String toString() {
        return super.toString()+"["+field+"<-"+typeField+"]";
    }
}
