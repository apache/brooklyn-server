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
package org.apache.brooklyn.camp.yoml;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.core.typereg.AbstractFormatSpecificTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.AbstractTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.Yoml;
import org.apache.brooklyn.util.yoml.YomlException;
import org.apache.brooklyn.util.yoml.YomlSerializer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/** 
 * Makes it possible for Brooklyn to resolve YOML items,
 * both types registered with the system using YOML
 * and plans in the YOML format (sent here as unregistered types),
 * and supporting any objects and special handling for "spec" objects.
 * 
 * DONE
 * - test programmatic addition and parse (beans) with manual objects
 * - figure out supertypes and use that to determine java type
 * - attach custom serializers (on plan)
 * - test serializers
 * - support serializers by annotation
 * - set serializers when adding to catalog and test
 * 
 * NEXT
 * - $brooklyn:object(format): <object-in-given-format>
 * - catalog impl supports format
 * 
 * (initdish can now be made to work)
 *   
 * THEN
 * - support specs from yoml
 * - type registry api, add arbitrary types via yoml, specifying format
 * - catalog impl in yoml as test?
 * - REST API for deploy accepts specific format, can call yoml (can we test this earlier?)
 * 
 * AND THEN
 * - generate its own documentation
 * - persistence switches to using yoml, warns if any types are not yoml-ized
 * - yoml allows `constructor: [list]` and `constructor: { mode: static, type: factory, method: newInstance, args: ... }`
 *   and maybe even `constructor: { mode: chain, steps: [ { mode: constructor, type: Foo.Builder }, { mode: method, method: bar, args: [ true ] }, { mode: method, method: build } ] }`  
 * - type access control -- ie restrict who can see what types
 * - java instantiation access control - ie permission required to access java in custom types (deployed or added to catalog)
 */
public class YomlTypePlanTransformer extends AbstractTypePlanTransformer {

    private static final List<String> FORMATS = ImmutableList.of("yoml");
    
    public static final String FORMAT = FORMATS.get(0);
    
    public YomlTypePlanTransformer() {
        super(FORMAT, "YOML Brooklyn syntax", "Standard YOML adapters for Apache Brooklyn including OASIS CAMP");
    }

    @Override
    protected double scoreForNullFormat(Object planData, RegisteredType type, RegisteredTypeLoadingContext context) {
        int score = 0;
        Maybe<Map<?,?>> plan = RegisteredTypes.getAsYamlMap(planData);
        if (plan.isPresent()) {
            score += 1;
            if (plan.get().containsKey("type")) score += 5;
            if (plan.get().containsKey("services")) score += 2;
        }
        if (type instanceof YomlTypeImplementationPlan) score += 100;
        return (1.0 - 8.0/(score+8));
    }

    @Override
    protected double scoreForNonmatchingNonnullFormat(String planFormat, Object planData, RegisteredType type, RegisteredTypeLoadingContext context) {
        if (FORMATS.contains(planFormat.toLowerCase())) return 0.9;
        return 0;
    }

    @Override
    protected AbstractBrooklynObjectSpec<?, ?> createSpec(RegisteredType type, RegisteredTypeLoadingContext context) throws Exception {
        // TODO
        return null;
    }

    @Override
    protected Object createBean(RegisteredType type, RegisteredTypeLoadingContext context) throws Exception {
        BrooklynYomlTypeRegistry tr = new BrooklynYomlTypeRegistry(mgmt, context);
        Yoml y = Yoml.newInstance(tr);
        // TODO could cache the parse, could cache the instantiation instructions
        Object data = type.getPlan().getPlanData();
        
        Class<?> expectedSuperType = context.getExpectedJavaSuperType();
        String expectedSuperTypeName = tr.getTypeNameOfClass(expectedSuperType);
        
        if (data==null || (data instanceof String)) {
            if (Strings.isBlank((String)data)) {
                // blank plan means to use the java type
                Maybe<Class<?>> jt = tr.getJavaTypeInternal(type, context);
                if (jt.isAbsent()) throw new YomlException("Type '"+type+"' has no plan or java type in its definition");
                try {
                    return jt.get().newInstance();
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    throw new YomlException("Type '"+type+"' has no plan and its java type cannot be instantiated", e);
                }
            }
            
            // normal processing
            return y.read((String) data, expectedSuperTypeName);
        }
        
        // could do this
//      return y.readFromYamlObject(data, expectedSuperTypeName);
        // but we require always a string
        throw new IllegalArgumentException("The implementation plan for '"+type+"' should be a string in order to process as YOML");
    }
    
    @Override
    public double scoreForTypeDefinition(String formatCode, Object catalogData) {
        // TODO catalog parsing
        return 0;
    }

    @Override
    public List<RegisteredType> createFromTypeDefinition(String formatCode, Object catalogData) {
        // TODO catalog parsing
        return null;
    }

    static class YomlTypeImplementationPlan extends AbstractFormatSpecificTypeImplementationPlan<String> {
        final String javaType;
        final List<YomlSerializer> serializers;
        
        public YomlTypeImplementationPlan(String planData, Class<?> javaType, Iterable<YomlSerializer> serializers) {
            super(FORMATS.get(0), planData);
            this.javaType = Preconditions.checkNotNull(javaType).getName();
            this.serializers = MutableList.copyOf(serializers);
        }
    }
}
