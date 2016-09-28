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
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslInterpreter;
import org.apache.brooklyn.camp.spi.resolve.PlanInterpreter;
import org.apache.brooklyn.camp.spi.resolve.interpret.PlanInterpretationContext;
import org.apache.brooklyn.camp.spi.resolve.interpret.PlanInterpretationNode;
import org.apache.brooklyn.core.typereg.AbstractFormatSpecificTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.AbstractTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.typereg.UnsupportedTypePlanException;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.task.ValueResolver;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.Yoml;
import org.apache.brooklyn.util.yoml.YomlConfig;
import org.apache.brooklyn.util.yoml.YomlException;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstruction;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstructions;
import org.apache.brooklyn.util.yoml.internal.YomlConfigs.Builder;
import org.yaml.snakeyaml.Yaml;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

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
 * - $brooklyn:object-yoml: <object-in-given-format>
 * - support $brooklyn DSL in yoml
 * 
 * NEXT
 * - catalog impl supports format
 * 
 * (initdish can now be made to work)
 *   
 * THEN
 * - update docs for above, including $brooklyn:object-yoml, yoml overview
 * - support specs from yoml
 * - type registry api, add arbitrary types via yoml, specifying format
 * - catalog impl in yoml as test?
 * - type registry persistence
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

    private final static Set<String> IGNORE_SINGLE_KEYS = ImmutableSet.of("name", "version");
    
    @Override
    protected double scoreForNullFormat(Object planData, RegisteredType type, RegisteredTypeLoadingContext context) {
        int score = 0;
        Maybe<Map<?,?>> plan = RegisteredTypes.getAsYamlMap(planData);
        if (plan.isPresent()) {
            if (plan.get().size()>1 || (plan.get().size()==1 && !IGNORE_SINGLE_KEYS.contains(plan.get().keySet().iterator().next()))) {
                // weed out obvious bad plans -- in part so that tests pass
                // TODO in future we should give a tiny score to anything else (once we want to enable this as a popular auto-detetction target)
//                score += 1;
                // but for now we require at least one recognised keyword
            }
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
        throw new UnsupportedTypePlanException("YOML doesn't yet support specs");
    }

    @Override
    protected Object createBean(RegisteredType type, RegisteredTypeLoadingContext context) throws Exception {
        Yoml y = Yoml.newInstance(newYomlConfig(mgmt, context).build());
        
        // TODO could cache the parse, could cache the instantiation instructions
        Object data = type.getPlan().getPlanData();
        
        Class<?> expectedSuperType = context.getExpectedJavaSuperType();
        String expectedSuperTypeName = y.getConfig().getTypeRegistry().getTypeNameOfClass(expectedSuperType);
        
        Object parsedInput;
        if (data==null || (data instanceof String)) {
            if (Strings.isBlank((String)data)) {
                // blank plan means to use the java type / construction instruction
                Maybe<Class<?>> javaType = ((BrooklynYomlTypeRegistry) y.getConfig().getTypeRegistry()).getJavaTypeInternal(type, context); 
                ConstructionInstruction constructor = context.getConstructorInstruction();
                if (javaType.isAbsent() && constructor==null) {
                    return Maybe.absent(new IllegalStateException("Type "+type+" has no plan YAML and error in type", ((Maybe.Absent<?>)javaType).getException()));
                }
                
                Maybe<Object> result = ConstructionInstructions.Factory.newDefault(javaType.get(), constructor).create();

                if (result.isAbsent()) {
                    throw new YomlException("Type '"+type+"' has no plan and its java type cannot be instantiated", ((Maybe.Absent<?>)result).getException());
                }
                return result.get();
            }
            
            // else we need to parse to json objects, then translate it (below)
            parsedInput = new Yaml().load((String) data);
            
        } else {
            // we do this (supporint non-string and pre-parsed plans) in order to support the YAML DSL 
            // and other limited use cases (see DslYomlObject);
            // NB this is only for ad hoc plans (code above) and we may deprecate it altogether as soon as we can 
            // get the underlying string in the $brooklyn DSL context
            
            if (type.getSymbolicName()!=null) {
                throw new IllegalArgumentException("The implementation plan for '"+type+"' should be a string in order to process as YOML");
            }
            parsedInput = data;
            
        }
        
        // in either case, now run interpreters (dsl) if it's a m=ap, then translate 
        
        if (parsedInput instanceof Map) {
            List<PlanInterpreter> interpreters = MutableList.<PlanInterpreter>of(new BrooklynDslInterpreter());
            @SuppressWarnings("unchecked")
            PlanInterpretationNode interpretation = new PlanInterpretationNode(
                new PlanInterpretationContext((Map<String,?>)parsedInput, interpreters));
            parsedInput = interpretation.getNewValue();
        }
        
        return y.readFromYamlObject(parsedInput, expectedSuperTypeName);
    }

    @Beta @VisibleForTesting
    public static Builder<YomlConfig.Builder> newYomlConfig(@Nonnull ManagementContext mgmt) {
        return newYomlConfig(mgmt, (RegisteredTypeLoadingContext)null);
    }
    
    @Beta @VisibleForTesting
    public static Builder<YomlConfig.Builder> newYomlConfig(@Nonnull ManagementContext mgmt, @Nullable RegisteredTypeLoadingContext context) {
        if (context==null) context = RegisteredTypeLoadingContexts.any();
        BrooklynYomlTypeRegistry tr = new BrooklynYomlTypeRegistry(mgmt, context);
        return newYomlConfig(mgmt, tr);
    }

    static YomlConfig.Builder newYomlConfig(ManagementContext mgmt, BrooklynYomlTypeRegistry typeRegistry) {
        return YomlConfig.Builder.builder().typeRegistry(typeRegistry).
            serializersPostAddDefaults().
            // TODO any custom serializers?
            coercer(new ValueResolver.ResolvingTypeCoercer());
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
