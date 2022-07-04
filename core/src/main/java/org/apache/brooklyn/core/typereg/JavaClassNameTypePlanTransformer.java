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
package org.apache.brooklyn.core.typereg;

import java.lang.reflect.Constructor;
import java.util.Map;

import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;

/**
 * Instantiates classes from a registered type which simply
 * defines the java class name and OSGi bundles to use.
 * <p>
 * This is used where a {@link RegisteredType} is defined simply with the name of a java class
 * e.g. with a no-arg constructor -- no YAML etc just the name of the class.
 */
public class JavaClassNameTypePlanTransformer extends AbstractTypePlanTransformer {
    
    public static final String FORMAT = "java-type-name";

    public static class JavaClassNameTypeImplementationPlan extends AbstractFormatSpecificTypeImplementationPlan<String> {
        public JavaClassNameTypeImplementationPlan(String javaType) { super(FORMAT, javaType); }
    }

    public JavaClassNameTypePlanTransformer() {
        super(FORMAT, "Java type name", "Expects a java type name in a format suitable for use with ClassLoader.loadClass");
    }

    @Override
    protected double scoreForNullFormat(Object planData, RegisteredType type, RegisteredTypeLoadingContext context) {
        // the "good" regex doesn't allow funny unicode chars; we'll accept that for now 
        if (type.getPlan().getPlanData() instanceof String && 
                ((String)type.getPlan().getPlanData()).matches(Identifiers.JAVA_GOOD_BINARY_REGEX)) {
            return 0.1;
        }
        return 0;
    }
    
    @Override
    protected double scoreForNonmatchingNonnullFormat(String planFormat, Object planData, RegisteredType type, RegisteredTypeLoadingContext context) {
        return 0;
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    protected AbstractBrooklynObjectSpec<?,?> createSpec(RegisteredType type, RegisteredTypeLoadingContext context) throws Exception {
        return decorateWithCommonTagsModifyingSpecSummary(RegisteredTypes.newSpecInstance(mgmt, (Class<? extends BrooklynObject>) getType(type, context)), type, null, null, null);
    }

    @Override
    protected Object createBean(RegisteredType type, RegisteredTypeLoadingContext context) throws Exception {
        Class<?> clz = getType(type, context);
        Constructor<?> constr;
        try {
            constr = clz.getDeclaredConstructor();
        } catch (Exception e) {
            throw Exceptions.propagateAnnotated("No 0-arg constructor found for "+clz+" for "+type.getId(), e);
        }
        return constr.newInstance();
    }

    private Class<?> getType(RegisteredType type, RegisteredTypeLoadingContext context) throws Exception {
        String planData = ((String)type.getPlan().getPlanData()).trim();

        // expects a string; caller might have given us 'type: xxx' yaml, or automatically wrapped;
        // in those simple cases, unpack it
        try {
            Iterable<Object> yaml = Yamls.parseAll(planData);
            if (Iterables.size(yaml) == 1) {
                Object ym = yaml.iterator().next();
                if (ym instanceof Map) {
                    Object yt = ((Map) ym).get("type");
                    if (yt instanceof String) {
                        planData = (String) yt;
                    }
                }
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            // swallow if not parseable yaml; treat as string
        }

        return RegisteredTypes.loadActualJavaType(planData, mgmt, type, context);
    }
    
    
}
