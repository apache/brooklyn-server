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
package org.apache.brooklyn.core.resolve.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.typereg.AbstractTypePlanTransformer;
import org.apache.brooklyn.core.typereg.UnsupportedTypePlanException;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;

import java.util.Iterator;
import java.util.Map;

public class BeanWithTypePlanTransformer extends AbstractTypePlanTransformer {

    public static final String FORMAT = BeanWithTypeUtils.FORMAT;
    public static final String TYPE_SIMPLE_KEY = "type";
    public static final String TYPE_UNAMBIGUOUS_KEY = AsPropertyIfAmbiguous.CONFLICTING_TYPE_NAME_PROPERTY_TRANSFORM.apply("type");

    public BeanWithTypePlanTransformer() {
        super(FORMAT, "Brooklyn YAML serialized with type",
                "Bean serialization format read/written as YAML or JSON as per Jackson with one trick that the '"+TYPE_SIMPLE_KEY+"' field specifies the type or supertype");
    }

    @Override
    protected double scoreForNullFormat(Object o, RegisteredType registeredType, RegisteredTypeLoadingContext registeredTypeLoadingContext) {
        if (registeredType.getKind()!= RegisteredTypeKind.SPEC) {
            if (Strings.toString(o).contains(TYPE_UNAMBIGUOUS_KEY+":")) {
                return 0.8;
            }
            if (Strings.toString(o).contains(TYPE_SIMPLE_KEY+":")) {
                return 0.5;
            }
            if (Strings.toString(o).contains(AsPropertyIfAmbiguous.CONFLICTING_TYPE_NAME_PROPERTY_TRANSFORM_ALT.apply(TYPE_SIMPLE_KEY))) {
                return 0.4;
            }
        }
        return 0;
    }

    @Override
    protected double scoreForNonmatchingNonnullFormat(String s, Object o, RegisteredType registeredType, RegisteredTypeLoadingContext registeredTypeLoadingContext) {
        return 0;
    }

    @Override
    public double scoreForType(RegisteredType type, RegisteredTypeLoadingContext context) {
        if (RegisteredTypeKind.SPEC.equals(type.getKind())) return 0;
        return super.scoreForType(type, context);
    }

    @Override
    protected AbstractBrooklynObjectSpec<?, ?> createSpec(RegisteredType registeredType, RegisteredTypeLoadingContext registeredTypeLoadingContext) throws Exception {
        throw new UnsupportedTypePlanException("spec not supported by this bean transformer");
    }

    @Override
    protected Object createBean(RegisteredType registeredType, RegisteredTypeLoadingContext registeredTypeLoadingContext) throws Exception {
        Map<String,Object> definition;
        try {
            Iterable<Object> parsed;
            parsed = Yamls.parseAll((String)registeredType.getPlan().getPlanData());
            Iterator<Object> pi = parsed.iterator();
            if (!pi.hasNext()) throw new IllegalStateException("No data");
            definition = (Map<String, Object>) pi.next();
            if (pi.hasNext()) throw new IllegalStateException("YAML contained multiple items");
        } catch (Exception e) {
            throw Exceptions.propagateAnnotated("Invalid YAML in definition of '"+registeredType.getId()+"'", e);
        }


        BrooklynClassLoadingContext loader = registeredTypeLoadingContext != null ? registeredTypeLoadingContext.getLoader() : null;
        loader = CatalogUtils.newClassLoadingContext(mgmt, registeredType, loader);

        String definitionMapSerializedAsString = BeanWithTypeUtils.newSimpleMapper().writeValueAsString(definition);
        ObjectMapper mapper = BeanWithTypeUtils.newMapper(mgmt, true, loader, true);
        Object result = mapper.readValue(definitionMapSerializedAsString, Object.class);

        Object typeS = definition.get("type");
        if (typeS instanceof String) {
            // if size is 1, it's a weird thing like a ListExtended, so don't try to deserialize from an empty object
            TypeToken<?> expectedType = new BrooklynTypeNameResolution.BrooklynTypeNameResolver("creating-bean-"+registeredType, mgmt, loader, true, true)
                    .getTypeToken((String) typeS);
            if (!expectedType.getRawType().isInstance(result)) {
                // we did not get the expected type; this should throw an error, though maybe it will work
                definition.remove("type");
                definitionMapSerializedAsString = BeanWithTypeUtils.newSimpleMapper().writeValueAsString(definition);
                result = mapper.readValue(definitionMapSerializedAsString, BrooklynJacksonType.asJavaType(mapper, expectedType));
            }
        }
        return result;
    }

}
