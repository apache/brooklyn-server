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

import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.mgmt.classloading.OsgiBrooklynClassLoadingContext;
import org.apache.brooklyn.core.typereg.AbstractTypePlanTransformer;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BeanWithTypePlanTransformer extends AbstractTypePlanTransformer {

    public static final String FORMAT = BeanWithTypeUtils.FORMAT;

    public BeanWithTypePlanTransformer() {
        super(FORMAT, "Brooklyn YAML serialized with type",
                "Bean serialization format read/written as YAML or JSON as per Jackson with one trick that the 'type' field specifies the type or supertype");
    }

    @Override
    protected double scoreForNullFormat(Object o, RegisteredType registeredType, RegisteredTypeLoadingContext registeredTypeLoadingContext) {
        if (registeredType.getKind()!= RegisteredTypeKind.SPEC && Strings.toString(o).contains("type:")) {
            return 0.2;
        } else {
            return 0;
        }
    }

    @Override
    protected double scoreForNonmatchingNonnullFormat(String s, Object o, RegisteredType registeredType, RegisteredTypeLoadingContext registeredTypeLoadingContext) {
        return 0;
    }

    @Override
    protected AbstractBrooklynObjectSpec<?, ?> createSpec(RegisteredType registeredType, RegisteredTypeLoadingContext registeredTypeLoadingContext) throws Exception {
        throw new IllegalStateException("spec not supported by this transformer");
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

        return BeanWithTypeUtils.newMapper(mgmt, true, loader, true).readValue(
                BeanWithTypeUtils.newSimpleMapper().writeValueAsString(definition), Object.class);
    }

}
