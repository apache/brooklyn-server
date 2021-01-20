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

import com.google.common.reflect.TypeToken;
import java.util.List;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.guava.TypeTokens;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BrooklynJacksonTypeTest {

    @Test
    public void testTypeTokensGenericStrings() {
        Assert.assertEquals(TypeTokens.getGenericParameterTypeTokensWhenUpcastToClassRaw(new TypeToken<List<String>>() {}, Iterable.class),
                new TypeToken<?>[] { TypeToken.of(String.class) });

        Assert.assertEquals(TypeTokens.getGenericParameterTypeTokensWhenUpcastToClassRaw(parseTestTypes("list<string>"), Iterable.class),
                new TypeToken<?>[] { TypeToken.of(String.class) });
    }

    @Test
    public void testTypeTokensGenericRegisteredType() {
        TypeToken<?> foo = parseTestTypes("foo");
        Asserts.assertStringContainsIgnoreCase(foo.toString(), "foo:1", "Object");

        TypeToken<?>[] fooG = TypeTokens.getGenericParameterTypeTokensWhenUpcastToClassRaw(parseTestTypes("list<foo>"), Iterable.class);
        Assert.assertEquals(fooG, new TypeToken<?>[] {foo});
    }

    public static TypeToken<?> parseTestTypes(String s1) {
        return BrooklynTypeNameResolution.parseTypeToken(s1, s2 -> {
            if ("foo".equals(s2))
                return Maybe.of(BrooklynJacksonType.asTypeToken(RegisteredTypes.bean("foo", "1", new BasicTypeImplementationPlan("x", null))));
            if ("iterable".equals(s2)) return Maybe.of(TypeToken.of(Iterable.class));
            return BrooklynTypeNameResolution.getTypeTokenForBuiltInTypeName(s2);
        }).get();
    }

}
