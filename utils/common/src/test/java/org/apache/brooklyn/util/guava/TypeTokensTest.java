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
package org.apache.brooklyn.util.guava;

import com.google.common.base.Optional;
import com.google.common.reflect.TypeToken;
import java.util.List;
import java.util.Map;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.UserFacingException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TypeTokensTest {

    @Test
    public void testGetGenericArguments() {
        TypeToken<?> t = new TypeToken<Map<String,Integer>>() {};
        List<TypeToken<?>> genericArguments = TypeTokens.getGenericArguments(t);
        Assert.assertEquals(genericArguments, MutableList.of(TypeToken.of(String.class), TypeToken.of(Integer.class)));
    }

    @Test
    public void testLowestCommonAncestor() {
        TypeToken<?> map1 = new TypeToken<Map<String,Integer>>() {};
        Assert.assertEquals( TypeTokens.union(TypeToken.of(MutableMap.class), map1, true), map1);
    }

}
