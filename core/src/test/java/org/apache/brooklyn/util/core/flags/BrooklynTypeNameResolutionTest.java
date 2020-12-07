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
package org.apache.brooklyn.util.core.flags;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import java.util.List;
import java.util.function.Function;
import static org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution.parseTypeGenerics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BrooklynTypeNameResolutionTest {

    public static final Logger log = LoggerFactory.getLogger(BrooklynTypeNameResolutionTest.class);

    @Test
    public void testParse() throws NoSuchFieldException {
        Assert.assertEquals(parseTypeGenerics("t1").toString(), "t1");
        Assert.assertEquals(parseTypeGenerics(" t1 ").toString(), "t1");

        Assert.assertEquals(parseTypeGenerics(" t1 < a >").toString(), "t1<a>");
        Assert.assertEquals(parseTypeGenerics(" t1 < a >").baseName.toString(), "t1");
        Assert.assertEquals(Iterables.getOnlyElement(parseTypeGenerics(" t1 < a >").params).toString(), "a");

        Assert.assertEquals(parseTypeGenerics(" t1 < t2<t3>, t4,t5< t6 > >").toString(), "t1<t2<t3>,t4,t5<t6>>");
    }

    @Test
    public void testMakeTypeToken() throws NoSuchFieldException {
        Function<String,TypeToken<?>> parse = s1 -> BrooklynTypeNameResolution.parseTypeToken(s1, s2 -> BrooklynTypeNameResolution.getClassForBuiltInTypeName(s2).get());

        Assert.assertEquals(parse.apply("string"), TypeToken.of(String.class));
        Assert.assertEquals(parse.apply("list<string>"), new TypeToken<List<String>>() {});
    }

}