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
package org.apache.brooklyn.util.core.xstream;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.thoughtworks.xstream.XStream;
import java.util.Map;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.testng.annotations.Test;

public class TransformedMapConverterTest extends ConverterTestFixture {

    @Override
    protected void registerConverters(XStream xstream) {
        super.registerConverters(xstream);

        xstream.alias("MutableMap", MutableMap.class);
        xstream.registerConverter(new StringKeyMapConverter(xstream.getMapper()), /* priority */ 10);

        xstream.alias("MutableSet", MutableSet.class);
        xstream.registerConverter(new MutableSetConverter(xstream.getMapper()));
    }

    private static class AddOne implements Function<Integer, Integer> {
        @Override
        public @Nullable Integer apply(@Nullable Integer input) {
            return input+1;
        }
    }


    @Test
    public void testGuavaPortableSerialization_MapTranformValues() {
        useNewCachedXstream();
        XmlSerializer.addStandardInnerClassHelpers(cachedXstream);

        Function<String,String> xml = clazz -> "<com.google.common.collect.Maps_-TransformedEntriesMap>\n" +
                "  <fromMap class=\"MutableMap\">\n" +
                "    <a type=\"int\">1</a>\n" +
                "  </fromMap>\n" +
                "  <transformer class=\""+clazz+"\">\n" +
                "    <val_-function class=\"org.apache.brooklyn.util.core.xstream.TransformedMapConverterTest$AddOne\"/>\n" +
                "  </transformer>\n" +
                "</com.google.common.collect.Maps_-TransformedEntriesMap>";

        Map<String, Integer> out = Maps.transformValues(MutableMap.of("a", 1), new AddOne());
        String preferred = xml.apply("com.google.common.collect.Maps._inners.valueTransformer");

        // IMPORTANT! for backwards compatibility
        String guava18 = xml.apply("com.google.common.collect.Maps$7");

        // NOT IMPORTANT - for reference; should not be written, so if guava changes this can be changed
        String guava27 = xml.apply("com.google.common.collect.Maps$9");

        assertX(out, preferred, guava18, guava27);
    }

    @Test
    public void testGuavaPortableSerialization_IterablesTranform() {
        useNewCachedXstream();
        XmlSerializer.addStandardInnerClassHelpers(cachedXstream);

        Iterable<Integer> out = Iterables.transform(MutableSet.of(1), new AddOne());
        Function<String,String> xml = clazz -> "<"+clazz+">\n" +
                "  <iterableDelegate class=\"com.google.common.base.Absent\"/>\n" +
                "  <val_-fromIterable class=\"MutableSet\">\n" +
                "    <int>1</int>\n" +
                "  </val_-fromIterable>\n" +
                "  <val_-function class=\"org.apache.brooklyn.util.core.xstream.TransformedMapConverterTest$AddOne\"/>\n" +
                "</"+clazz+">";

        String preferred = xml.apply("com.google.common.collect.Iterables.__inners.transform");

        // NOT IMPORTANT - for reference; should not be written, so if guava changes this can be changed
        // (note $ becomes_- when used as tag, since $ not allowed there)
        String guava27 = xml.apply("com.google.common.collect.Iterables_-5");

        assertX(out, (i1,i2) -> {
            Asserts.assertEquals(MutableList.copyOf(i1), MutableList.copyOf(i2));
            return true;
        }, preferred, guava27);
    }

}
