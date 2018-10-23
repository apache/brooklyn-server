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
package org.apache.brooklyn.util.javalang;

import org.assertj.core.api.WithAssertions;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Tests for {@link Reflections#findMappedNameAndLog(Map, String)} and
 * {@link Reflections#findMappedNameMaybe(Map, String)} methods.
 */
public class ReflectionsFindMappedNameTest  implements WithAssertions {

    @Test
    public void givenRenamesIsNullThenReturnOriginalName() {
        //given
        final Map<String, String> renames = null;
        final String originalName = createAName();
        //when
        final String result = Reflections.findMappedNameAndLog(renames, originalName);
        //then
        assertThat(result).isEqualTo(originalName);
    }

    @Test
    public void givenRenamesIsEmptyThenReturnOriginalName() {
        //given
        final Map<String, String> renames = new HashMap<>();
        final String originalName = createAName();
        //when
        final String result = Reflections.findMappedNameAndLog(renames, originalName);
        //then
        assertThat(result).isEqualTo(originalName);
    }

    @Test
    public void givenRenamesHasNoMatchThenReturnOriginalName() {
        //given
        final Map<String, String> renames = new HashMap<>();
        renames.put(createAName(), createAName());
        final String originalName = createAName();
        //when
        final String result = Reflections.findMappedNameAndLog(renames, originalName);
        //then
        assertThat(result).isEqualTo(originalName);
    }

    @Test
    public void givenRenamesHasMatchThenReturnUpdatedName() {
        //given
        final Map<String, String> renames = new HashMap<>();
        final String originalName = createAName();
        final String updatedName = createAName();
        renames.put(originalName, updatedName);
        //when
        final String result = Reflections.findMappedNameAndLog(renames, originalName);
        //then
        assertThat(result).isEqualTo(updatedName);
    }

    @Test
    public void givenInnerClassHasNoMatchThenReturnOriginalName() {
        //given
        final Map<String, String> renames = new HashMap<>();
        final String originalName = createAName() + "$" + createAName();
        //when
        final String result = Reflections.findMappedNameAndLog(renames, originalName);
        //then
        assertThat(result).isEqualTo(originalName);
    }

    @Test
    public void givenInnerClassHasMatchThenReturnUpdatedName() {
        //given
        final Map<String, String> renames = new HashMap<>();
        final String originalName = createAName() + "$" + createAName();
        final String updatedName = createAName();
        renames.put(originalName, updatedName);
        //when
        final String result = Reflections.findMappedNameAndLog(renames, originalName);
        //then
        assertThat(result).isEqualTo(updatedName);
    }

    @Test
    public void givenInnerClassWhenOnlyOuterClassHasMatchThenReturnUpdatedName() {
        //given
        final Map<String, String> renames = new HashMap<>();
        final String originalOuterClass = createAName();
        final String originalInnerClass = createAName();
        final String originalName = originalOuterClass + "$" + originalInnerClass;
        final String updatedOuterClass = createAName();
        final String updatedName = updatedOuterClass + "$" + originalInnerClass;
        renames.put(originalOuterClass, updatedOuterClass);
        //when
        final String result = Reflections.findMappedNameAndLog(renames, originalName);
        //then
        assertThat(result).isEqualTo(updatedName);
    }

    @Test
    public void givenInnerClassWhenOuterAndInnerClassHasMatchThenReturnInnerClassUpdatedName() {
        //given
        final Map<String, String> renames = new HashMap<>();
        // the outer class has been renamed
        final String originalOuterClass = createAName();
        final String updatedOuterClass = createAName();
        renames.put(originalOuterClass, updatedOuterClass);
        // the inner class has an explicit rename to a different outer class
        final String originalInnerClass = createAName();
        final String originalName = originalOuterClass + "$" + originalInnerClass;
        final String updatedName = createAName() + "$" + createAName();
        renames.put(originalName, updatedName);
        //when
        final String result = Reflections.findMappedNameAndLog(renames, originalName);
        //then
        // the explicit rename overrides the outer class rename
        assertThat(result).isEqualTo(updatedName);
    }

    //wrap_blah_aws-java-sdk-bundle-1.11.245.jar\:*  : wrap_blah_aws-java-sdk-bundle-1.11.411.jar\:*
    @Test
    public void allClassesInOneBundleAreNowInOtherBundle() {
        //given
        final Map<String, String> renames = new HashMap<>();
        final String wildcard = "*";
        final String originalBundle = "wrap_blah_aws-java-sdk-bundle-1.11.245.jar:";
        final String updatedBundle = "wrap_blah_aws-java-sdk-bundle-1.11.411.jar:";
        renames.put(originalBundle + wildcard, updatedBundle + wildcard);
        final String className = createAName();
        final String originalName = originalBundle + className;
        final String updatedName = updatedBundle + className;
        //when
        final String result = Reflections.findMappedNameAndLog(renames, originalName);
        //then
        assertThat(result).isSameAs(updatedName);
    }

    private String createAName() {
        return UUID.randomUUID().toString();
    }

}
