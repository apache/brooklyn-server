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

package org.apache.brooklyn.location.jclouds.templates.customize;

import org.apache.brooklyn.location.jclouds.JcloudsLocationConfig;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Enums;
import org.apache.brooklyn.util.text.ByteSizeStrings;
import org.apache.brooklyn.util.text.Strings;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.compute.domain.TemplateBuilderSpec;

public class TemplateBuilderCustomizers {

    private TemplateBuilderCustomizers() {
    }

    public static TemplateBuilderCustomizer hardwareId() {
        return new HardwareIdTemplateBuilder();
    }

    public static TemplateBuilderCustomizer imageDescription() {
        return new ImageDescriptionRegexTemplateBuilder();
    }

    public static TemplateBuilderCustomizer imageId() {
        return new ImageIdTemplateBuilder();
    }

    public static TemplateBuilderCustomizer imageNameRegex() {
        return new ImageNameRegexTemplateBuilder();
    }

    public static TemplateBuilderCustomizer minCores() {
        return new MinCoresTemplateBuilder();
    }

    public static TemplateBuilderCustomizer minDisk() {
        return new MinDiskTemplateBuilder();
    }

    public static TemplateBuilderCustomizer minRam() {
        return new MinRamTemplateBuilder();
    }

    public static TemplateBuilderCustomizer noOp() {
        return new NoOpTemplateBuilder();
    }

    public static TemplateBuilderCustomizer os64Bit() {
        return new Os64BitTemplateBuidler();
    }

    public static TemplateBuilderCustomizer osFamily() {
        return new OsFamilyTemplateBuilder();
    }

    public static TemplateBuilderCustomizer osVersionRegex() {
        return new OsVersionRegexTemplateBuilder();
    }

    public static TemplateBuilderCustomizer templateSpec() {
        return new TemplateSpecTemplateBuilder();
    }

    private static class MinRamTemplateBuilder implements TemplateBuilderCustomizer {
        @Override
        public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
            tb.minRam((int) (ByteSizeStrings.parse(Strings.toString(v), "mb") / 1000 / 1000));
        }
    }

    private static class MinCoresTemplateBuilder implements TemplateBuilderCustomizer {
        @Override
        public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
            tb.minCores(TypeCoercions.coerce(v, Double.class));
        }
    }

    private static class MinDiskTemplateBuilder implements TemplateBuilderCustomizer {
        @Override
        public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
            tb.minDisk((int) (ByteSizeStrings.parse(Strings.toString(v), "gb") / 1000 / 1000 / 1000));
        }
    }

    private static class HardwareIdTemplateBuilder implements TemplateBuilderCustomizer {
        @Override
        public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
            tb.hardwareId(v.toString());
        }
    }

    private static class ImageIdTemplateBuilder implements TemplateBuilderCustomizer {
        @Override
        public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
            tb.imageId(v.toString());
        }
    }

    private static class ImageDescriptionRegexTemplateBuilder implements TemplateBuilderCustomizer {
        @Override
        public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
            tb.imageDescriptionMatches(v.toString());
        }
    }

    private static class ImageNameRegexTemplateBuilder implements TemplateBuilderCustomizer {
        @Override
        public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
            tb.imageNameMatches(v.toString());
        }
    }

    private static class OsFamilyTemplateBuilder implements TemplateBuilderCustomizer {
        @Override
        public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
            Maybe<OsFamily> osFamily = Enums.valueOfIgnoreCase(OsFamily.class, v.toString());
            if (osFamily.isAbsent()) {
                throw new IllegalArgumentException("Invalid " + JcloudsLocationConfig.OS_FAMILY + " value " + v);
            }
            tb.osFamily(osFamily.get());
        }
    }

    private static class OsVersionRegexTemplateBuilder implements TemplateBuilderCustomizer {
        @Override
        public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
            tb.osVersionMatches(v.toString());
        }
    }

    private static class TemplateSpecTemplateBuilder implements TemplateBuilderCustomizer {
        @Override
        public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
            tb.from(TemplateBuilderSpec.parse(v.toString()));
        }
    }

    private static class Os64BitTemplateBuidler implements TemplateBuilderCustomizer {
        @Override
        public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
            Boolean os64Bit = TypeCoercions.coerce(v, Boolean.class);
            if (os64Bit != null) {
                tb.os64Bit(os64Bit);
            }
        }
    }

    private static class NoOpTemplateBuilder implements TemplateBuilderCustomizer {
        @Override
        public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
        }
    }
}
