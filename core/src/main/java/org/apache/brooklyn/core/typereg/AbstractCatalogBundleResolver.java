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

import java.io.File;
import java.io.InputStream;
import java.util.function.Supplier;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience supertype for {@link BrooklynCatalogBundleResolver} instances.
 * <p>
 * This supplies a default {@link #scoreForBundle(String, InputStream)} method
 * method which returns 1 if the format code matches,
 * and otherwise branches to various abstract score methods which subclasses can implement,
 * cf {@link AbstractTypePlanTransformer}.
 */
public abstract class AbstractCatalogBundleResolver implements BrooklynCatalogBundleResolver {

    private static final Logger log = LoggerFactory.getLogger(AbstractCatalogBundleResolver.class);

    protected ManagementContext mgmt;

    @Override
    public void setManagementContext(ManagementContext mgmt) {
        this.mgmt = mgmt;
    }

    public AbstractCatalogBundleResolver withManagementContext(ManagementContext mgmt) {
        this.mgmt = mgmt;
        return this;
    }

    private final String format;
    private final String formatName;
    private final String formatDescription;

    protected AbstractCatalogBundleResolver(String format, String formatName, String formatDescription) {
        this.format = format;
        this.formatName = formatName;
        this.formatDescription = formatDescription;
    }
    
    @Override
    public String getFormatCode() {
        return format;
    }

    @Override
    public String getFormatName() {
        return formatName;
    }

    @Override
    public String getFormatDescription() {
        return formatDescription;
    }

    @Override
    public String toString() {
        return getFormatCode()+":"+JavaClassNames.simpleClassName(this);
    }
    
    @Override
    public double scoreForBundle(String format, Supplier<InputStream> f) {
        if (getFormatCode().equals(format)) return 1;
        if (format==null)
            return scoreForNullFormat(f);
        else
            return scoreForNonmatchingNonnullFormat(format, f);
    }

    protected abstract double scoreForNullFormat(Supplier<InputStream> f);

    protected double scoreForNonmatchingNonnullFormat(String format, Supplier<InputStream> f) {
        return 0;
    }

}
