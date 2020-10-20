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

import com.google.common.annotations.Beta;
import java.io.File;
import java.io.InputStream;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.core.mgmt.ManagementContextInjectable;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;

/**
 * Interface for use by schemes which provide the capability to add types to the type registry.
 * Typically this is by installing an OSGi bundle with metadata, or sometimes with a YAML file.
 * <p>
 * To add a new resolver for a bundle of types, simply create an implementation and declare it
 * as an OSGi service in blueprint.xml (and usually also as a java service cf {@link ServiceLoader} for testing).
 * <p>
 * Implementations may wish to extend {@link AbstractCatalogBundleResolver} which simplifies the process.
 * <p>
 * See also {@link BrooklynTypePlanTransformer} for resolving individual types.
 */
public interface BrooklynCatalogBundleResolver extends ManagementContextInjectable {

    /** @return An identifier for the resolver.
     * This may be used when installing a bundle to target a specific resolver. */
    String getFormatCode();
    /** @return A display name for this resolver.
     * This may be used to prompt a user what type of bundle they are supplying. */
    String getFormatName();
    /** @return A description for this resolver */
    String getFormatDescription();

    /** 
     * Determines how appropriate is this transformer for the artifact.
     *
     * @param format the format, eg {@link BrooklynBomBundleCatalogBundleResolver#FORMAT}; if null, auto-detect
     * @param input a renewable supplier of {@link InputStream} -- each call should return a new stream.
     *              consider using {@link org.apache.brooklyn.util.stream.InputStreamSource}.
     *
     * @return A co-ordinated score / confidence value in the range 0 to 1. 
     * 0 means not compatible, 
     * 1 means this is clearly the intended transformer and no others need be tried 
     * (for instance because the format is explicitly specified),
     * and values between 0 and 1 indicate how likely a transformer believes it should be used.
     * <p>
     * Values greater than 0.5 are generally reserved for the presence of marker tags or files
     * which strongly indicate that the format is compatible.
     * Such a value should be returned even if the plan is not actually parseable, but if it looks like a user error
     * which prevents parsing (eg mal-formed YAML) and the transformer could likely be the intended target.
     * <p>
     * */
    double scoreForBundle(@Nullable String format, @Nonnull Supplier<InputStream> input);

    /** Installs the given bundle to the type {@link BrooklynTypeRegistry}.
     * <p>
     * The framework guarantees this will only be invoked when {@link #scoreForBundle(String, Supplier<InputStream>)}
     * has returned a positive value.
     * <p>
     * Implementations should either return null or reference {@link UnsupportedCatalogBundleException} in the return object (or throw);
     * if upon closer inspection following a non-null score, they do not actually support the given {@link File}.
     * If they should support the artifact but it contains an error, they should reference (or throw) the relevant error for feedback to the user. */
    @Beta  // return type is too detailed, but the detail is useful
    public ReferenceWithError<OsgiBundleInstallationResult> install(@Nonnull Supplier<InputStream> input, BundleInstallationOptions options);

    public class BundleInstallationOptions {
        protected String format;
        protected boolean forceUpdateOfNonSnapshots = false;
        protected boolean validateTypes = true;
        protected boolean deferredStart = false;
        protected boolean start = true;
        protected boolean loadCatalogBom = true;
        protected ManagedBundle knownBundleMetadata = null;

        public void setFormat(String format) {
            this.format = format;
        }

        public void setStart(boolean start) {
            this.start = start;
        }

        public void setLoadCatalogBom(boolean loadCatalogBom) {
            this.loadCatalogBom = loadCatalogBom;
        }

        public void setForceUpdateOfNonSnapshots(boolean forceUpdateOfNonSnapshots) {
            this.forceUpdateOfNonSnapshots = forceUpdateOfNonSnapshots;
        }

        public void setValidateTypes(boolean validateTypes) {
            this.validateTypes = validateTypes;
        }

        public void setDeferredStart(boolean deferredStart) {
            this.deferredStart = deferredStart;
        }

        public void setKnownBundleMetadata(ManagedBundle knownBundleMetadata) {
            this.knownBundleMetadata = knownBundleMetadata;
        }

        public String getFormat() {
            return format;
        }

        public ManagedBundle getKnownBundleMetadata() {
            return knownBundleMetadata;
        }

        public boolean isDeferredStart() {
            return deferredStart;
        }

        public boolean isForceUpdateOfNonSnapshots() {
            return forceUpdateOfNonSnapshots;
        }

        public boolean isLoadCatalogBom() {
            return loadCatalogBom;
        }

        public boolean isStart() {
            return start;
        }

        public boolean isValidateTypes() {
            return validateTypes;
        }
    }

}
