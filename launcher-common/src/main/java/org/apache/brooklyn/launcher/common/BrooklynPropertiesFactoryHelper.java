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
package org.apache.brooklyn.launcher.common;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.internal.BrooklynProperties.Factory.Builder;
import org.apache.brooklyn.util.exceptions.FatalRuntimeException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.io.FileUtil;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;

public class BrooklynPropertiesFactoryHelper {
    private static final Logger LOG = LoggerFactory.getLogger(BrooklynPropertiesFactoryHelper.class);

    private final String globalBrooklynPropertiesFile;
    private final String localBrooklynPropertiesFile;
    private final Supplier<Map<?, ?>> propertiesSupplier;
    private final BrooklynProperties brooklynProperties;

    public BrooklynPropertiesFactoryHelper(String globalBrooklynPropertiesFile, String localBrooklynPropertiesFile) {
        this(globalBrooklynPropertiesFile, localBrooklynPropertiesFile, null, null);
    }

    public BrooklynPropertiesFactoryHelper(BrooklynProperties brooklynProperties) {
        this(null, null, brooklynProperties, null);
    }

    public BrooklynPropertiesFactoryHelper(String globalBrooklynPropertiesFile,
            String localBrooklynPropertiesFile,
            BrooklynProperties brooklynProperties) {
        this(globalBrooklynPropertiesFile, localBrooklynPropertiesFile, brooklynProperties, null);
    }

    public BrooklynPropertiesFactoryHelper(
            String globalBrooklynPropertiesFile,
            String localBrooklynPropertiesFile,
            Supplier<Map<?, ?>> propertiesSupplier) {
        this(globalBrooklynPropertiesFile, localBrooklynPropertiesFile, null, propertiesSupplier);
    }

    public BrooklynPropertiesFactoryHelper(String globalBrooklynPropertiesFile,
            String localBrooklynPropertiesFile,
            BrooklynProperties brooklynProperties,
            Supplier<Map<?, ?>> propertiesSupplier) {
        this.globalBrooklynPropertiesFile = globalBrooklynPropertiesFile;
        this.localBrooklynPropertiesFile = localBrooklynPropertiesFile;
        this.brooklynProperties = brooklynProperties;
        this.propertiesSupplier = propertiesSupplier;
    }

    public BrooklynProperties.Factory.Builder createPropertiesBuilder() {
        if (brooklynProperties == null) {
            BrooklynProperties.Factory.Builder builder = BrooklynProperties.Factory.builderDefault();
    
            if (Strings.isNonEmpty(globalBrooklynPropertiesFile)) {
                File globalProperties = new File(Os.tidyPath(globalBrooklynPropertiesFile));
                if (globalProperties.exists()) {
                    globalProperties = resolveSymbolicLink(globalProperties);
                    checkFileReadable(globalProperties);
                    // brooklyn.properties stores passwords (web-console and cloud credentials),
                    // so ensure it has sensible permissions
                    checkFilePermissionsX00(globalProperties);
                    LOG.debug("Using global properties file " + globalProperties);
                } else {
                    LOG.debug("Global properties file " + globalProperties + " does not exist, will ignore");
                }
                builder.globalPropertiesFile(globalProperties.getAbsolutePath());
            } else {
                LOG.debug("Global properties file disabled");
                builder.globalPropertiesFile(null);
            }
            
            if (Strings.isNonEmpty(localBrooklynPropertiesFile)) {
                File localProperties = new File(Os.tidyPath(localBrooklynPropertiesFile));
                localProperties = resolveSymbolicLink(localProperties);
                checkFileReadable(localProperties);
                checkFilePermissionsX00(localProperties);
                builder.localPropertiesFile(localProperties.getAbsolutePath());
            }

            if (propertiesSupplier != null) {
                builder.propertiesSupplier(propertiesSupplier);
            }
            return builder;
        } else {
            if (globalBrooklynPropertiesFile != null)
                LOG.warn("Ignoring globalBrooklynPropertiesFile "+globalBrooklynPropertiesFile+" because explicit brooklynProperties supplied");
            if (localBrooklynPropertiesFile != null)
                LOG.warn("Ignoring localBrooklynPropertiesFile "+localBrooklynPropertiesFile+" because explicit brooklynProperties supplied");
            return Builder.fromProperties(brooklynProperties);
        }
    }

    /**
     * @return The canonical path of the argument.
     */
    private File resolveSymbolicLink(File f) {
        File f2 = f;
        try {
            f2 = f.getCanonicalFile();
            if (Files.isSymbolicLink(f.toPath())) {
                LOG.debug("Resolved symbolic link: {} -> {}", f, f2);
            }
        } catch (IOException e) {
            LOG.warn("Could not determine canonical name of file "+f+"; returning original file", e);
        }
        return f2;
    }

    private void checkFileReadable(File f) {
        if (!f.exists()) {
            throw new FatalRuntimeException("File " + f + " does not exist");
        }
        if (!f.isFile()) {
            throw new FatalRuntimeException(f + " is not a file");
        }
        if (!f.canRead()) {
            throw new FatalRuntimeException(f + " is not readable");
        }
    }
    
    private void checkFilePermissionsX00(File f) {

        Maybe<String> permission = FileUtil.getFilePermissions(f);
        if (permission.isAbsent()) {
            LOG.debug("Could not determine permissions of file; assuming ok: "+f);
        } else {
            if (!permission.get().subSequence(4, 10).equals("------")) {
                throw new FatalRuntimeException("Invalid permissions for file " + f + "; expected ?00 but was " + permission.get());
            }
        }
    }
    

}
