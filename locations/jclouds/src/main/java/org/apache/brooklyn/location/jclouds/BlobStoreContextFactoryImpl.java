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
package org.apache.brooklyn.location.jclouds;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Properties;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.core.location.internal.LocationInternal;
import org.apache.brooklyn.core.mgmt.persist.DeserializingJcloudsRenamesProvider;
import org.apache.brooklyn.util.collections.MutableList;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.encryption.bouncycastle.config.BouncyCastleCryptoModule;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Module;

public class BlobStoreContextFactoryImpl implements BlobStoreContextFactory {

    // TODO Comparing this with {@link JcloudsLocation#getComputeService()}, that calls
    // uses {@code ResolvingConfigBag.newInstanceExtending(getManagementContext(), config)}.
    // The reason is to allow DSL to be handled better - in particular when the context in
    // which the DSL must be resolved depends upon the entity on which it is defined (e.g. using
    // {@code provisioning.properties}).
    //
    // For blobstore usage (which is used for configuring persistence), we'll still want to 
    // support the use of DSL (especially for externalised configuration), but we won't have to
    // evaluate it in the context of a particular entity. We can therefore probably get away without
    // using {@link ResolvingConfigBag}. However, it's worth improving the test coverage for this!

    // TODO We could try to remove some duplication compared to {@link ComputeServiceRegistryImpl},
    // e.g. for {@link #getCommonModules()}. However, there's not that much duplication (and the 
    // common-modules could be different for blob-store versus compute-service).

    private static final Logger LOG = LoggerFactory.getLogger(BlobStoreContextFactoryImpl.class);

    public static final BlobStoreContextFactory INSTANCE = new BlobStoreContextFactoryImpl();
    
    protected BlobStoreContextFactoryImpl() {
    }

    @Override
    public BlobStoreContext newBlobStoreContext(Location location) {
        String rawProvider = checkNotNull(location.getConfig(LocationConfigKeys.CLOUD_PROVIDER), "provider must not be null");
        String provider = DeserializingJcloudsRenamesProvider.INSTANCE.applyJcloudsRenames(rawProvider);
        String identity = checkNotNull(location.getConfig(LocationConfigKeys.ACCESS_IDENTITY), "identity must not be null");
        String credential = checkNotNull(location.getConfig(LocationConfigKeys.ACCESS_CREDENTIAL), "credential must not be null");
        String endpoint = location.getConfig(CloudLocationConfig.CLOUD_ENDPOINT);

        Properties overrides = new Properties();
        // * Java 7,8 bug workaround - sockets closed by GC break the internal bookkeeping
        //   of HttpUrlConnection, leading to invalid handling of the "HTTP/1.1 100 Continue"
        //   response. Coupled with a bug when using SSL sockets reads will block
        //   indefinitely even though a read timeout is explicitly set.
        // * Java 6 ignores the header anyways as it is included in its restricted headers black list.
        // * Also there's a bug in SL object store which still expects Content-Length bytes
        //   even when it responds with a 408 timeout response, leading to incorrectly
        //   interpreting the next request (triggered by above problem).
        overrides.setProperty(Constants.PROPERTY_STRIP_EXPECT_HEADER, "true");

        // Add extra jclouds-specific configuration
        Map<String, Object> extra = Maps.filterKeys(((LocationInternal)location).config().getBag().getAllConfig(), Predicates.containsPattern("^jclouds\\."));
        if (extra.size() > 0) {
            LOG.debug("Configuring custom jclouds property overrides for {}: {}", provider, Sanitizer.sanitize(extra));
        }
        overrides.putAll(Maps.filterValues(extra, Predicates.notNull()));

        ContextBuilder contextBuilder = ContextBuilder.newBuilder(provider).credentials(identity, credential);
        contextBuilder.modules(MutableList.copyOf(getCommonModules()));
        if (!org.apache.brooklyn.util.text.Strings.isBlank(endpoint)) {
            contextBuilder.endpoint(endpoint);
        }
        contextBuilder.overrides(overrides);
        BlobStoreContext context = contextBuilder.buildView(BlobStoreContext.class);
        return context;
    }

    /** returns the jclouds modules we typically install */ 
    protected ImmutableSet<Module> getCommonModules() {
        return ImmutableSet.<Module> of(
                new SshjSshClientModule(),
                new SLF4JLoggingModule(),
                new BouncyCastleCryptoModule());
    }
}
