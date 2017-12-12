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
package org.apache.brooklyn.core.mgmt.persist.jclouds;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;
import java.util.Set;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.location.jclouds.BlobStoreContextFactoryImpl;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.http.HttpTool;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.apache.http.client.HttpClient;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.domain.Credentials;
import org.jclouds.openstack.keystone.auth.AuthHeaders;
import org.jclouds.openstack.keystone.v2_0.domain.Access;
import org.jclouds.openstack.keystone.v2_0.domain.Service;
import org.jclouds.openstack.keystone.v2_0.domain.Token;
import org.jclouds.openstack.keystone.v2_0.domain.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

/**
 * Tests that the token is automatically renewed. It does this by requesting a short-lived token, 
 * and injecting that into the guts of jclouds!
 * 
 * Expect to see in the log something like the following:
 * <ol>
 *   <li>Requesting a token:
 *       <pre>
 *       2017-01-17 21:30:18,832 INFO  o.a.b.c.m.p.j.BlobStoreExpiryTest [main]: Requested token with explicit lifetime: 5s at https://ams01.objectstorage.softlayer.net/auth/v1.0
 *       HttpToolResponse{responseCode=0}
 *       {Content-Length=[1472], X-Auth-Token-Expires=[4], X-Auth-Token=[AUTH_temptemptemptemptemptemptemptempte ...
 *       </pre>
 *   <li>First request with that token succeeds:
 *       <br>
 *       <pre>
 *       2017-01-17 21:30:18,863 DEBUG jclouds.headers [main]: >> PUT https://ams01.objectstorage.softlayer.net/v1/AUTH_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/brooklyn-persistence-test-W6eNdNXp HTTP/1.1
 *       2017-01-17 21:30:18,863 DEBUG jclouds.headers [main]: >> Accept: application/json
 *       2017-01-17 21:30:18,863 DEBUG jclouds.headers [main]: >> X-Auth-Token: AUTH_temptemptemptemptemptemptemptempte
 *       2017-01-17 21:30:18,925 DEBUG jclouds.headers [main]: << HTTP/1.1 201 Created
 *       </pre>
 *   <li>After 10 seconds, next request fails with 401 Unauthorized:
 *       <pre>
 *       2017-01-17 21:30:29,023 DEBUG jclouds.headers [main]: >> GET https://ams01.objectstorage.softlayer.net/v1/AUTH_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx?format=json HTTP/1.1
 *       2017-01-17 21:30:29,023 DEBUG jclouds.headers [main]: >> Accept: application/json
 *       2017-01-17 21:30:29,023 DEBUG jclouds.headers [main]: >> X-Auth-Token: AUTH_temptemptemptemptemptemptemptempte
 *       2017-01-17 21:30:29,140 DEBUG jclouds.headers [main]: << HTTP/1.1 401 Unauthorized
 *       </pre>
 *   <li>Automatically renews:
 *       <pre>
 *       2017-01-17 21:30:29,141 DEBUG o.j.o.k.v.h.RetryOnRenew [main]: invalidating authentication token - first time for [method=org.jclouds.openstack.swift.v1.features.ContainerApi.public abstract com.google.common.collect.FluentIterable org.jclouds.openstack.swift.v1.features.ContainerApi.list()[], request=GET https://ams01.objectstorage.softlayer.net/v1/AUTH_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx?format=json HTTP/1.1]
 *       2017-01-17 21:30:29,146 DEBUG o.j.r.i.InvokeHttpMethod [main]: >> invoking TempAuthApi.auth
 *       2017-01-17 21:30:29,146 DEBUG o.j.h.i.JavaUrlHttpCommandExecutorService [main]: Sending request -203921621: GET https://ams01.objectstorage.softlayer.net/auth/v1.0 HTTP/1.1
 *       2017-01-17 21:30:29,146 DEBUG jclouds.headers [main]: >> GET https://ams01.objectstorage.softlayer.net/auth/v1.0 HTTP/1.1
 *       2017-01-17 21:30:29,288 DEBUG jclouds.headers [main]: << HTTP/1.1 200 OK
 *       2017-01-17 21:30:29,288 DEBUG jclouds.headers [main]: << X-Storage-Token: AUTH_newnewnewnewnewnewnewnewnewnewnewn
 *       </pre>
 *   <li>Request is repeated automatically with the new token, and works:
 *       <pre>
 *       2017-01-17 21:30:29,290 DEBUG jclouds.headers [main]: >> GET https://ams01.objectstorage.softlayer.net/v1/AUTH_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx?format=json HTTP/1.1
 *       2017-01-17 21:30:29,290 DEBUG jclouds.headers [main]: >> Accept: application/json
 *       2017-01-17 21:30:29,290 DEBUG jclouds.headers [main]: >> X-Auth-Token: AUTH_newnewnewnewnewnewnewnewnewnewnewn
 *       2017-01-17 21:30:29,347 DEBUG jclouds.headers [main]: << HTTP/1.1 200 OK
 *       </pre>
 * </ol>
 */
@Test(groups={"Live", "Live-sanity"})
public class BlobStoreExpiryTest {

    private static final Logger log = LoggerFactory.getLogger(BlobStoreExpiryTest.class);
    
    /**
     * Live tests as written require a location defined as follows:
     * 
     * <code>
     * brooklyn.location.named.brooklyn-jclouds-objstore-test-1==jclouds:openstack-swift:https://ams01.objectstorage.softlayer.net/auth/v1.0
     * brooklyn.location.named.brooklyn-jclouds-objstore-test-1.identity=IBMOS1234-5:yourname
     * brooklyn.location.named.brooklyn-jclouds-objstore-test-1.credential=0123abcd.......
     * brooklyn.location.named.brooklyn-jclouds-objstore-test-1.jclouds.keystone.credential-type=tempAuthCredentials
     * </code>
     */
    
    public static final String PERSIST_TO_OBJECT_STORE_FOR_TEST_SPEC = BlobStoreTest.PERSIST_TO_OBJECT_STORE_FOR_TEST_SPEC;
    private String locationSpec = PERSIST_TO_OBJECT_STORE_FOR_TEST_SPEC;
    public static final String CONTAINER_PREFIX = "brooklyn-persistence-test";
    
    private ManagementContext mgmt;
    private JcloudsLocation location;

    private String identity;
    private String credential;
    private String endpoint;

    private BlobStoreContext context;
    private String testContainerName;

    @BeforeMethod(alwaysRun=true)
    public void setup() {
        testContainerName = CONTAINER_PREFIX+"-"+Identifiers.makeRandomId(8);
        mgmt = new LocalManagementContextForTests(BrooklynProperties.Factory.newDefault());
        location = (JcloudsLocation) mgmt.getLocationRegistry().getLocationManaged(locationSpec);
        identity = checkNotNull(location.getConfig(LocationConfigKeys.ACCESS_IDENTITY), "identity must not be null");
        credential = checkNotNull(location.getConfig(LocationConfigKeys.ACCESS_CREDENTIAL), "credential must not be null");
        endpoint = location.getConfig(CloudLocationConfig.CLOUD_ENDPOINT);
        context = BlobStoreContextFactoryImpl.INSTANCE.newBlobStoreContext(location);
    }
    
    @AfterMethod(alwaysRun=true)
    public void teardown() {
        Entities.destroyAll(mgmt);
        if (context!=null) context.close();
        context = null;
    }

    public void testRenewAuthSucceedsInSwiftObjectStore() throws Exception {
        injectShortLivedTokenForSwiftAuth();
        
        context.getBlobStore().createContainerInLocation(null, testContainerName);
        
        assertContainerFound();
        
        log.info("created container, now sleeping for expiration");
        
        Time.sleep(Duration.TEN_SECONDS);
        
        assertContainerFound();

        context.getBlobStore().deleteContainer(testContainerName);
    }

    private void assertContainerFound() {
        PageSet<? extends StorageMetadata> ps = context.getBlobStore().list();
        BlobStoreTest.assertHasItemNamed(ps, testContainerName);
    }
    
    /**
     * Injects into the guts of jclouds' openstack-keystone a token that was requested, which 
     * should last for only 5 seconds. By sleeping for 10 seconds in the test, it should mean
     * the token subsequently used by jclouds will expire by the time the second half of the 
     * test executes.
     */
    private void injectShortLivedTokenForSwiftAuth() throws Exception {
        URL endpointUrl = new URL(endpoint);
        Credentials creds = new Credentials(identity, credential);
        Set<Service> services = getServices(creds);

        HttpToolResponse tokenHttpResponse1 = requestTokenWithExplicitLifetime(endpointUrl,
            identity, credential, Duration.FIVE_SECONDS);
        
        Access access = Access.builder()
                .user(User.builder()
                        .id(identity)
                        .name(identity)
                        .build())
                .token(Token.builder()
                        .id(tokenHttpResponse1.getHeaderLists().get(AuthHeaders.AUTH_TOKEN).get(0))
                        .expires(new Date(System.currentTimeMillis() + 5000))
                        .build())
                .services(services)
                .build();

        getAuthCache(context).put(creds, access);
    }

    private LoadingCache<Credentials, Access> getAuthCache(BlobStoreContext context) {
        return context.utils().injector().getInstance(CachePeeker.class).authenticationResponseCache;
    }
    
    private Set<Service> getServices(Credentials creds) throws Exception {
        BlobStoreContext tmpContext = BlobStoreContextFactoryImpl.INSTANCE.newBlobStoreContext(location);
        try {
            tmpContext.getBlobStore().list();
            LoadingCache<Credentials, Access> authCache = getAuthCache(tmpContext);
            Access tmpAccess = authCache.get(creds);
            return ImmutableSet.copyOf(tmpAccess);
        } finally {
            tmpContext.close();
        }

    }
    
    public static class CachePeeker {
        private final LoadingCache<Credentials, Access> authenticationResponseCache;

        @Inject
        protected CachePeeker(LoadingCache<Credentials, Access> authenticationResponseCache) {
           this.authenticationResponseCache = authenticationResponseCache;
        }
    }

    public static HttpToolResponse requestTokenWithExplicitLifetime(URL url, String user, String key, Duration expiration) throws URISyntaxException {
        HttpClient client = HttpTool.httpClientBuilder().build();
        HttpToolResponse response = HttpTool.httpGet(client, url.toURI(), MutableMap.<String,String>of()
            .add(AuthHeaders.AUTH_USER, user)
            .add(AuthHeaders.AUTH_KEY, key)
            .add("Host", url.getHost())
            .add("X-Auth-New-Token", "" + true)
            .add("X-Auth-Token-Lifetime", "" + expiration.toSeconds())
            );
//        curl -v https://ams01.objectstorage.softlayer.net/auth/v1.0/v1.0 -H "X-Auth-User: IBMOS12345-2:username" -H "X-Auth-Key: <API KEY>" -H "Host: ams01.objectstorage.softlayer.net" -H "X-Auth-New-Token: true" -H "X-Auth-Token-Lifetime: 15"
        log.info("Requested token with explicit lifetime: "+expiration+" at "+url+"\n"+response+"\n"+response.getHeaderLists());
        return response;
    }
}
