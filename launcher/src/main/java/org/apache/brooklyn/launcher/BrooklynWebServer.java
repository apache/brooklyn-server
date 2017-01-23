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
package org.apache.brooklyn.launcher;

import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import javax.security.auth.spi.LoginModule;

import com.google.common.collect.ImmutableList;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.rest.NopSecurityHandler;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.BrooklynVersion;
import org.apache.brooklyn.core.internal.BrooklynInitialization;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.server.BrooklynServerPaths;
import org.apache.brooklyn.core.server.BrooklynServiceAttributes;
import org.apache.brooklyn.launcher.config.CustomResourceLocator;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.rest.RestApiSetup;
import org.apache.brooklyn.rest.filter.CsrfTokenFilter;
import org.apache.brooklyn.rest.filter.EntitlementContextFilter;
import org.apache.brooklyn.rest.filter.CorsImplSupplierFilter;
import org.apache.brooklyn.rest.filter.HaHotCheckResourceFilter;
import org.apache.brooklyn.rest.filter.LoggingFilter;
import org.apache.brooklyn.rest.filter.NoCacheFilter;
import org.apache.brooklyn.rest.filter.RequestTaggingFilter;
import org.apache.brooklyn.rest.filter.RequestTaggingRsFilter;
import org.apache.brooklyn.rest.security.jaas.BrooklynLoginModule;
import org.apache.brooklyn.rest.security.jaas.BrooklynLoginModule.RolePrincipal;
import org.apache.brooklyn.rest.security.jaas.JaasUtils;
import org.apache.brooklyn.rest.util.ManagementContextProvider;
import org.apache.brooklyn.core.mgmt.ShutdownHandler;
import org.apache.brooklyn.rest.util.ShutdownHandlerProvider;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.BrooklynNetworkUtils;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.crypto.FluentKeySigner;
import org.apache.brooklyn.util.core.crypto.SecureKeys;
import org.apache.brooklyn.util.core.flags.FlagUtils;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.io.FileUtil;
import org.apache.brooklyn.util.javalang.Threads;
import org.apache.brooklyn.util.logging.LoggingSetup;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.web.ContextHandlerCollectionHotSwappable;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

/**
 * Starts the web-app running, connected to the given management context
 */
public class BrooklynWebServer {
    private static final Logger log = LoggerFactory.getLogger(BrooklynWebServer.class);

    public static final String BROOKLYN_WAR_URL = "classpath://brooklyn.war";
    static {
        // support loading the WAR in dev mode from an alternate location 
        CustomResourceLocator.registerAlternateLocator(new CustomResourceLocator.SearchingClassPathInDevMode(
                BROOKLYN_WAR_URL, "/brooklyn-server/launcher/target", 
                "/brooklyn-ui/target/brooklyn-jsgui-"+BrooklynVersion.get()+".war"));
    }
    
    static {
        LoggingSetup.installJavaUtilLoggingBridge();
    }
    
    protected Server server;

    private WebAppContext rootContext;
    
    /** base port to use, for http if enabled or else https; if not set, it uses httpPort or httpsPort */
    @SetFromFlag("port")
    protected PortRange requestedPort = null;
    
    @SetFromFlag
    protected PortRange httpPort = PortRanges.fromString("8081+");
    @SetFromFlag
    protected PortRange httpsPort = PortRanges.fromString("8443+");
    
    /** actual port where this gets bound; will be consistent with the "port" passed in
     * but that might be a range and here it is a single port, or -1 if not yet set */
    protected volatile int actualPort = -1;
    /** actual NIC where this is listening; in the case of 0.0.0.0 being passed in as bindAddress,
     * this will revert to one address (such as localhost) */
    protected InetAddress actualAddress = null;

    @SetFromFlag
    protected String war = BROOKLYN_WAR_URL;

    /** IP of NIC where this server should bind, or null to autodetect 
     * (e.g. 0.0.0.0 if security is configured, or loopback if no security) */
    @SetFromFlag
    protected InetAddress bindAddress = null;

    /** The address that this server's management context will be publically available on. */
    @SetFromFlag
    protected InetAddress publicAddress = null;

    /**
     * map of context-prefix to file
     */
    @SetFromFlag
    private Map<String, String> wars = new LinkedHashMap<String, String>();

    // would like to remove wars in favour of this but SetFromFlag means we have no idea where it's used.
    private Map<String, WebAppContextProvider> contextProviders = new LinkedHashMap<>();

    @SetFromFlag
    protected boolean ignoreWebappDeploymentFailures = false;

    @SetFromFlag
    private Map<String, Object> attributes = new LinkedHashMap<String, Object>();

    private ManagementContext managementContext;

    @SetFromFlag
    private Boolean httpsEnabled;

    @SetFromFlag
    private String sslCertificate;

    @SetFromFlag
    private String keystoreUrl;

    @SetFromFlag @Deprecated /** @deprecated use keystoreUrl */
    private String keystorePath;

    @SetFromFlag
    private String keystorePassword;

    @SetFromFlag
    private String keystoreCertAlias;

    @SetFromFlag
    private String truststorePath;

    @SetFromFlag
    private String trustStorePassword;
    
    @SetFromFlag
    private String transportProtocols;
    
    @SetFromFlag
    private String transportCiphers;

    private File webappTempDir;
    
    /**
     * @deprecated since 0.9.0, use {@link #consoleSecurity} to disable security or
     * register an alternative JAAS {@link LoginModule}.
     * {@link BrooklynLoginModule} used by default.
     */
    @Deprecated
    private Class<org.apache.brooklyn.rest.filter.BrooklynPropertiesSecurityFilter> securityFilterClazz;
    
    @SetFromFlag
    private boolean skipSecurity = false;

    private ShutdownHandler shutdownHandler;

    public BrooklynWebServer(ManagementContext managementContext) {
        this(Maps.newLinkedHashMap(), managementContext);
    }

    /**
     * accepts flags:  port,
     * war (url of war file which is the root),
     * wars (map of context-prefix to url),
     * attrs (map of attribute-name : object pairs passed to the servlet)
     */
    public BrooklynWebServer(Map<?,?> flags, ManagementContext managementContext) {
        this.managementContext = managementContext;
        Map<?,?> leftovers = FlagUtils.setFieldsFromFlags(flags, this);
        if (!leftovers.isEmpty())
            log.warn("Ignoring unknown flags " + leftovers);
        
        webappTempDir = BrooklynServerPaths.getBrooklynWebTmpDir(managementContext);
        JaasUtils.init(managementContext);
    }

    public BrooklynWebServer(ManagementContext managementContext, int port) {
        this(managementContext, port, "brooklyn.war");
    }

    public BrooklynWebServer(ManagementContext managementContext, int port, String warUrl) {
        this(MutableMap.of("port", port, "war", warUrl), managementContext);
    }

    /** @deprecated since 0.9.0, use {@link #skipSecurity} or {@link BrooklynLoginModule} */
    @Deprecated
    public void setSecurityFilter(Class<org.apache.brooklyn.rest.filter.BrooklynPropertiesSecurityFilter> filterClazz) {
        this.securityFilterClazz = filterClazz;
    }

    public BrooklynWebServer skipSecurity() {
        return skipSecurity(true);
    }

    public BrooklynWebServer skipSecurity(boolean skipSecurity) {
        this.skipSecurity = skipSecurity;
        return this;
    }

    public void setShutdownHandler(@Nullable ShutdownHandler shutdownHandler) {
        this.shutdownHandler = shutdownHandler;
    }

    public BrooklynWebServer setPort(Object port) {
        if (getActualPort()>0)
            throw new IllegalStateException("Can't set port after port has been assigned to server (using "+getActualPort()+")");
        this.requestedPort = TypeCoercions.coerce(port, PortRange.class);
        return this;
    }

    @VisibleForTesting
    File getWebappTempDir() {
        return webappTempDir;
    }
    
    public BrooklynWebServer setHttpsEnabled(Boolean httpsEnabled) {
        this.httpsEnabled = httpsEnabled;
        return this;
    }
    
    public boolean getHttpsEnabled() {
        return getConfig(httpsEnabled, BrooklynWebConfig.HTTPS_REQUIRED);
    }
    
    public PortRange getRequestedPort() {
        return requestedPort;
    }
    
    /** returns port where this is running, or -1 if not yet known */
    public int getActualPort() {
        return actualPort;
    }

    /** interface/address where this server is listening;
     * if bound to 0.0.0.0 (all NICs, e.g. because security is set) this will return one NIC where this is bound */
    public InetAddress getAddress() {
        return actualAddress;
    }
    
    /** URL for accessing this web server (root context) */
    public String getRootUrl() {
        String address = (publicAddress != null) ? publicAddress.getHostName() : getAddress().getHostName();
        if (getActualPort()>0){
            String protocol = getHttpsEnabled()?"https":"http";
            return protocol+"://"+address+":"+getActualPort()+"/";
        } else {
            return null;
        }
    }

      /** sets the WAR to use as the root context (only if server not yet started);
     * cf deploy("/", url) */
    public BrooklynWebServer setWar(String url) {
        this.war = url;
        return this;
    }

    /** specifies a WAR to use at a given context path (only if server not yet started);
     * cf deploy(path, url) */
    public BrooklynWebServer addWar(String path, String warUrl) {
        addWar(new WebAppContextProvider(path, warUrl));
        return this;
    }

    public BrooklynWebServer addWar(WebAppContextProvider contextProvider) {
        contextProviders.put(contextProvider.getPath(), contextProvider);
        return this;
    }

    /** InetAddress to which server should bind;
     * defaults to 0.0.0.0 (although common call path is to set to 127.0.0.1 when security is not set) */
    public BrooklynWebServer setBindAddress(InetAddress address) {
        bindAddress = address;
        return this;
    }

    /**
     * Sets the public address that the server's management context's REST API will be available on
     */
    public BrooklynWebServer setPublicAddress(InetAddress address) {
        publicAddress = address;
        return this;
    }

    /** @deprecated use setAttribute */
    public BrooklynWebServer addAttribute(String field, Object value) {
        return setAttribute(field, value);
    }

    /** Specifies an attribute passed to deployed webapps 
     * (in addition to {@link BrooklynServiceAttributes#BROOKLYN_MANAGEMENT_CONTEXT} */
    public BrooklynWebServer setAttribute(String field, Object value) {
        attributes.put(field, value);
        return this;
    }
    
    public <T> BrooklynWebServer configure(ConfigKey<T> key, T value) {
        return setAttribute(key.getName(), value);
    }

    /** Specifies attributes passed to deployed webapps 
     * (in addition to {@link BrooklynServiceAttributes#BROOKLYN_MANAGEMENT_CONTEXT} */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public BrooklynWebServer putAttributes(Map newAttrs) {
        if (newAttrs!=null) attributes.putAll(newAttrs);
        return this;
    }

    ContextHandlerCollectionHotSwappable handlers = new ContextHandlerCollectionHotSwappable();
    
    /**
     * Starts the embedded web application server.
     */
    public synchronized void start() throws Exception {
        if (server != null) throw new IllegalStateException(""+this+" already running");

        if (actualPort == -1){
            PortRange portRange = getConfig(requestedPort, BrooklynWebConfig.WEB_CONSOLE_PORT);
            if (portRange==null) {
                portRange = getHttpsEnabled() ? httpsPort : httpPort;
            }
            actualPort = LocalhostMachineProvisioningLocation.obtainPort(getAddress(), portRange);
            if (actualPort == -1) 
                throw new IllegalStateException("Unable to provision port for web console (wanted "+portRange+")");
        }


        // use a nice name in the thread pool (otherwise this is exactly the same as Server defaults)
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setName("brooklyn-jetty-server-"+actualPort+"-"+threadPool.getName());

        server = new Server(threadPool);

        // Can be moved to jetty-web.xml inside wars or a global jetty.xml.
        JAASLoginService loginService = new JAASLoginService();
        loginService.setName("webconsole");
        loginService.setLoginModuleName("webconsole");
        loginService.setRoleClassNames(new String[] {RolePrincipal.class.getName()});
        server.addBean(loginService);

        final ServerConnector connector;

        if (getHttpsEnabled()) {
            HttpConfiguration sslHttpConfig = new HttpConfiguration();
            sslHttpConfig.setSecureScheme("https");
            sslHttpConfig.setSecurePort(actualPort);

            SslContextFactory sslContextFactory = createContextFactory();
            connector = new ServerConnector(server, new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()), new HttpConnectionFactory(sslHttpConfig));
        } else {
            connector = new ServerConnector(server, new HttpConnectionFactory());
        }

        if (bindAddress != null) {
            connector.setHost(bindAddress.getHostName());
        }
        connector.setPort(actualPort);
        server.setConnectors(new Connector[]{connector});

        if (bindAddress == null || bindAddress.equals(InetAddress.getByAddress(new byte[] { 0, 0, 0, 0 }))) {
            actualAddress = BrooklynNetworkUtils.getLocalhostInetAddress();
        } else {
            actualAddress = bindAddress;
        }

        if (log.isDebugEnabled())
            log.debug("Starting Brooklyn console at "+getRootUrl()+", running " + war + (wars != null ? " and " + wars.values() : ""));
        
        addShutdownHook();

        MutableMap<String, WebAppContextProvider> allWars = MutableMap.copyOf(contextProviders);
        for (Map.Entry<String, String> entry : wars.entrySet()) {
            allWars.put(entry.getKey(), new WebAppContextProvider(entry.getKey(), entry.getValue()));
        }

        WebAppContextProvider rootWar = allWars.remove(""); // leading slash stripped by WebAppContextProvider
        if (rootWar==null) rootWar = new WebAppContextProvider("/", war);
        
        for (WebAppContextProvider contextProvider : allWars.values()) {
            WebAppContext webapp = deploy(contextProvider);
            webapp.setTempDirectory(Os.mkdirs(new File(webappTempDir, newTimestampedDirName("war", 8))));
        }

        rootContext = deploy(rootWar);
        deployRestApi(rootContext);
        rootContext.setTempDirectory(Os.mkdirs(new File(webappTempDir, "war-root")));

        server.setHandler(handlers);
        server.start();
        //reinit required because some webapps (eg grails) might wipe our language extension bindings
        BrooklynInitialization.reinitAll();

        if (managementContext instanceof ManagementContextInternal) {
            ((ManagementContextInternal) managementContext).setManagementNodeUri(new URI(getRootUrl()));
        }

        log.info("Started Brooklyn console at "+getRootUrl()+", running " + rootWar + (allWars!=null && !allWars.isEmpty() ? " and " + wars.values() : ""));
    }

    private WebAppContext deployRestApi(WebAppContext context) {
        ImmutableList.Builder<Object> providersListBuilder = ImmutableList.builder();
        providersListBuilder.add(
                new ManagementContextProvider(),
                new ShutdownHandlerProvider(shutdownHandler),
                new RequestTaggingRsFilter(),
                new NoCacheFilter(),
                new HaHotCheckResourceFilter(),
                new EntitlementContextFilter(),
                new CsrfTokenFilter());
        if (BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_CORS_CXF_PROPERTY)) {
            providersListBuilder.add(new CorsImplSupplierFilter(managementContext));
        }

        RestApiSetup.installRest(context,
                providersListBuilder.build().toArray());
        RestApiSetup.installServletFilters(context,
                RequestTaggingFilter.class,
                LoggingFilter.class);
        if (securityFilterClazz != null) {
            RestApiSetup.installServletFilters(context, securityFilterClazz);
        }
        return context;
    }

    private SslContextFactory createContextFactory() throws KeyStoreException {
        SslContextFactory sslContextFactory = new SslContextFactory();

        // allow webconsole keystore & related properties to be set in brooklyn.properties
        String ksUrl = getKeystoreUrl();
        String ksPassword = getConfig(keystorePassword, BrooklynWebConfig.KEYSTORE_PASSWORD);
        String ksCertAlias = getConfig(keystoreCertAlias, BrooklynWebConfig.KEYSTORE_CERTIFICATE_ALIAS);
        String trProtos = getConfig(transportProtocols, BrooklynWebConfig.TRANSPORT_PROTOCOLS);
        String trCiphers = getConfig(transportCiphers, BrooklynWebConfig.TRANSPORT_CIPHERS);
        
        if (ksUrl!=null) {
            sslContextFactory.setKeyStorePath(getLocalKeyStorePath(ksUrl));
            if (Strings.isEmpty(ksPassword))
                throw new IllegalArgumentException("Keystore password is required and non-empty if keystore is specified.");
            sslContextFactory.setKeyStorePassword(ksPassword);
            if (Strings.isNonEmpty(ksCertAlias))
                sslContextFactory.setCertAlias(ksCertAlias);
        } else {
            log.info("No keystore specified but https enabled; creating a default keystore");
            
            if (Strings.isEmpty(ksCertAlias))
                ksCertAlias = "web-console";
            
            // if password is blank the process will block and read from stdin !
            if (Strings.isEmpty(ksPassword)) {
                ksPassword = Identifiers.makeRandomId(8);
                log.debug("created random password "+ksPassword+" for ad hoc internal keystore");
            }
            
            KeyStore ks = SecureKeys.newKeyStore();
            KeyPair key = SecureKeys.newKeyPair();
            X509Certificate cert = new FluentKeySigner("brooklyn").newCertificateFor("web-console", key);
            ks.setKeyEntry(ksCertAlias, key.getPrivate(), ksPassword.toCharArray(),
                new Certificate[] { cert });
            
            sslContextFactory.setKeyStore(ks);
            sslContextFactory.setKeyStorePassword(ksPassword);
            sslContextFactory.setCertAlias(ksCertAlias);
        }
        if (!Strings.isEmpty(truststorePath)) {
            sslContextFactory.setTrustStorePath(checkFileExists(truststorePath, "truststore"));
            sslContextFactory.setTrustStorePassword(trustStorePassword);
        }

        if (Strings.isNonBlank(trProtos)) {
            sslContextFactory.setIncludeProtocols(parseArray(trProtos));
        }
        if (Strings.isNonBlank(trCiphers)) {
            sslContextFactory.setIncludeCipherSuites(parseArray(trCiphers));
        }
        return sslContextFactory;
    }

    private String[] parseArray(String list) {
        List<String> arr = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(list);
        return arr.toArray(new String[arr.size()]);
    }

    private String getKeystoreUrl() {
        if (keystoreUrl != null) {
            if (Strings.isNonBlank(keystorePath) && !keystoreUrl.equals(keystorePath)) {
                log.warn("Deprecated 'keystorePath' supplied with different value than 'keystoreUrl', preferring the latter: "+
                        keystorePath+" / "+keystoreUrl);
            }
            return keystoreUrl;
        } else if (Strings.isNonBlank(keystorePath)) {
            log.warn("Deprecated 'keystorePath' used; callers should use 'keystoreUrl'");
            return keystorePath;
        } else {
            return managementContext.getConfig().getConfig(BrooklynWebConfig.KEYSTORE_URL);
        }
    }

    private <T> T getConfig(T override, ConfigKey<T> key) {
        if (override!=null) {
            return override;
        } else {
            return managementContext.getConfig().getConfig(key);
        }
    }

    private String getLocalKeyStorePath(String keystoreUrl) {
        ResourceUtils res = ResourceUtils.create(this);
        res.checkUrlExists(keystoreUrl, BrooklynWebConfig.KEYSTORE_URL.getName());
        if (new File(keystoreUrl).exists()) {
            return keystoreUrl;
        } else {
            InputStream keystoreStream;
            try {
                keystoreStream = res.getResourceFromUrl(keystoreUrl);
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                throw new IllegalArgumentException("Unable to access URL: "+keystoreUrl, e);
            }
            File tmp = Os.newTempFile("brooklyn-keystore", "ks");
            tmp.deleteOnExit();
            try {
                FileUtil.copyTo(keystoreStream, tmp);
            } finally {
                Streams.closeQuietly(keystoreStream);
            }
            return tmp.getAbsolutePath();
        }
    }

    private String newTimestampedDirName(String prefix, int randomSuffixLength) {
        return prefix + "-" + new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date()) + "-" + Identifiers.makeRandomId(randomSuffixLength);
    }
    
    private String checkFileExists(String path, String name) {
        if(!new File(path).exists()){
            throw new IllegalArgumentException("Could not find "+name+": "+path);
        }
        return path;
    }

    /**
     * Asks the app server to stop and waits for it to finish up.
     */
    public synchronized void stop() throws Exception {
        if (server==null) return;
        String root = getRootUrl();
        if (shutdownHook != null) Threads.removeShutdownHook(shutdownHook);
        if (log.isDebugEnabled())
            log.debug("Stopping Brooklyn web console at "+root+ " (" + war + (wars != null ? " and " + wars.values() : "") + ")");

        server.stop();
        try {
            server.join();
        } catch (Exception e) {
            /* NPE may be thrown e.g. if threadpool not started */
        }
        server = null;
        LocalhostMachineProvisioningLocation.releasePort(getAddress(), actualPort);
        actualPort = -1;
        if (log.isDebugEnabled())
            log.debug("Stopped Brooklyn web console at "+root);
    }

    private Thread shutdownHook = null;

    protected synchronized void addShutdownHook() {
        if (shutdownHook!=null) return;
        // some webapps can generate a lot of output if we don't shut down the browser first
        shutdownHook = Threads.addShutdownHook(new Runnable() {
            @Override
            public void run() {
                log.debug("BrooklynWebServer detected shutdown: stopping web-console");
                try {
                    stop();
                } catch (Exception e) {
                    log.error("Failure shutting down web-console: "+e, e);
                }
            }
        });
    }

    public WebAppContext deploy(String pathSpec, String war) {
        return deploy(new WebAppContextProvider(pathSpec, war));
    }

    /**
     * Serve the given WAR at the given pathSpec. If not yet started, it is remembered until start.
     * If the server is already running the context for this WAR is started.
     * @return the context created and added as a handler (and possibly already started if server is started,
     * so be careful with any changes you make to it!)
     */
    public WebAppContext deploy(WebAppContextProvider contextProvider) {
        WebAppContext context = contextProvider.get(managementContext, attributes, ignoreWebappDeploymentFailures);
        initSecurity(context);
        deploy(context);
        return context;
    }

    private void initSecurity(WebAppContext context) {
        if (skipSecurity) {
            // Could add <security-constraint> in an override web.xml here
            // instead of relying on the war having it (useful for downstream).
            // context.addOverrideDescriptor("override-web.xml");
            // But then should do the same in OSGi. For now require the web.xml
            // to have security pre-configured and ignore it if noConsoleSecurity used.
            //
            // Ignore security config in web.xml.
            context.setSecurityHandler(new NopSecurityHandler());
        } else {
            // Cover for downstream projects which don't have the changes.
            context.addOverrideDescriptor(getClass().getResource("/web-security.xml").toExternalForm());
        }
    }

    public void deploy(WebAppContext context) {
        try {
            handlers.updateHandler(context);
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }
    
    public Server getServer() {
        return server;
    }
    
    public WebAppContext getRootContext() {
        return rootContext;
    }

}
