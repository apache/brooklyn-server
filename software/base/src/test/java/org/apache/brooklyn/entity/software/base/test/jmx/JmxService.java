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
package org.apache.brooklyn.entity.software.base.test.jmx;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.ObjectName;
import javax.management.StandardEmitterMBean;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import mx4j.tools.naming.NamingService;
import mx4j.tools.naming.NamingServiceMBean;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.entity.java.UsesJmx;
import org.apache.brooklyn.feed.jmx.JmxHelper;
import org.apache.brooklyn.test.NetworkingTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Set up a JMX service ready for clients to connect. This consists of an MBean server, a connector server and a naming
 * service.
 */
public class JmxService {
    private static final Logger logger = LoggerFactory.getLogger(JmxService.class);

    public static JmxService newJmxServiceRetrying(String host, int retries) throws Exception {
        List<Integer> portsToTry = Lists.newArrayList();
        for (int i = 0; i < retries; i++) {
            portsToTry.add((int)(11000+(5000*Math.random())));
        }
        return JmxService.newJmxServiceRetrying(host, PortRanges.fromIterable(portsToTry));
    }
    
    public static JmxService newJmxServiceRetrying(String host, PortRange portRange) throws Exception {
        int size = Iterables.size(portRange);
        int attempt = 0;
        Exception lastException = null;
        for (int port : portRange) {
            attempt++;
            try {
                return new JmxService(host, port);
            } catch (Exception e) {
                logger.debug("Unable to create JMX service during test using port "+port+" (attempt "+attempt+" of "+size+")");
                lastException = e;
            }
        }
        if (lastException == null) throw new NoSuchElementException("No ports in range "+portRange);
        throw lastException;
    }


    private MBeanServer server;
    private NamingServiceMBean namingServiceMBean;
    private JMXConnectorServer connectorServer;
    private String jmxHost;
    private int jmxPort;
    private String url;

    public JmxService() throws Exception {
        this("localhost", NetworkingTestUtils.randomPortAround(28000));
        
        // TODO why this message if the constructor is not actually deprecated, and it seems useful?
        //logger.warn("use of deprecated default host and port in JmxService");
    }
    
    /**
     * @deprecated since 0.6.0; either needs abandoning, or updating to support JmxSupport (and JmxmpAgent, etc) */
    @Deprecated
    public JmxService(Entity e) throws Exception {
        this(e.getAttribute(Attributes.HOSTNAME) != null ? e.getAttribute(Attributes.HOSTNAME) : "localhost", 
                 e.getAttribute(UsesJmx.JMX_PORT) != null ? e.getAttribute(UsesJmx.JMX_PORT) : null);
    }
    
    public JmxService(String jmxHost, Integer jmxPort) throws Exception {
        this.jmxHost = jmxHost;
        Preconditions.checkNotNull(jmxPort, "JMX_PORT must be set when starting JmxService"); 
        this.jmxPort = jmxPort;
        url = JmxHelper.toRmiJmxUrl(jmxHost, jmxPort, jmxPort, "jmxrmi");

        try {
            JMXServiceURL address = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + jmxHost + ":" + jmxPort + "/jmxrmi");
            connectorServer = JMXConnectorServerFactory.newJMXConnectorServer(address, null, null);
            server = MBeanServerFactory.createMBeanServer();
            ObjectName cntorServerName = ObjectName.getInstance("connectors:protocol=rmi");
            server.registerMBean(connectorServer, cntorServerName);
    
            ObjectName naming = new ObjectName("Naming:type=registry");
            server.registerMBean(new NamingService(jmxPort), naming);
            Object proxy = MBeanServerInvocationHandler.newProxyInstance(server, naming, NamingServiceMBean.class, false);
            namingServiceMBean = (NamingServiceMBean) proxy;
            for (int i=0; ; i++) {
                try {
                    namingServiceMBean.start();
                    break;
                } catch (Exception e) {
                    // may take a bit of time for port to be available, if it had just been used
                    if (i==0) logger.warn("JmxService couldn't start test mbean ("+e+"); will delay and retry");
                    else if (i>=180) {
                        logger.warn("JmxService couldn't start test mbean ("+e+"); definitive; throwing "+e);
                        throw e;
                    } else {
                        logger.debug("JmxService couldn't start test mbean on retry attempt "+i+" ("+e+"); will continue retrying");
                    }
                    Thread.sleep(1000);
                }
            }
    
            connectorServer.start();
            logger.info("JMX tester service started at URL {}", address);
        } catch (Exception e) {
            try {
                shutdown();
            } catch (Exception e2) {
                logger.warn("Error shutting down JmxService, after error during startup; rethrowing original error", e2);
            }
            throw e;
        }
    }

    public int getJmxPort() {
        return jmxPort;
    }
    
    public void shutdown() throws IOException {
        if (connectorServer != null) connectorServer.stop();
        if (namingServiceMBean != null) namingServiceMBean.stop();
        if (server != null) MBeanServerFactory.releaseMBeanServer(server);
        connectorServer = null;
        namingServiceMBean = null;
        server = null;
        logger.info("JMX tester service stopped ({}:{})", jmxHost, jmxPort);
    }

    public String getUrl() {
        return url;
    }
    
    public GeneralisedDynamicMBean registerMBean(String name) throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException, MalformedObjectNameException, NullPointerException {
        return registerMBean(ImmutableMap.of(), ImmutableMap.of(), name);
    }

    /**
     * Construct a {@link GeneralisedDynamicMBean} and register it with this MBean server.
     *
     * @param initialAttributes a {@link Map} of attributes that make up the MBean's initial set of attributes and their * values
     * @param name the name of the MBean
     * @return the newly created and registered MBean
     * @throws NullPointerException 
     * @throws MalformedObjectNameException 
     * @throws NotCompliantMBeanException 
     * @throws MBeanRegistrationException 
     * @throws InstanceAlreadyExistsException 
     */
    @SuppressWarnings({ "rawtypes" })
    public GeneralisedDynamicMBean registerMBean(Map initialAttributes, String name) throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException, MalformedObjectNameException, NullPointerException {
        return registerMBean(initialAttributes, ImmutableMap.of(), name);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public GeneralisedDynamicMBean registerMBean(Map initialAttributes, Map operations, String name) throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException, MalformedObjectNameException, NullPointerException {
        GeneralisedDynamicMBean mbean = new GeneralisedDynamicMBean(initialAttributes, operations);
        server.registerMBean(mbean, new ObjectName(name));
        return mbean;
    }
    
    public StandardEmitterMBean registerMBean(List<String> notifications, String name) throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException, MalformedObjectNameException, NullPointerException {
        String[] types = notifications.toArray(new String[0]);
        MBeanNotificationInfo info = new MBeanNotificationInfo(types, Notification.class.getName(), "Notification");
        NotificationEmitter emitter = new NotificationBroadcasterSupport(info);
        StandardEmitterMBean mbean = new StandardEmitterMBean(emitter, NotificationEmitter.class, emitter);
        server.registerMBean(mbean, new ObjectName(name));
        return mbean;
    }
}
