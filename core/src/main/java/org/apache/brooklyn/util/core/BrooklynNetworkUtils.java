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
package org.apache.brooklyn.util.core;

import java.net.InetAddress;

import org.apache.brooklyn.core.location.geo.LocalhostExternalIpLoader;
import org.apache.brooklyn.core.server.BrooklynServiceAttributes;
import org.apache.brooklyn.util.JavaGroovyEquivalents;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.net.Networking;

/**
 * <tt>BrooklynNetworkUtils</tt> is for utility methods that rely on some other part(s) of Brooklyn,
 * or seem too custom in how they are used/configured to be considered a "common utility".
 * 
 * See {@link Networking} for more generic network utilities.
 */
public class BrooklynNetworkUtils {

    /** returns the externally-facing IP address from which this host comes, or 127.0.0.1 if not resolvable */
    public static String getLocalhostExternalIp() {
        return LocalhostExternalIpLoader.getLocalhostIpQuicklyOrDefault();
    }

    /** returns an IP address for localhost,
     * paying attention to system property 
     * {@link BrooklynServiceAttributes#LOCALHOST_IP_ADDRESS}
     * if set to prevent default selection when needed,
     * otherwise finding the first bindable/reachable NIC from a system lookup which usually
     * prefers IPv4 then non-loopback devices (but use the system property if if needed) */
    public static InetAddress getLocalhostInetAddress() {
        return TypeCoercions.coerce(JavaGroovyEquivalents.elvis(BrooklynServiceAttributes.LOCALHOST_IP_ADDRESS.getValue(),
                Networking.getLocalHost(true, false, true, true, 500)), InetAddress.class);
    }
}