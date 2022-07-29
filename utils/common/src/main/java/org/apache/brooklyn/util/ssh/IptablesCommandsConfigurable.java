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
package org.apache.brooklyn.util.ssh;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;

public class IptablesCommandsConfigurable {

    public enum Chain {
        INPUT, FORWARD, OUTPUT
    }

    public enum Policy {
        ACCEPT, REJECT, DROP, LOG
    }

    private final BashCommandsConfigurable bash;

    public IptablesCommandsConfigurable(BashCommandsConfigurable bash) {
        this.bash = bash;
    }

    /**
     * @deprecated since 0.7; use {@link org.apache.brooklyn.util.net.Protocol}; kept for persisted state backwards compatibility.
     */
    @Deprecated
    public enum Protocol {
        TCP("tcp"), UDP("udp"), ALL("all");

        final String protocol;

        private Protocol(String protocol) {
            this.protocol = protocol;
        }

        @Override
        public String toString() {
            return protocol;
        }

        org.apache.brooklyn.util.net.Protocol convert() {
            switch (this) {
                case TCP: return org.apache.brooklyn.util.net.Protocol.TCP;
                case UDP: return org.apache.brooklyn.util.net.Protocol.UDP;
                case ALL: return org.apache.brooklyn.util.net.Protocol.ALL;
                default: throw new IllegalStateException("Unexpected protocol "+this);
            }
        }
    }

    @Beta // implementation not portable across distros
    public String iptablesService(String cmd) {
        return bash.sudo(bash.alternatives(
                bash.ifExecutableElse1("service", "service iptables " + cmd),
                "/sbin/service iptables " + cmd));
    }

    @Beta // implementation not portable across distros
    public String iptablesServiceStop() {
        return iptablesService("stop");
    }

    @Beta // implementation not portable across distros
    public String iptablesServiceStart() {
        return iptablesService("start");
    }

    @Beta // implementation not portable across distros
    public String iptablesServiceRestart() {
        return iptablesService("restart");
    }

    @Beta // implementation not portable across distros
    public String iptablesServiceStatus() {
        return iptablesService("status");
    }

    @Beta // implementation not portable across distros
    public String firewalldService(String cmd) {
        return bash.sudo(bash.alternatives(
                bash.ifExecutableElse1("systemctl", "systemctl " + cmd + " firewalld"),
                "/usr/bin/systemctl " + cmd + " firewalld"));
    }

    @Beta // implementation not portable across distros
    public String firewalldServiceStop() {
        return firewalldService("stop");
    }

    @Beta // implementation not portable across distros
    public String firewalldServiceStart() {
        return firewalldService("start");
    }

    @Beta // implementation not portable across distros
    public String firewalldServiceRestart() {
        return firewalldService("restart");
    }

    @Beta // implementation not portable across distros
    public String firewalldServiceStatus() {
        return firewalldService("status");
    }

    @Beta // implementation not portable across distros
    public String firewalldServiceIsActive() {
        return firewalldService("is-active");
    }

    /**
     * Returns the command that saves iptables rules on file.
     *
     * @return Returns the command that saves iptables rules on file.
     *
     */
    public String saveIptablesRules() {
        return bash.alternatives(bash.sudo("service iptables save"),
                bash.chain(bash.installPackage("iptables-persistent"), bash.sudo("/etc/init.d/iptables-persistent save")));
    }

    /**
     * Returns the command that cleans up iptables rules.
     *
     * @return Returns the command that cleans up iptables rules.
     */
    public String cleanUpIptablesRules() {
       return bash.sudo("/sbin/iptables -F");
    }

    /**
     * Returns the iptables rules.
     *
     * @return Returns the command that list all the iptables rules.
     */
    public String listIptablesRule() {
       return bash.sudo("/sbin/iptables -L -v -n");
    }

    /**
     * Returns the command that inserts a rule on top of the iptables' rules to all interfaces.
     *
     * @return Returns the command that inserts a rule on top of the iptables'
     *         rules.
     */
    public String insertIptablesRule(Chain chain, org.apache.brooklyn.util.net.Protocol protocol, int port, Policy policy) {
        return addIptablesRule("-I", chain, Optional.<String> absent(), protocol, port, policy);
    }

    /**
     * Returns the command that inserts a rule on top of the iptables' rules.
     *
     * @return Returns the command that inserts a rule on top of the iptables'
     *         rules.
     */
    public String insertIptablesRule(Chain chain, String networkInterface, org.apache.brooklyn.util.net.Protocol protocol, int port, Policy policy) {
        return addIptablesRule("-I", chain, Optional.of(networkInterface), protocol, port, policy);
    }

    /**
     * Returns the command that appends a rule to iptables to all interfaces.
     *
     * @return Returns the command that appends a rule to iptables.
     */
    public String appendIptablesRule(Chain chain, org.apache.brooklyn.util.net.Protocol protocol, int port, Policy policy) {
        return addIptablesRule("-A", chain, Optional.<String> absent(), protocol, port, policy);
    }

    /**
     * Returns the command that appends a rule to iptables.
     *
     * @return Returns the command that appends a rule to iptables.
     */
    public String appendIptablesRule(Chain chain, String networkInterface, org.apache.brooklyn.util.net.Protocol protocol, int port, Policy policy) {
        return addIptablesRule("-A", chain, Optional.of(networkInterface), protocol, port, policy);
    }

    /**
     * Returns the command that creates a rule to iptables.
     *
     * @return Returns the command that creates a rule for iptables.
     */
    public String addIptablesRule(String direction, Chain chain, Optional<String> networkInterface, org.apache.brooklyn.util.net.Protocol protocol, int port, Policy policy) {
        String addIptablesRule;
        if(networkInterface.isPresent()) {
           addIptablesRule = String.format("/sbin/iptables %s %s -i %s -p %s --dport %d -j %s", direction, chain, networkInterface.get(), protocol, port, policy);
        } else {
           addIptablesRule = String.format("/sbin/iptables %s %s -p %s --dport %d -j %s", direction, chain, protocol, port, policy);
        }
        return bash.sudo(addIptablesRule);
    }

    /**
     * Returns the command that adds firewalld direct rule.
     *
     * @return Returns the command that adds firewalld direct rule.
     */
    public String addFirewalldRule(Chain chain, org.apache.brooklyn.util.net.Protocol protocol, int port, Policy policy) {
        return addFirewalldRule(chain, Optional.<String>absent(), protocol, port, policy);
    }
    
    /**
     * Returns the command that adds firewalld direct rule.
     *
     * @return Returns the command that adds firewalld direct rule.
     */
    public String addFirewalldRule(Chain chain, Optional<String> networkInterface, org.apache.brooklyn.util.net.Protocol protocol, int port, Policy policy) {
        String command = new String("/usr/bin/firewall-cmd");
        String commandPermanent = new String("/usr/bin/firewall-cmd --permanent");
        
        String interfaceParameter = String.format("%s", networkInterface.isPresent() ? " -i " + networkInterface.get() : "");
        
        String commandParameters = String.format(" --direct --add-rule ipv4 filter %s 0 %s -p %s --dport %d -j %s", 
                                                                chain, interfaceParameter,  protocol, port, policy);
        
        return bash.sudo(bash.chain(command + commandParameters, commandPermanent + commandParameters));
    }
}
