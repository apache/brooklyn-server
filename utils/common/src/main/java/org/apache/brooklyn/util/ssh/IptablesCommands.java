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

@Deprecated /** @deprecated since 1.1 use {@link IptablesCommandsConfigurable} */
public class IptablesCommands {

    public enum Chain {
        INPUT, FORWARD, OUTPUT;
        public IptablesCommandsConfigurable.Chain adapted() {
            return IptablesCommandsConfigurable.Chain.valueOf(name());
        }
    }

    public enum Policy {
        ACCEPT, REJECT, DROP, LOG;
        public IptablesCommandsConfigurable.Policy adapted() {
            return IptablesCommandsConfigurable.Policy.valueOf(name());
        }
    }

    private static final IptablesCommandsConfigurable instance = new IptablesCommandsConfigurable(BashCommandsConfigurable.newInstance());

    @Beta // implementation not portable across distros
    public static String iptablesServiceStop() {
        return instance.iptablesServiceStop();
    }

    @Beta // implementation not portable across distros
    public static String iptablesServiceStart() {
        return instance.iptablesServiceStart();
    }

    @Beta // implementation not portable across distros
    public static String iptablesServiceRestart() {
        return instance.iptablesServiceRestart();
    }

    @Beta // implementation not portable across distros
    public static String iptablesServiceStatus() {
        return instance.iptablesServiceStatus();
    }

    @Beta // implementation not portable across distros
    public static String firewalldService(String cmd) {
        return instance.firewalldService(cmd);
    }

    @Beta // implementation not portable across distros
    public static String firewalldServiceStop() {
        return instance.firewalldServiceStop();
    }

    @Beta // implementation not portable across distros
    public static String firewalldServiceStart() {
        return instance.firewalldServiceStart();
    }

    @Beta // implementation not portable across distros
    public static String firewalldServiceRestart() {
        return instance.firewalldServiceRestart();
    }

    @Beta // implementation not portable across distros
    public static String firewalldServiceStatus() {
        return instance.firewalldServiceStatus();
    }

    @Beta // implementation not portable across distros
    public static String firewalldServiceIsActive() {
        return instance.firewalldServiceIsActive();
    }

    /**
     * Returns the command that saves iptables rules on file.
     *
     * @return Returns the command that saves iptables rules on file.
     *
     */
    public static String saveIptablesRules() {
        return instance.saveIptablesRules();
    }

    /**
     * Returns the command that cleans up iptables rules.
     *
     * @return Returns the command that cleans up iptables rules.
     */
    public static String cleanUpIptablesRules() {
        return instance.cleanUpIptablesRules();
    }

    /**
     * Returns the iptables rules.
     *
     * @return Returns the command that list all the iptables rules.
     */
    public static String listIptablesRule() {
        return instance.listIptablesRule();
    }

    /**
     * Returns the command that inserts a rule on top of the iptables' rules to all interfaces.
     *
     * @return Returns the command that inserts a rule on top of the iptables'
     *         rules.
     */
    public static String insertIptablesRule(Chain chain, org.apache.brooklyn.util.net.Protocol protocol, int port, Policy policy) {
        return instance.insertIptablesRule(chain.adapted(), protocol, port, policy.adapted());
    }

    /**
     * Returns the command that inserts a rule on top of the iptables' rules.
     *
     * @return Returns the command that inserts a rule on top of the iptables'
     *         rules.
     */
    public static String insertIptablesRule(Chain chain, String networkInterface, org.apache.brooklyn.util.net.Protocol protocol, int port, Policy policy) {
        return instance.insertIptablesRule(chain.adapted(), networkInterface, protocol, port, policy.adapted());
    }

    /**
     * Returns the command that appends a rule to iptables to all interfaces.
     *
     * @return Returns the command that appends a rule to iptables.
     */
    public static String appendIptablesRule(Chain chain, org.apache.brooklyn.util.net.Protocol protocol, int port, Policy policy) {
        return instance.appendIptablesRule(chain.adapted(), protocol, port, policy.adapted());
    }

    /**
     * Returns the command that appends a rule to iptables.
     *
     * @return Returns the command that appends a rule to iptables.
     */
    public static String appendIptablesRule(Chain chain, String networkInterface, org.apache.brooklyn.util.net.Protocol protocol, int port, Policy policy) {
        return instance.appendIptablesRule(chain.adapted(), networkInterface, protocol, port, policy.adapted());
    }

    /**
     * Returns the command that creates a rule to iptables.
     *
     * @return Returns the command that creates a rule for iptables.
     */
    public static String addIptablesRule(String direction, Chain chain, Optional<String> networkInterface, org.apache.brooklyn.util.net.Protocol protocol, int port, Policy policy) {
        return instance.addIptablesRule(direction, chain.adapted(), networkInterface, protocol, port, policy.adapted());
    }

    /**
     * Returns the command that adds firewalld direct rule.
     *
     * @return Returns the command that adds firewalld direct rule.
     */
    public static String addFirewalldRule(Chain chain, org.apache.brooklyn.util.net.Protocol protocol, int port, Policy policy) {
        return instance.addFirewalldRule(chain.adapted(), protocol, port, policy.adapted());
    }

    /**
     * Returns the command that adds firewalld direct rule.
     *
     * @return Returns the command that adds firewalld direct rule.
     */
    public static String addFirewalldRule(Chain chain, Optional<String> networkInterface, org.apache.brooklyn.util.net.Protocol protocol, int port, Policy policy) {
        return instance.addFirewalldRule(chain.adapted(), networkInterface, protocol, port, policy.adapted());
    }

}
