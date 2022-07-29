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
        INPUT, FORWARD, OUTPUT
    }

    public enum Policy {
        ACCEPT, REJECT, DROP, LOG
    }

    private static final IptablesCommandsConfigurable instance = new IptablesCommandsConfigurable(BashCommandsConfigurable.newInstance());

    @Beta // implementation not portable across distros
    public String iptablesServiceStop() {
        return instance.iptablesServiceStop();
    }

    @Beta // implementation not portable across distros
    public String iptablesServiceStart() {
        return instance.iptablesServiceStart();
    }

    @Beta // implementation not portable across distros
    public String iptablesServiceRestart() {
        return instance.iptablesServiceRestart();
    }

    @Beta // implementation not portable across distros
    public String iptablesServiceStatus() {
        return instance.iptablesServiceStatus();
    }

    @Beta // implementation not portable across distros
    public String firewalldService(String cmd) {
        return instance.firewalldService(cmd);
    }

    @Beta // implementation not portable across distros
    public String firewalldServiceStop() {
        return instance.firewalldServiceStop();
    }

    @Beta // implementation not portable across distros
    public String firewalldServiceStart() {
        return instance.firewalldServiceStart();
    }

    @Beta // implementation not portable across distros
    public String firewalldServiceRestart() {
        return instance.firewalldServiceRestart();
    }

    @Beta // implementation not portable across distros
    public String firewalldServiceStatus() {
        return instance.firewalldServiceStatus();
    }

    @Beta // implementation not portable across distros
    public String firewalldServiceIsActive() {
        return instance.firewalldServiceIsActive();
    }

    /**
     * Returns the command that saves iptables rules on file.
     *
     * @return Returns the command that saves iptables rules on file.
     *
     */
    public String saveIptablesRules() {
        return instance.saveIptablesRules();
    }

    /**
     * Returns the command that cleans up iptables rules.
     *
     * @return Returns the command that cleans up iptables rules.
     */
    public String cleanUpIptablesRules() {
        return instance.cleanUpIptablesRules();
    }

    /**
     * Returns the iptables rules.
     *
     * @return Returns the command that list all the iptables rules.
     */
    public String listIptablesRule() {
        return instance.listIptablesRule();
    }

    /**
     * Returns the command that inserts a rule on top of the iptables' rules to all interfaces.
     *
     * @return Returns the command that inserts a rule on top of the iptables'
     *         rules.
     */
    public String insertIptablesRule(IptablesCommandsConfigurable.Chain chain, org.apache.brooklyn.util.net.Protocol protocol, int port, IptablesCommandsConfigurable.Policy policy) {
        return instance.insertIptablesRule(chain, protocol, port, policy);
    }

    /**
     * Returns the command that inserts a rule on top of the iptables' rules.
     *
     * @return Returns the command that inserts a rule on top of the iptables'
     *         rules.
     */
    public String insertIptablesRule(IptablesCommandsConfigurable.Chain chain, String networkInterface, org.apache.brooklyn.util.net.Protocol protocol, int port, IptablesCommandsConfigurable.Policy policy) {
        return instance.insertIptablesRule(chain, networkInterface, protocol, port, policy);
    }

    /**
     * Returns the command that appends a rule to iptables to all interfaces.
     *
     * @return Returns the command that appends a rule to iptables.
     */
    public String appendIptablesRule(IptablesCommandsConfigurable.Chain chain, org.apache.brooklyn.util.net.Protocol protocol, int port, IptablesCommandsConfigurable.Policy policy) {
        return instance.appendIptablesRule(chain, protocol, port, policy);
    }

    /**
     * Returns the command that appends a rule to iptables.
     *
     * @return Returns the command that appends a rule to iptables.
     */
    public String appendIptablesRule(IptablesCommandsConfigurable.Chain chain, String networkInterface, org.apache.brooklyn.util.net.Protocol protocol, int port, IptablesCommandsConfigurable.Policy policy) {
        return instance.appendIptablesRule(chain, networkInterface, protocol, port, policy);
    }

    /**
     * Returns the command that creates a rule to iptables.
     *
     * @return Returns the command that creates a rule for iptables.
     */
    public String addIptablesRule(String direction, IptablesCommandsConfigurable.Chain chain, Optional<String> networkInterface, org.apache.brooklyn.util.net.Protocol protocol, int port, IptablesCommandsConfigurable.Policy policy) {
        return instance.addIptablesRule(direction, chain, networkInterface, protocol, port, policy);
    }

    /**
     * Returns the command that adds firewalld direct rule.
     *
     * @return Returns the command that adds firewalld direct rule.
     */
    public String addFirewalldRule(IptablesCommandsConfigurable.Chain chain, org.apache.brooklyn.util.net.Protocol protocol, int port, IptablesCommandsConfigurable.Policy policy) {
        return instance.addFirewalldRule(chain, protocol, port, policy);
    }

    /**
     * Returns the command that adds firewalld direct rule.
     *
     * @return Returns the command that adds firewalld direct rule.
     */
    public String addFirewalldRule(IptablesCommandsConfigurable.Chain chain, Optional<String> networkInterface, org.apache.brooklyn.util.net.Protocol protocol, int port, IptablesCommandsConfigurable.Policy policy) {

        return instance.addFirewalldRule(chain, networkInterface, protocol, port, policy);
    }

}
