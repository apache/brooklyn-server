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

package org.apache.brooklyn.location.jclouds.templates.customize;

// Has "2" in its name so JcloudsLocation can use it without having to use a fully qualified reference.
public class TemplateOptionCustomizers {

    private TemplateOptionCustomizers() {}

    public static TemplateOptionCustomizer autoAssignFloatingIp() {
        return new AutoAssignFloatingIpOption();
    }

    public static TemplateOptionCustomizer autoCreateFloatingIps() {
        return new AutoCreateFloatingIpsOption();
    }

    public static TemplateOptionCustomizer autoGenerateKeypairs() {
        return new AutoGenerateKeypairsOption();
    }

    public static TemplateOptionCustomizer domainName() {
        return new DomainNameOption();
    }

    public static TemplateOptionCustomizer extraPublicKeyDataToAuth() {
        return new ExtraPublicKeyDataToAuthOption();
    }

    public static TemplateOptionCustomizer inboundPorts() {
        return new InboundPortsOption();
    }

    public static TemplateOptionCustomizer keyPair() {
        return new KeyPairOption();
    }

    public static TemplateOptionCustomizer loginUser() {
        return new LoginUserOption();
    }

    public static TemplateOptionCustomizer loginUserPassword() {
        return new LoginUserPasswordOption();
    }

    public static TemplateOptionCustomizer loginUserPrivateKeyData() {
        return new LoginUserPrivateKeyDataOption();
    }

    public static TemplateOptionCustomizer loginUserPrivateKeyFile() {
        return new LoginUserPrivateKeyFileOption();
    }

    public static TemplateOptionCustomizer networkName() {
        return new NetworkNameOption();
    }

    public static TemplateOptionCustomizer runAsRoot() {
        return new RunAsRootOption();
    }

    public static TemplateOptionCustomizer securityGroups() {
        return new SecurityGroupOption();
    }

    public static TemplateOptionCustomizer stringTags() {
        return new StringTagsOption();
    }

    public static TemplateOptionCustomizer templateOptions() {
        return new TemplateOptionsOption();
    }

    public static TemplateOptionCustomizer userDataUuencoded() {
        return new UserDataUuencodedOption();
    }

    public static TemplateOptionCustomizer userMetadataMap() {
        return new UserMetadataMapOption();
    }

    public static TemplateOptionCustomizer userMetadataString() {
        return new UserMetadataStringOption();
    }

}
