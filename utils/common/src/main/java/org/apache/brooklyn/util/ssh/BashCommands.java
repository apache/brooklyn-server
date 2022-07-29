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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.StringEscapes.BashStringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class BashCommands {

    private static final BashCommandsConfigurable instance = BashCommandsConfigurable.newInstance();

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static final String INSTALL_TAR = instance.INSTALL_TAR;

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static final String INSTALL_CURL = instance.INSTALL_CURL;

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static final String INSTALL_WGET = instance.INSTALL_WGET;

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static final String INSTALL_ZIP = instance.INSTALL_ZIP;

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static final String INSTALL_UNZIP = instance.INSTALL_UNZIP;

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static final String INSTALL_SYSSTAT = instance.INSTALL_SYSSTAT;


    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installExecutable(Map<?,?> flags, String executable) { return instance.installExecutable(flags, executable); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installExecutable(String executable) { return instance.installExecutable(executable); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String quiet(String command) { return instance.quiet(command); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String ok(String command) { return instance.ok(command); }

    public static String sudo(String command) { return instance.sudo(command); }

    public static String authSudo(String command, String password) { return instance.authSudo(command, password); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String sudoAsUser(String user, String command) { return instance.sudoAsUser(user, command); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String addSbinPathCommand() { return instance.addSbinPathCommand(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String sbinPath() { return instance.sbinPath(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String executeCommandThenAsUserTeeOutputToFile(String commandWhoseOutputToWrite, String user, String file) { return instance.executeCommandThenAsUserTeeOutputToFile(commandWhoseOutputToWrite, user, file); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String dontRequireTtyForSudo() { return instance.dontRequireTtyForSudo(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String generateKeyInDotSshIdRsaIfNotThere() { return instance.generateKeyInDotSshIdRsaIfNotThere(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String ifFileExistsElse0(String path, String command) { return instance.ifFileExistsElse0(path, command); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String ifFileExistsElse1(String path, String command) { return instance.ifFileExistsElse1(path, command); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String ifExecutableElse0(String executable, String command) { return instance.ifExecutableElse0(executable, command); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String ifExecutableElse1(String executable, String command) { return instance.ifExecutableElse1(executable, command); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String ifNotExecutable(String command, String statement) { return instance.ifNotExecutable(command, statement); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String onlyIfExecutableMissing(String executable, String command) { return instance.onlyIfExecutableMissing(executable, command); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String ifExecutableElse(String command, String ifNotExist, String ifExist) { return instance.ifExecutableElse(command, ifNotExist, ifExist); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String ifExecutableDoesNotExistElse(String command, String ifNotExist, String ifExist) { return instance.ifExecutableDoesNotExistElse(command, ifNotExist, ifExist); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static ImmutableList<String> ifExecutableElse(String command, List<String> ifNotExist, List<String> ifExist) { return instance.ifExecutableElse(command, ifNotExist, ifExist); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static ImmutableList<String> ifExecutableDoesNotExistElse(String command, List<String> ifNotExist, List<String> ifExist) { return instance.ifExecutableDoesNotExistElse(command, ifNotExist, ifExist); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String chain(Collection<String> commands) { return instance.chain(commands); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String chain(String ...commands) { return instance.chain(commands); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String chainGroup(Collection<String> commands) { return instance.chainGroup(commands); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String chainGroup(String ...commands) { return instance.chainGroup(commands); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String chainSubshell(Collection<String> commands) { return instance.chainSubshell(commands); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String chainSubshell(String ...commands) { return instance.chainSubshell(commands); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String alternatives(Collection<String> commands) { return instance.alternatives(commands); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String alternatives(String ...commands) { return instance.alternatives(commands); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String alternativesGroup(Collection<String> commands) { return instance.alternativesGroup(commands); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String alternativesGroup(String ...commands) { return instance.alternativesGroup(commands); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String alternativesSubshell(Collection<String> commands) { return instance.alternativesSubshell(commands); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String alternativesSubshell(String ...commands) { return instance.alternativesSubshell(commands); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String formatIfNotNull(String pattern, Object arg) { return instance.formatIfNotNull(pattern, arg); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installPackage(String packageDefaultName) { return instance.installPackage(packageDefaultName); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installPackage(Map<?,?> flags, String packageDefaultName) { return instance.installPackage(flags, packageDefaultName); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installPackageOrFail(Map<?,?> flags, String packageDefaultName) { return instance.installPackageOrFail(flags, packageDefaultName); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installPackageOr(Map<?,?> flags, String packageDefaultName, String optionalCommandToRunIfNone) { return instance.installPackageOr(flags, packageDefaultName, optionalCommandToRunIfNone); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String warn(String message) { return instance.warn(message); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String fail(String message, int code) { return instance.fail(message, code); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String require(String command, String failureMessage, int exitCode) { return instance.require(command, failureMessage, exitCode); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String require(String command, String failureMessage) { return instance.require(command, failureMessage); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String requireTest(String test, String failureMessage, int exitCode) { return instance.requireTest(test, failureMessage, exitCode); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String requireTest(String test, String failureMessage) { return instance.requireTest(test, failureMessage); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String requireFile(String file) { return instance.requireFile(file); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String requireExecutable(String command) { return instance.requireExecutable(command); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String waitForFileContents(String file, String desiredContent, Duration timeout, boolean failOnTimeout) { return instance.waitForFileContents(file, desiredContent, timeout, failOnTimeout); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String waitForFileExists(String file, Duration timeout, boolean failOnTimeout) { return instance.waitForFileExists(file, timeout, failOnTimeout); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String waitForPortFree(int port, Duration timeout, boolean failOnTimeout) { return instance.waitForPortFree(port, timeout, failOnTimeout); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String unzip(String file, String targetDir) { return instance.unzip(file, targetDir); }


    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static List<String> commandsToDownloadUrlsAs(List<String> urls, String saveAs) { return instance.commandsToDownloadUrlsAs(urls, saveAs); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static List<String> commandsToDownloadUrlsAsWithMinimumTlsVersion(List<String> urls, String saveAs, String tlsVersion) { return instance.commandsToDownloadUrlsAsWithMinimumTlsVersion(urls, saveAs, tlsVersion); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String commandToDownloadUrlsAs(List<String> urls, String saveAs) { return instance.commandToDownloadUrlsAs(urls, saveAs); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String commandToDownloadUrlAs(String url, String saveAs) { return instance.commandToDownloadUrlAs(url, saveAs); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String downloadToStdout(List<String> urls) { return instance.downloadToStdout(urls); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String downloadToStdout(String ...urls) { return instance.downloadToStdout(urls); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String simpleDownloadUrlAs(List<String> urls, String saveAs) { return instance.simpleDownloadUrlAs(urls, saveAs); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String simpleDownloadUrlAs(List<String> urls, String saveAs, String tlsVersion) { return instance.simpleDownloadUrlAs(urls, saveAs, tlsVersion); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String simpleDownloadUrlAs(List<String> urls, String user, String password, String saveAs, String tlsVersion) { return instance.simpleDownloadUrlAs(urls, user, password, saveAs, tlsVersion); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installJava(int version) { return instance.installJava(version); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installJava6() { return instance.installJava6(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installJava7() { return instance.installJava7(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installJava8() { return instance.installJava8(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installJava6IfPossible() { return instance.installJava6IfPossible(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installJava7IfPossible() { return instance.installJava7IfPossible(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installJava8IfPossible() { return instance.installJava8IfPossible(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installJava6OrFail() { return instance.installJava6OrFail(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installJava7OrFail() { return instance.installJava7OrFail(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installJava7Or6OrFail() { return instance.installJava7Or6OrFail(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installJavaLatestOrFail() { return instance.installJavaLatestOrFail(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String installJavaLatestOrWarn() { return instance.installJavaLatestOrWarn(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String addOpenJDKPPK() { return instance.addOpenJDKPPK(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String upgradeNSS() { return instance.upgradeNSS(); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String pipeTextTo(String text, String command) { return instance.pipeTextTo(text, command); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String pipeTextToFile(String text, String filepath) { return instance.pipeTextToFile(text, filepath); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String prependToEtcHosts(String ip, String... hostnames) { return instance.prependToEtcHosts(ip, hostnames); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static String appendToEtcHosts(String ip, String... hostnames) { return instance.appendToEtcHosts(ip, hostnames); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static List<String> setHostname(String newHostname) { return instance.setHostname(newHostname); }

    @Deprecated /** @deprecated since 1.1 use {@link BashCommandsConfigurable} */
    public static List<String> setHostname(String hostPart, String domainPart) { return instance.setHostname(hostPart, domainPart); }

}
