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
package org.apache.brooklyn.container.location.kubernetes;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.LogWatcher;
import org.apache.brooklyn.test.LogWatcher.EventPredicates;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Identifiers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class KubernetesCertsTest {

    private List<File> tempFiles;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        tempFiles = Lists.newArrayList();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (tempFiles != null) {
            for (File tempFile : tempFiles) {
                tempFile.delete();
            }
        }
    }

    @Test
    public void testCertsAbsent() throws Exception {
        ConfigBag config = ConfigBag.newInstance();
        KubernetesCerts certs = new KubernetesCerts(config);

        assertFalse(certs.caCertData.isPresent());
        assertFalse(certs.clientCertData.isPresent());
        assertFalse(certs.clientKeyData.isPresent());
        assertFalse(certs.clientKeyAlgo.isPresent());
        assertFalse(certs.clientKeyPassphrase.isPresent());
    }

    @Test
    public void testCertsFromData() throws Exception {
        ConfigBag config = ConfigBag.newInstance(ImmutableMap.builder()
                .put(KubernetesLocationConfig.CA_CERT_DATA, "myCaCertData")
                .put(KubernetesLocationConfig.CLIENT_CERT_DATA, "myClientCertData")
                .put(KubernetesLocationConfig.CLIENT_KEY_DATA, "myClientKeyData")
                .put(KubernetesLocationConfig.CLIENT_KEY_ALGO, "myClientKeyAlgo")
                .put(KubernetesLocationConfig.CLIENT_KEY_PASSPHRASE, "myClientKeyPassphrase")
                .build());
        KubernetesCerts certs = new KubernetesCerts(config);

        assertEquals(certs.caCertData.get(), "myCaCertData");
        assertEquals(certs.clientCertData.get(), "myClientCertData");
        assertEquals(certs.clientKeyData.get(), "myClientKeyData");
        assertEquals(certs.clientKeyAlgo.get(), "myClientKeyAlgo");
        assertEquals(certs.clientKeyPassphrase.get(), "myClientKeyPassphrase");
    }

    @Test
    public void testCertsFromFile() throws Exception {
        ConfigBag config = ConfigBag.newInstance(ImmutableMap.builder()
                .put(KubernetesLocationConfig.CA_CERT_FILE, newTempFile("myCaCertData").getAbsolutePath())
                .put(KubernetesLocationConfig.CLIENT_CERT_FILE, newTempFile("myClientCertData").getAbsolutePath())
                .put(KubernetesLocationConfig.CLIENT_KEY_FILE, newTempFile("myClientKeyData").getAbsolutePath())
                .build());
        KubernetesCerts certs = new KubernetesCerts(config);

        assertEquals(certs.caCertData.get(), "myCaCertData");
        assertEquals(certs.clientCertData.get(), "myClientCertData");
        assertEquals(certs.clientKeyData.get(), "myClientKeyData");
    }

    @Test
    public void testCertsFailsIfConflictingConfig() throws Exception {
        ConfigBag config = ConfigBag.newInstance(ImmutableMap.builder()
                .put(KubernetesLocationConfig.CA_CERT_DATA, "myCaCertData")
                .put(KubernetesLocationConfig.CA_CERT_FILE, newTempFile("differentCaCertData").getAbsolutePath())
                .build());
        try {
            new KubernetesCerts(config);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "Duplicate conflicting configuration for caCertData and caCertFile");
        }
    }

    @Test
    public void testCertsWarnsIfConflictingConfig() throws Exception {
        ConfigBag config = ConfigBag.newInstance(ImmutableMap.builder()
                .put(KubernetesLocationConfig.CA_CERT_DATA, "myCaCertData")
                .put(KubernetesLocationConfig.CA_CERT_FILE, newTempFile("myCaCertData").getAbsolutePath())
                .build());

        String loggerName = KubernetesCerts.class.getName();
        ch.qos.logback.classic.Level logLevel = ch.qos.logback.classic.Level.WARN;
        Predicate<ILoggingEvent> filter = EventPredicates.containsMessage("Duplicate (matching) configuration for "
                + "caCertData and caCertFile (continuing)");
        LogWatcher watcher = new LogWatcher(loggerName, logLevel, filter);

        watcher.start();
        KubernetesCerts certs;
        try {
            certs = new KubernetesCerts(config);
            watcher.assertHasEvent();
        } finally {
            watcher.close();
        }

        assertEquals(certs.caCertData.get(), "myCaCertData");
    }

    @Test
    public void testCertsFailsIfFileNotFound() throws Exception {
        ConfigBag config = ConfigBag.newInstance(ImmutableMap.builder()
                .put(KubernetesLocationConfig.CA_CERT_FILE, "/path/to/fileDoesNotExist-" + Identifiers.makeRandomId(8))
                .build());
        try {
            new KubernetesCerts(config);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "not found on classpath or filesystem");
        }
    }

    private File newTempFile(String contents) throws Exception {
        File file = File.createTempFile("KubernetesCertsTest", ".txt");
        tempFiles.add(file);
        Files.write(contents, file, Charsets.UTF_8);
        return file;
    }
}
