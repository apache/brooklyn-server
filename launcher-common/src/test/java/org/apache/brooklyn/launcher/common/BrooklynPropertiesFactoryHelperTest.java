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
package org.apache.brooklyn.launcher.common;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.util.exceptions.FatalRuntimeException;
import org.apache.brooklyn.util.io.FileUtil;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.StringFunctions;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.google.common.io.Files;

public class BrooklynPropertiesFactoryHelperTest {

    @Test(groups="UNIX")
    public void testChecksGlobalBrooklynPropertiesPermissionsX00() throws Exception {
        File propsFile = File.createTempFile("testChecksGlobalBrooklynPropertiesPermissionsX00", ".properties");
        propsFile.setReadable(true, false);
        try {
            new BrooklynPropertiesFactoryHelper(propsFile.getAbsolutePath(), null)
                    .createPropertiesBuilder();

            Assert.fail("Should have thrown");
        } catch (FatalRuntimeException e) {
            if (!e.toString().contains("Invalid permissions for file")) throw e;
        } finally {
            propsFile.delete();
        }
    }

    @Test(groups="UNIX")
    public void testChecksLocalBrooklynPropertiesPermissionsX00() throws Exception {
        File propsFile = File.createTempFile("testChecksLocalBrooklynPropertiesPermissionsX00", ".properties");
        propsFile.setReadable(true, false);
        try {
            new BrooklynPropertiesFactoryHelper(propsFile.getAbsolutePath(), null)
                .createPropertiesBuilder();

            Assert.fail("Should have thrown");
        } catch (FatalRuntimeException e) {
            if (!e.toString().contains("Invalid permissions for file")) throw e;
        } finally {
            propsFile.delete();
        }
    }

    @Test(groups="UNIX")
    public void testStartsWithSymlinkedBrooklynPropertiesPermissionsX00() throws Exception {
        File dir = Files.createTempDir();
        Path globalPropsFile = java.nio.file.Files.createFile(Paths.get(dir.toString(), "globalProps.properties"));
        Path globalSymlink = java.nio.file.Files.createSymbolicLink(Paths.get(dir.toString(), "globalLink"), globalPropsFile);
        Path localPropsFile = java.nio.file.Files.createFile(Paths.get(dir.toString(), "localPropsFile.properties"));
        Path localSymlink = java.nio.file.Files.createSymbolicLink(Paths.get(dir.toString(), "localLink"), localPropsFile);

        Files.write(getMinimalLauncherPropertiesString() + "key_in_global=1", globalPropsFile.toFile(), Charset.defaultCharset());
        Files.write("key_in_local=2", localPropsFile.toFile(), Charset.defaultCharset());
        FileUtil.setFilePermissionsTo600(globalPropsFile.toFile());
        FileUtil.setFilePermissionsTo600(localPropsFile.toFile());
        try {
            BrooklynProperties props = new BrooklynPropertiesFactoryHelper(
                    globalSymlink.toAbsolutePath().toString(),
                    localSymlink.toAbsolutePath().toString())
                .createPropertiesBuilder()
                .build();
            
            assertEquals(props.getFirst("key_in_global"), "1");
            assertEquals(props.getFirst("key_in_local"), "2");
        } finally {
            Os.deleteRecursively(dir);
        }
    }

    @Test
    public void testStartsWithBrooklynPropertiesPermissionsX00() throws Exception {
        File globalPropsFile = File.createTempFile("testChecksLocalBrooklynPropertiesPermissionsX00_global", ".properties");
        Files.write(getMinimalLauncherPropertiesString()+"key_in_global=1", globalPropsFile, Charset.defaultCharset());
        File localPropsFile = File.createTempFile("testChecksLocalBrooklynPropertiesPermissionsX00_local", ".properties");
        Files.write("key_in_local=2", localPropsFile, Charset.defaultCharset());
        FileUtil.setFilePermissionsTo600(globalPropsFile);
        FileUtil.setFilePermissionsTo600(localPropsFile);
        try {
            BrooklynProperties props = new BrooklynPropertiesFactoryHelper(
                    globalPropsFile.getAbsolutePath(),
                    localPropsFile.getAbsolutePath())
                .createPropertiesBuilder()
                .build();
            
            assertEquals(props.getFirst("key_in_global"), "1");
            assertEquals(props.getFirst("key_in_local"), "2");
        } finally {
            globalPropsFile.delete();
            localPropsFile.delete();
        }
    }
    
    public static String getMinimalLauncherPropertiesString() throws IOException {
        BrooklynProperties p1 = LocalManagementContextForTests.builder(true).buildProperties();
        Properties p = new Properties();
        p.putAll(Maps.transformValues(p1.asMapWithStringKeys(), StringFunctions.toStringFunction()));
        Writer w = new StringWriter();
        p.store(w, "test");
        w.close();
        return w.toString()+"\n";
    }


}
