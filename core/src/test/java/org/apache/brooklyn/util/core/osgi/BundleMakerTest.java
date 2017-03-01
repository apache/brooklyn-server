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
package org.apache.brooklyn.util.core.osgi;

import static org.apache.brooklyn.test.Asserts.assertEquals;
import static org.apache.brooklyn.test.Asserts.assertFalse;
import static org.apache.brooklyn.test.Asserts.assertNotNull;
import static org.apache.brooklyn.test.Asserts.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.osgi.framework.Bundle;
import org.osgi.framework.Constants;
import org.osgi.framework.Version;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

public class BundleMakerTest extends BrooklynMgmtUnitTestSupport {

    private BundleMaker bundleMaker;
    private File emptyJar;
    private File tempJar;
    private File generatedJar;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();
        super.setUp();
        
        bundleMaker = new BundleMaker(mgmt);
        emptyJar = createEmptyJarFile();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (emptyJar != null) emptyJar.delete();
        if (tempJar != null) tempJar.delete();
        if (generatedJar != null) generatedJar.delete();
    }
    
    @Test
    public void testCopyAdding() throws Exception {
        generatedJar = bundleMaker.copyAdding(emptyJar, ImmutableMap.of(new ZipEntry("myfile.txt"), new ByteArrayInputStream("mytext".getBytes())));
        assertJarContents(generatedJar, ImmutableMap.of("myfile.txt", "mytext"));
    }
    
    @Test
    public void testCopyAddingToNonEmpty() throws Exception {
        tempJar = bundleMaker.copyAdding(emptyJar, ImmutableMap.of(new ZipEntry("preExisting.txt"), new ByteArrayInputStream("myPreExisting".getBytes())));
        generatedJar = bundleMaker.copyAdding(tempJar, ImmutableMap.of(new ZipEntry("myfile.txt"), new ByteArrayInputStream("mytext".getBytes())));
        assertJarContents(generatedJar, ImmutableMap.of("preExisting.txt", "myPreExisting", "myfile.txt", "mytext"));
    }
    
    @Test
    public void testCopyAddingOverwritesEntry() throws Exception {
        tempJar = bundleMaker.copyAdding(emptyJar, ImmutableMap.of(new ZipEntry("myfile.txt"), new ByteArrayInputStream("myPreExisting".getBytes())));
        generatedJar = bundleMaker.copyAdding(tempJar, ImmutableMap.of(new ZipEntry("myfile.txt"), new ByteArrayInputStream("mytext".getBytes())));
        assertJarContents(generatedJar, ImmutableMap.of("myfile.txt", "mytext"));
    }
    
    @Test
    public void testCopyAddingManifest() throws Exception {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.2.3"); // needs version, or nothing added to manifest!
        manifest.getMainAttributes().putValue("mykey", "myval");
        generatedJar = bundleMaker.copyAddingManifest(emptyJar, manifest);
        
        String expectedManifest = 
                "Manifest-Version: 1.2.3\r\n" + 
                "mykey: myval\r\n" +
                "\r\n";
        assertJarContents(generatedJar, ImmutableMap.of(JarFile.MANIFEST_NAME, expectedManifest));
    }
    
    @Test
    public void testCopyAddingManifestByMap() throws Exception {
        Map<String, String> manifest = ImmutableMap.of(Attributes.Name.MANIFEST_VERSION.toString(), "1.2.3", "mykey", "myval");
        generatedJar = bundleMaker.copyAddingManifest(emptyJar, manifest);
        
        String expectedManifest = 
                "Manifest-Version: 1.2.3\r\n" + 
                "mykey: myval\r\n" +
                "\r\n";
        assertJarContents(generatedJar, ImmutableMap.of(JarFile.MANIFEST_NAME, expectedManifest));
    }
    
    @Test
    public void testCopyAddingManifestOverwritesExisting() throws Exception {
        Map<String, String> origManifest = ImmutableMap.of(Attributes.Name.MANIFEST_VERSION.toString(), "4.5.6", "preExistingKey", "preExistingVal");
        tempJar = bundleMaker.copyAddingManifest(emptyJar, origManifest);
        
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.2.3");
        manifest.getMainAttributes().putValue("mykey", "myval");
        generatedJar = bundleMaker.copyAddingManifest(tempJar, manifest);
        
        String expectedManifest = 
                "Manifest-Version: 1.2.3\r\n" + 
                "mykey: myval\r\n" +
                "\r\n";
        assertJarContents(generatedJar, ImmutableMap.of(JarFile.MANIFEST_NAME, expectedManifest));
    }
    
    @Test
    public void testCopyRemovingPredicate() throws Exception {
        tempJar = bundleMaker.copyAdding(emptyJar, ImmutableMap.of(
                new ZipEntry("myfile.txt"), new ByteArrayInputStream("mytext".getBytes()),
                new ZipEntry("myfile2.txt"), new ByteArrayInputStream("mytext2".getBytes())));
        generatedJar = bundleMaker.copyRemoving(tempJar, Predicates.equalTo("myfile.txt"));
        assertJarContents(generatedJar, ImmutableMap.of("myfile.txt", "mytext"));
    }
    
    @Test
    public void testCopyRemovingItems() throws Exception {
        tempJar = bundleMaker.copyAdding(emptyJar, ImmutableMap.of(
                new ZipEntry("myfile.txt"), new ByteArrayInputStream("mytext".getBytes()),
                new ZipEntry("myfile2.txt"), new ByteArrayInputStream("mytext2".getBytes())));
        generatedJar = bundleMaker.copyRemoving(tempJar, ImmutableSet.of("myfile.txt"));
        assertJarContents(generatedJar, ImmutableMap.of("myfile2.txt", "mytext2"));
    }
    
    // TODO Not supported - can't remove an entire directory like this
    @Test(enabled=false)
    public void testCopyRemovingItemDir() throws Exception {
        tempJar = bundleMaker.copyAdding(emptyJar, ImmutableMap.of(
                new ZipEntry("mydir/myfile.txt"), new ByteArrayInputStream("mytext".getBytes()),
                new ZipEntry("mydir2/myfile2.txt"), new ByteArrayInputStream("mytext2".getBytes())));
        generatedJar = bundleMaker.copyRemoving(tempJar, ImmutableSet.of("mydir"));
        assertJarContents(generatedJar, ImmutableMap.of("mydir2/myfile2.txt", "mytext2"));
    }
    
    @Test
    public void testCopyRemovingItemsUnmatched() throws Exception {
        tempJar = bundleMaker.copyAdding(emptyJar, ImmutableMap.of(new ZipEntry("myfile.txt"), new ByteArrayInputStream("mytext".getBytes())));
        generatedJar = bundleMaker.copyRemoving(tempJar, ImmutableSet.of("wrong.txt"));
        assertJarContents(generatedJar, ImmutableMap.of("myfile.txt", "mytext"));
    }
    
    @Test
    public void testHasOsgiManifestWhenNoManifest() throws Exception {
        assertFalse(bundleMaker.hasOsgiManifest(emptyJar));
    }
    
    @Test
    public void testHasOsgiManifestWhenInvalidManifest() throws Exception {
        Map<String, String> manifest = ImmutableMap.of(Attributes.Name.MANIFEST_VERSION.toString(), "1.2.3", "mykey", "myval");
        generatedJar = bundleMaker.copyAddingManifest(emptyJar, manifest);
        assertFalse(bundleMaker.hasOsgiManifest(generatedJar));
    }
    
    @Test
    public void testHasOsgiManifestWhenValidManifest() throws Exception {
        Map<String, String> manifest = ImmutableMap.of(Attributes.Name.MANIFEST_VERSION.toString(), "1.2.3", Constants.BUNDLE_SYMBOLICNAME, "myname");
        generatedJar = bundleMaker.copyAddingManifest(emptyJar, manifest);
        assertTrue(bundleMaker.hasOsgiManifest(generatedJar));
    }
    
    @Test
    public void testCreateJarFromClasspathDirNoManifest() throws Exception {
        generatedJar = bundleMaker.createJarFromClasspathDir("/org/apache/brooklyn/util/core/osgi/test/bundlemaker/nomanifest");
        assertJarContents(generatedJar, ImmutableMap.of("myfile.txt", "mytext", "subdir/myfile2.txt", "mytext2"));
    }
    
    @Test
    public void testCreateJarFromClasspathDirWithManifest() throws Exception {
        generatedJar = bundleMaker.createJarFromClasspathDir("/org/apache/brooklyn/util/core/osgi/test/bundlemaker/withmanifest");
        
        String expectedManifest = 
                "Manifest-Version: 1.2.3\r\n" + 
                "mykey: myval\r\n" +
                "\r\n";
        assertJarContents(generatedJar, ImmutableMap.of(JarFile.MANIFEST_NAME, expectedManifest, "myfile.txt", "mytext", "subdir/myfile2.txt", "mytext2"));
    }
    
    @Test
    public void testInstallBundle() throws Exception {
        Map<String, String> manifest = ImmutableMap.of(
                Attributes.Name.MANIFEST_VERSION.toString(), "1.2.3", 
                Constants.BUNDLE_VERSION, "4.5.6",
                Constants.BUNDLE_SYMBOLICNAME, "myname");
        generatedJar = bundleMaker.copyAddingManifest(emptyJar, manifest);
        
        Bundle bundle = bundleMaker.installBundle(generatedJar, false);
        assertEquals(bundle.getSymbolicName(), "myname");
        assertEquals(bundle.getVersion(), new Version("4.5.6"));
        
        // Confirm it really is installed in the management context's OSGi framework
        Bundle bundle2 = Osgis.bundleFinder(mgmt.getOsgiManager().get().getFramework())
                .symbolicName("myname")
                .version("4.5.6")
                .findUnique()
                .get();
        assertEquals(bundle2, bundle);
    }

    private File createEmptyJarFile() throws Exception {
        File result = Os.newTempFile("base", "jar");
        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(result));
        zos.close();
        return result;
    }
    
    private void assertJarContents(File f, Map<String, String> expectedContents) throws Exception {
        ZipFile zipFile = new ZipFile(f);
        String zipEntriesMsg = "entries="+enumerationToList(zipFile.entries());
        try {
            for (Map.Entry<String, String> entry : expectedContents.entrySet()) {
                ZipEntry zipEntry = zipFile.getEntry(entry.getKey());
                assertNotNull(zipEntry, "No entry for "+entry.getKey()+"; "+zipEntriesMsg);
                String entryContents = Streams.readFullyString(zipFile.getInputStream(zipEntry));
                assertEquals(entryContents, entry.getValue());
            }
            assertEquals(zipFile.size(), expectedContents.size(), zipEntriesMsg);
            
        } finally {
            zipFile.close();
        }
    }
    
    private <T> List<T> enumerationToList(Enumeration<T> e) {
        List<T> result = Lists.newArrayList();
        while (e.hasMoreElements()) {
            result.add(e.nextElement());
        }
        return result;
        
    }
}
