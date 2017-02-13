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
package org.apache.brooklyn.core.mgmt.persist;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.core.mgmt.persist.PersistenceObjectStore.StoreObjectAccessorWithLock;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

@Test
public class FileBasedStoreObjectAccessorWriterTest extends PersistenceStoreObjectAccessorWriterTestFixture {

    private static final Duration FILE_OPERATION_TIMEOUT = Duration.seconds(20);
    private static final String TEST_FILE_CONTENT = generateContent();

    private File file;

    private static String generateContent() {
        final char[] charArray = new char[4096];
        java.util.Arrays.fill(charArray, ' ');
        return new String(charArray);
    }

    @Override
    protected StoreObjectAccessorWithLock newPersistenceStoreObjectAccessor() throws IOException {
        file = Os.newTempFile(getClass(), "txt");
        return new StoreObjectAccessorLocking(new FileBasedStoreObjectAccessor(file, ".tmp"));
    }
    
    @Override
    protected Duration getLastModifiedResolution() {
        // OSX is 1s, Windows FAT is 2s !
        return Duration.seconds(2);
    }
    
    @Override
    @Test(groups="Integration")
    public void testLastModifiedTime() throws Exception {
        super.testLastModifiedTime();
    }
    
    @Test(groups="Integration")
    public void testFilePermissions600() throws Exception {
        accessor.put("abc");
        assertEquals(Files.readLines(file, Charsets.UTF_8), ImmutableList.of("abc"));
        
        FileBasedObjectStoreTest.assertFilePermission600(file);
    }

    // Fails ~3 times on 5000 runs in Virtualbox (Ubuntu Xenial).
    // Fails only with multiple threads.
    // Illustrates the problem which led to the increase of {@link RebindTestUtils#TIMEOUT} from 20 to 40 seconds.
    @Test(groups={"Integration", "Broken"}, invocationCount=5000)
    public void testSimpleOperationsDelay() throws Exception {
        Callable<Void> r = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                File tmp = Os.newTempFile(getClass(), "txt");
                try(Writer out = new FileWriter(tmp)) {
                    out.write(TEST_FILE_CONTENT);
                }
                tmp.delete();
                return null;
            }
        };

        final Future<Void> f1 = executor.submit(r);
        final Future<Void> f2 = executor.submit(r);

        CountdownTimer time = CountdownTimer.newInstanceStarted(FILE_OPERATION_TIMEOUT);
        f1.get(time.getDurationRemaining().toMilliseconds(), TimeUnit.MILLISECONDS);
        f2.get(time.getDurationRemaining().toMilliseconds(), TimeUnit.MILLISECONDS);
    }
    
    @Test(groups="Integration")
    public void testPutCreatesNewFile() throws Exception {
        File nonExistantFile = Os.newTempFile(getClass(), "txt");
        nonExistantFile.delete();
        StoreObjectAccessorLocking accessor = new StoreObjectAccessorLocking(new FileBasedStoreObjectAccessor(nonExistantFile, ".tmp"));
        try {
            accessor.put("abc");
            assertEquals(Files.readLines(nonExistantFile, Charsets.UTF_8), ImmutableList.of("abc"));
        } finally {
            accessor.delete();
        }
    }

    @Test(groups="Integration")
    public void testPutCreatesNewFileAndParentDir() throws Exception {
        File nonExistantDir = Os.newTempDir(getClass());
        nonExistantDir.delete();
        File nonExistantFile = new File(nonExistantDir, "file.txt");
        StoreObjectAccessorLocking accessor = new StoreObjectAccessorLocking(new FileBasedStoreObjectAccessor(nonExistantFile, ".tmp"));
        try {
            accessor.put("abc");
            assertEquals(Files.readLines(nonExistantFile, Charsets.UTF_8), ImmutableList.of("abc"));
        } finally {
            accessor.delete();
        }
    }
}
