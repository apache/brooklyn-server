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

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.io.ByteSource;

public class InMemoryObjectStore implements PersistenceObjectStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryObjectStore.class);

    final Map<String,byte[]> filesByName;
    final Map<String, Date> fileModTimesByName;
    boolean prepared = false;
    
    public InMemoryObjectStore() {
        this(MutableMap.<String,byte[]>of(), MutableMap.<String,Date>of());
    }
    
    public InMemoryObjectStore(Map<String,byte[]> map, Map<String, Date> fileModTimesByName) {
        filesByName = map;
        this.fileModTimesByName = fileModTimesByName;
        log.debug("Using memory-based objectStore");
    }
    
    @Override
    public String getSummaryName() {
        return "in-memory (test) persistence store";
    }
    
    @Override
    public void prepareForMasterUse() {
    }

    @Override
    public void createSubPath(String subPath) {
    }

    @Override
    public StoreObjectAccessor newAccessor(final String path) {
        if (!prepared) throw new IllegalStateException("prepare method not yet invoked: "+this);
        return new StoreObjectAccessorLocking(new SingleThreadedInMemoryStoreObjectAccessor(filesByName, fileModTimesByName, path));
    }
    
    public static class SingleThreadedInMemoryStoreObjectAccessor implements StoreObjectAccessor {
        private final Map<String, byte[]> map;
        private final Map<String, Date> mapModTime;
        private final String key;

        public SingleThreadedInMemoryStoreObjectAccessor(Map<String,byte[]> map, Map<String, Date> mapModTime, String key) {
            this.map = map;
            this.mapModTime = mapModTime;
            this.key = key;
        }
        @Override
        public String get() {
            byte[] b = getBytes();
            if (b==null) return null;
            return new String(b);
        }
        @Override
        public byte[] getBytes() {
            synchronized (map) {
                return map.get(key);
            }
        }
        @Override
        public boolean exists() {
            synchronized (map) {
                return map.containsKey(key);
            }
        }
        @Override
        public void put(String val) {
            put(ByteSource.wrap(val.getBytes()));
        }
        @Override
        public void put(ByteSource bytes) {
            try {
                synchronized (map) {
                    map.put(key, Streams.readFullyAndClose(bytes.openStream()));
                    mapModTime.put(key, new Date());
                }
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            }
        }
        @Override
        public void append(String val) {
            synchronized (map) {
                String val2 = get();
                if (val2==null) val2 = val;
                else val2 = val2 + val;

                put(val2);
                mapModTime.put(key, new Date());
            }
        }
        @Override
        public void delete() {
            synchronized (map) {
                map.remove(key);
                mapModTime.remove(key);
            }
        }
        @Override
        public Date getLastModifiedDate() {
            synchronized (map) {
                return mapModTime.get(key);
            }
        }
    }

    @Override
    public List<String> listContentsWithSubPath(final String parentSubPath) {
        if (!prepared) throw new IllegalStateException("prepare method not yet invoked: "+this);
        synchronized (filesByName) {
            List<String> result = MutableList.of();
            for (String file: filesByName.keySet())
                if (file.startsWith(parentSubPath))
                    result.add(file);
            return result;
        }
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("size", filesByName.size()).toString();
    }

    @Override
    public void injectManagementContext(ManagementContext mgmt) {
    }
    
    @Override
    public void prepareForSharedUse(PersistMode persistMode, HighAvailabilityMode haMode) {
        prepared = true;
    }

    @Override
    public void deleteCompletely() {
        synchronized (filesByName) {
            filesByName.clear();
        }
    }

}
