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
package org.apache.brooklyn.location.jclouds;

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.jclouds.blobstore.BlobStoreContext;

/**
 * For creating a new {@link BlobStoreContext}, to access object stores such as S3 or Swift. 
 * 
 * @see {@link BlobStoreContextFactoryImpl}
 */
public interface BlobStoreContextFactory {

    // TODO Longer term, we could make this more testable by having the BlobStoreContextFactory configurable.
    // See the pattern used in {@link JcloudsLocationConfig#COMPUTE_SERVICE_REGISTRY}.
    //
    // However, for now we have just kept the separation of interface and implementation.
    
    public BlobStoreContext newBlobStoreContext(ConfigBag conf);
}
