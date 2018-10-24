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
package org.apache.brooklyn.core.catalog.internal;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.RegisteredType;

class RegisteredTypesSupplier implements Supplier<Iterable<RegisteredType>> {
    private final ManagementContext mgmt;
    private final Predicate<? super RegisteredType> filter;

    RegisteredTypesSupplier(ManagementContext mgmt, Predicate<? super RegisteredType> predicate) {
        this.mgmt = mgmt;
        this.filter = predicate;
    }
    @Override
    public Iterable<RegisteredType> get() {
        return mgmt.getTypeRegistry().getMatching(filter);
    }
}
