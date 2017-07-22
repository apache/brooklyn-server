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
package org.apache.brooklyn.entity.stock;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.entity.trait.AsyncStartable;

/**
 * An app that starts up asynchronously. Calling start will call start on its children,
 * but it does not expect the children to have started by the time the start() effector
 * has returned. Instead, it infers from the children's state whether they are up or not.
 */
@ImplementedBy(AsyncApplicationImpl.class)
public interface AsyncApplication extends StartableApplication, AsyncStartable {
}
