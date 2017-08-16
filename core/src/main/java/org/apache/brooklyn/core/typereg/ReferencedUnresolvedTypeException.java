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
package org.apache.brooklyn.core.typereg;

import org.apache.brooklyn.api.typereg.RegisteredType;

/** Indicates a type has requested to resolve another type which is not resolved or not resolvable */
public class ReferencedUnresolvedTypeException extends UnsupportedTypePlanException {

    private static final long serialVersionUID = -5590108442839125317L;

    public ReferencedUnresolvedTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReferencedUnresolvedTypeException(String message) {
        super(message);
    }

    public ReferencedUnresolvedTypeException(RegisteredType t) {
        this(t, false, null);
    }
    
    public ReferencedUnresolvedTypeException(RegisteredType t, boolean attemptedAnyway, Throwable cause) {
        this("Reference to known type "+t.getVersionedName()+" in plan but it's definition is unresolved (recursive plan or premature evaluation?)"
            + (attemptedAnyway ? "; attempted to read definition again" : ""), cause);
    }
    
    public ReferencedUnresolvedTypeException(Throwable cause) {
        super(cause);
    }

}
