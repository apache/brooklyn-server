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
package org.apache.brooklyn.util.exceptions;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Wraps a throwable in something which does not serialize the original, but which does preserve the message after serialization.
 */
public class LossySerializingThrowable extends RuntimeException {

    transient Throwable error;
    String type;

    // jackson constructor
    private LossySerializingThrowable() {}

    public LossySerializingThrowable(Throwable error) {
        super(Exceptions.collapseText(error), null, false, false);
        this.error = error;
        this.type = error.getClass().getCanonicalName();
    }

    @JsonIgnore
    public Throwable getError() {
        if (error!=null) return error;
        return new RuntimeException(getMessage()!=null ? getMessage() : type);
    }

    @JsonIgnore
    public String getType() {
        return type;
    }
}
