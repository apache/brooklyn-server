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
package org.apache.brooklyn.util.yoml;

import org.apache.brooklyn.util.yoml.internal.YomlContext;

public class YomlException extends RuntimeException {

    private static final long serialVersionUID = 7825908737102292499L;
    
    YomlContext context;
    
    public YomlException(String message) { super(message); }
    public YomlException(String message, Throwable cause) { super(message, cause); }
    public YomlException(String message, YomlContext context) { this(message); this.context = context; }
    public YomlException(String message, YomlContext context, Throwable cause) { this(message, cause); this.context = context; }

    public YomlContext getContext() {
        return context;
    }
    
    @Override
    public String getMessage() {
        if (context==null) return getBaseMessage();
        return getBaseMessage() + " ("+context+")";
    }
    
    public String getBaseMessage() {
        return super.getMessage();
    }
    
}
