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
package org.apache.brooklyn.util.core.units;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.List;
import org.apache.brooklyn.core.resolve.jackson.JsonSymbolDependentDeserializer;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.units.Range.RangeDeserializer;

@JsonDeserialize(using = RangeDeserializer.class)
public class Range extends MutableList<Object> {

    public Range() {}
    public Range(List<Object> l) {
        setValue(l);
    }

    public boolean add(Object o) {
        if (size() >= 2) throw new IllegalStateException("Range must be of size 2; cannot add '"+o+"' when currently "+this);
        if (o instanceof String && "unbounded".equalsIgnoreCase((String)o)) {
            o = isEmpty() ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        }
        if (!(o instanceof Integer)) throw new IllegalStateException("Invalid value for range; must be an integer or 'UNBOUNDED'");
        return super.add((Integer)o);
    }

    public int min() { return (Integer) get(0); }
    public int max() { return (Integer) get(1); }

    private void setValue(List<Object> l) {
        if (l.size() != 2) throw new IllegalStateException("Range must be of size 2; cannot create from "+l);
        l.forEach(this::add);
    }

    // TODO this could be replaced by a ConstructorMatchingSymbolDependentDeserializer
    public static class RangeDeserializer extends JsonSymbolDependentDeserializer {
        @Override
        protected Object deserializeArray(JsonParser p) throws IOException {
            return new Range( (List<Object>) super.deserializeArray(p) );
        }
        @Override
        protected JsonDeserializer<?> getArrayDeserializer() throws IOException {
            return ctxt.getFactory().createCollectionDeserializer(ctxt,
                            ctxt.getTypeFactory().constructCollectionType(List.class, Object.class),
                            getBeanDescription());
        }
    }

}
