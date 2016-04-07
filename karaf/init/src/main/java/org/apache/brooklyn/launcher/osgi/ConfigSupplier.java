/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.launcher.osgi;

import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.brooklyn.util.collections.MutableMap;
import org.osgi.service.cm.Configuration;

import com.google.common.base.Supplier;

public class ConfigSupplier implements Supplier<Map<?, ?>> {
    private Map<?, ?> props;

    public ConfigSupplier(@Nullable Configuration config) {
        if (config != null) {
            this.props = dictToMap(config.getProperties());
        } else {
            this.props = MutableMap.of();
        }
    }

    @Override
    public Map<?, ?> get() {
        return props;
    }

    public void update(Map<?, ?> props) {
        this.props = props;
    }

    private static Map<?, ?> dictToMap(Dictionary<?, ?> props) {
        Map<Object, Object> mapProps = MutableMap.of();
        Enumeration<?> keyEnum = props.keys();
        while (keyEnum.hasMoreElements()) {
            Object key = keyEnum.nextElement();
            mapProps.put(key, props.get(key));
        }
        return mapProps;
    }

}
