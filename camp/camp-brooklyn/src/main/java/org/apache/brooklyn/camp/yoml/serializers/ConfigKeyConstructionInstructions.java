package org.apache.brooklyn.camp.yoml.serializers;

import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstruction;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstructions.WrappingConstructionInstruction;

public class ConfigKeyConstructionInstructions {

    /** As the others, but expecting a one-arg constructor which takes a config map; 
     * this will deduce the appropriate values by looking at 
     * the inheritance declarations at the keys and at the values at the outer and inner constructor instructions. */
    public static ConstructionInstruction newUsingConfigKeyMapConstructor(Class<?> type, 
            Map<String, Object> values, 
            ConstructionInstruction optionalOuter,
            Map<String, ConfigKey<?>> keysByAlias) {
        return new ConfigKeyMapConstructionWithArgsInstruction(type, values, (WrappingConstructionInstruction) optionalOuter,
            keysByAlias);
    }

    public static ConstructionInstruction newUsingConfigBagConstructor(Class<?> type,
            Map<String, Object> values, 
            ConstructionInstruction optionalOuter,
            Map<String, ConfigKey<?>> keysByAlias) {
        return new ConfigBagConstructionWithArgsInstruction(type, values, (WrappingConstructionInstruction) optionalOuter,
            keysByAlias);
    }

}
