package org.apache.brooklyn.util.yorml.serializers;

import java.util.Map;

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.yorml.YormlConfig;
import org.apache.brooklyn.util.yorml.YormlContext;
import org.apache.brooklyn.util.yorml.YormlException;
import org.apache.brooklyn.util.yorml.YormlInternals.YormlContinuation;
import org.apache.brooklyn.util.yorml.YormlReadContext;
import org.apache.brooklyn.util.yorml.YormlSerializer;

public class InstantiateType implements YormlSerializer {

    @Override
    public YormlContinuation read(YormlReadContext context, YormlConfig config, Map<Object,Object> blackboard) {
        if (context.getJavaObject()!=null) return YormlContinuation.CONTINUE_UNCHANGED;
        if (!(context.getYamlObject() instanceof Map)) return YormlContinuation.CONTINUE_UNCHANGED;
        Object type = ((Map<?,?>)context.getYamlObject()).get("type");
        if (type==null) return YormlContinuation.CONTINUE_UNCHANGED;
        if (!(type instanceof String)) throw new YormlException("type must be a string");
        
        Object result = config.getTypeRegistry().newInstance((String)type);
        if (result==null) return YormlContinuation.CONTINUE_UNCHANGED;
        
        context.setJavaObject(result);
        return YormlContinuation.RESTART;
    }

    @Override
    public YormlContinuation write(YormlContext context, YormlConfig config, Map<Object,Object> blackboard) {
        if (context.getYamlObject()==null) {
            MutableMap<Object, Object> map = MutableMap.of();
            context.setYamlObject(map);
            // TODO primitives. map+list. type registry plain types. osgi.
            map.put("type", "java:"+context.getJavaObject().getClass().getName());
            // TODO put fields remaining in TODO on blackboard, then check
            return YormlContinuation.RESTART;
        }
        return YormlContinuation.CONTINUE_UNCHANGED;
    }

    @Override
    public String document(String type, YormlConfig config) {
        // TODO Auto-generated method stub
        return null;
    }

}
