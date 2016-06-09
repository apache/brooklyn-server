package org.apache.brooklyn.util.yorml.serializers;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yorml.YormlConfig;
import org.apache.brooklyn.util.yorml.YormlContext;
import org.apache.brooklyn.util.yorml.YormlException;
import org.apache.brooklyn.util.yorml.YormlInternals.YormlContinuation;
import org.apache.brooklyn.util.yorml.YormlReadContext;
import org.apache.brooklyn.util.yorml.YormlSerializer;

public class FieldsInFieldsMap implements YormlSerializer {


    @Override
    public YormlContinuation read(YormlReadContext context, YormlConfig config, Map<Object,Object> blackboard) {
        // TODO refactor to combine with InstantiateType
        if (context.getJavaObject()==null) return YormlContinuation.CONTINUE_UNCHANGED;
        if (!(context.getYamlObject() instanceof Map)) return YormlContinuation.CONTINUE_UNCHANGED;
        Object fields = ((Map<?,?>)context.getYamlObject()).get("fields");
        if (fields==null) return YormlContinuation.CONTINUE_UNCHANGED;
        if (!(fields instanceof Map)) throw new YormlException("fields must be a map");
        
        Object target = context.getJavaObject();
        for (Object f: ((Map<?,?>)fields).keySet()) {
            Object v = ((Map<?,?>)fields).get(f);
            try {
                Field ff = Reflections.findField(target.getClass(), Strings.toString(f));
                if (ff==null) {
                    // just skip (could throw, but leave it in case something else recognises)
                } else if (Modifier.isStatic(ff.getModifiers())) {
                    // as above
                } else {
                    ff.setAccessible(true);
                    ff.set(target, v);
                    ((Map<?,?>)fields).remove(Strings.toString(f));
                    if (((Map<?,?>)fields).isEmpty()) {
                        ((Map<?,?>)context.getYamlObject()).remove("fields");
                    }
                }
            } catch (Exception e) { throw Exceptions.propagate(e); }
        }
        return YormlContinuation.CONTINUE_CHANGED;
    }

    @SuppressWarnings("unchecked")
    @Override
    public YormlContinuation write(YormlContext context, YormlConfig config, Map<Object,Object> blackboard) {
        if (context.getYamlObject() instanceof Map) {
            Map<String,Object> fields = (Map<String, Object>) ((Map<?,?>)context.getYamlObject()).get("fields");
            if (fields==null) {
                fields = MutableMap.of();
                // TODO proper check. all fields. blackboard. etc.
                fields.put("color", "red");
                ((Map<Object,Object>)context.getYamlObject()).put("fields", fields);
                return YormlContinuation.RESTART;
            }
        }
        return YormlContinuation.CONTINUE_UNCHANGED;
    }

    @Override
    public String document(String type, YormlConfig config) {
        // TODO Auto-generated method stub
        return null;
    }

}
