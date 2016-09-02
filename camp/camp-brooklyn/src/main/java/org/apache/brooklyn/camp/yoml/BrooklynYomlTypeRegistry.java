package org.apache.brooklyn.camp.yoml;

import java.util.Collection;
import java.util.Set;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.core.mgmt.classloading.JavaBrooklynClassLoadingContext;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.Yoml;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.YomlTypeRegistry;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;

/** 
 * Provides a bridge so YOML can find things in the Brooklyn type registry.
 */
public class BrooklynYomlTypeRegistry implements YomlTypeRegistry {

    private ManagementContext mgmt;
    
    /*
     * NB, there are a few subtleties here around library loading.  For instance, given:
     * 
     * - id: x
     *   item: { type: X }
     * - id: x2
     *   item: { type: x }
     * - id: cluster-x
     *   item: { type: cluster, children: [ { type: x }, { type: X } ] }
     *   
     * Assume these items declare different libraries.
     * 
     * We *need* libraries to be used when resolving the reference to a parent java type (e.g. x's parent type X).
     * 
     * We *don't* want libraries to be used transitively, e.g. when x2 or cluster-x refers to x, 
     * only x's libraries apply to loading X, not x2's libraries.
     * 
     * We *may* want libraries to be used when resolving references to types in a plan besides the parent type;
     * e.g. cluster-x's libraries will be needed when resolving its reference to it's child of declared type X.
     * But we might *NOT* want to support that, and could instead require that any type referenced elsewhere
     * be defined as an explicit YAML type in the registry. This will simplify our lives as we will force all 
     * java objects to be explicitly defined as a type in the registry. But it means we won't be able to parse
     * current plans so for the moment this is deferred, and we'd want to go through a "warning" cycle when
     * applying. 
     * 
     * (In that last case we could even be stricter and say that any yaml types
     * should have a simple single yaml-java bridge eg using a JavaClassNameTypeImplementationPlan
     * to facilitate reverse lookup.)
     * 
     * The defaultLoadingContext is used for the first and third cases above.
     * The second case is handled by calling back to the registry with a limited context.
     * (Note it will require some work to be able to distinguish between the third case and the first,
     * as we don't currently have that contextual information when methods here are called.)
     *
     * <p>
     * 
     * One bit of ugly remains:
     * 
     * If we install v1 then v2, cluster-x:1 will pick up x:2.
     * We could change this:
     * - by encouraging explicit `type: x:1` in the definition of cluster-x
     * - by allowing `type: x:.` version shorthand, where `.` means the same version as the calling type
     * - by recording locally-preferred types (aka "friends") on a registered type, 
     *   and looking at those friends first; this would be nice in that we could allow private friends
     *   
     * Yoml.read(...) can take a special "top-level type extensions" in order to find friends,
     * and/or a special "top-level libraries" in order to resolve types in the first instance.
     * These would *not* be passed when resolving a found type.
     */
    private RegisteredTypeLoadingContext defaultLoadingContext;

    public BrooklynYomlTypeRegistry(ManagementContext mgmt, RegisteredTypeLoadingContext defaultLoadingContext) {
        this.mgmt = mgmt;
        this.defaultLoadingContext = defaultLoadingContext;
        
    }
    
    protected BrooklynTypeRegistry registry() {
        return mgmt.getTypeRegistry();
    }
    
    @Override
    public Maybe<Object> newInstanceMaybe(String typeName, Yoml yoml) {
        return newInstanceMaybe(typeName, yoml, defaultLoadingContext);
    }
    
    public Maybe<Object> newInstanceMaybe(String typeName, Yoml yoml, RegisteredTypeLoadingContext context) {
        RegisteredType typeR = registry().get(typeName, context);
        
        if (typeR!=null) {
            RegisteredTypeLoadingContext nextContext = null;
            if (context==null) {
                nextContext = RegisteredTypeLoadingContexts.alreadyEncountered(MutableSet.of(typeName));
            } else {
                if (!context.getAlreadyEncounteredTypes().contains(typeName)) {
                    // we lose any other contextual information, but that seems _good_ since it was used to find the type,
                    // we probably now want to shift to the loading context of that type, e.g. the x2 example above;
                    // we just need to ensure it doesn't try to load a super which is also a sub!
                    nextContext = RegisteredTypeLoadingContexts.alreadyEncountered(context.getAlreadyEncounteredTypes(), typeName);
                }
            }
            if (nextContext==null) {
                // fall through to path below; we have a circular reference, so need to load java instead
            } else {
                return Maybe.of(registry().create(typeR, context, null));
            }
        }
        
        Class<?> t = null;
        {
            Exception e = null;
            try {
                t = getJavaType(null, typeName, context);
            } catch (Exception ee) {
                Exceptions.propagateIfFatal(ee);
                e = ee;
            }
            if (t==null) {
                return Maybe.absent("Neither the type registry nor the classpath could load type "+typeName+
                    (e!=null && !Exceptions.isRootBoringClassNotFound(e, typeName) ? ": "+Exceptions.collapseText(e) : ""));
            }
        }
        try {
            return Maybe.of((Object)t.newInstance());
        } catch (Exception e) {
            return Maybe.absent("Error instantiating type "+typeName+": "+Exceptions.collapseText(e));
        }
    }

    @Override
    public Object newInstance(String typeN, Yoml yoml) {
        return newInstanceMaybe(typeN, yoml).orNull();
    }

    static class YomlClassNotFoundException extends RuntimeException {
        private static final long serialVersionUID = 5946251753146668070L;
        public YomlClassNotFoundException(String message, Throwable cause) {
            super(message, cause);
        }
    }
    
    @Override
    public Class<?> getJavaType(String typeName) {
        try {
            return getJavaType(registry().get(typeName), typeName, defaultLoadingContext);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            return null;
        }
    }

    // TODO use maybe
    protected Class<?> getJavaType(RegisteredType typeR, String typeName, RegisteredTypeLoadingContext context) {
        Class<?> result = null;
            
        if (typeR!=null) {
            result = RegisteredTypes.peekActualJavaType(typeR);
            if (result!=null) return result;
            // instantiate
            System.out.println("PRE-LOADING to determine type for "+typeR);
            Maybe<Object> m = newInstanceMaybe(typeName, null, context);
            System.out.println("PRE-LOADING DONE, got "+m+" for "+typeR);
            if (m.isPresent()) result = m.get().getClass();
        }
        
        if (result==null && typeName==null) return null;
        
        // strip generics for the purposes here
        if (typeName.indexOf('<')>0) typeName = typeName.substring(0, typeName.indexOf('<'));
        
        if (result==null) result = Boxing.boxedType(Boxing.getPrimitiveType(typeName).orNull());
        if (result==null && YomlUtils.TYPE_STRING.equals(typeName)) result = String.class;
        
        if (result==null) {  // && typeName.startsWith("java:")) {   // TODO require java: prefix?
            typeName = Strings.removeFromStart(typeName, "java:");
            BrooklynClassLoadingContext loader = null;
            if (context!=null && context.getLoader()!=null) 
                loader = context.getLoader();
            else
                loader = JavaBrooklynClassLoadingContext.create(mgmt);
            
            Maybe<Class<?>> resultM = loader.tryLoadClass(typeName);
            // TODO give better error if absent?
            result = resultM.get();
        }
        
        if (typeR!=null && result!=null) {
            RegisteredTypes.cacheActualJavaType(typeR, result);
        }
        return result;
    }

    @Override
    public String getTypeName(Object obj) {
        // TODO reverse lookup??
        return null;
    }

    @Override
    public <T> String getTypeNameOfClass(Class<T> type) {
        if (type==null) return null;
        // TODO reverse lookup??
        // look in catalog for something where plan matches and consists only of type
        return getDefaultTypeNameOfClass(type);
    }
    
    protected <T> String getDefaultTypeNameOfClass(Class<T> type) {
        Maybe<String> primitive = Boxing.getPrimitiveName(type);
        if (primitive.isPresent()) return primitive.get();
        if (String.class.equals(type)) return "string";
        // map and list handled by those serializers
        return "java:"+type.getName();
    }


    @Override
    public void collectSerializers(String typeName, Collection<YomlSerializer> serializers, Set<String> typesVisited) {
        // TODO wtf?
    }

}
