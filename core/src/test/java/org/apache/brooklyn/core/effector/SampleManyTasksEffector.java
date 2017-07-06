package org.apache.brooklyn.core.effector;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.EffectorTasks.EffectorTaskFactory;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;

/** Effector which can be used to create a lot of tasks with delays.
 * Mainly used for manual UI testing, with a blueprint such as the following:

<pre>

name: Test with many tasks
location: localhost
services:
- type: org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess
  brooklyn.initializers:
  - type: org.apache.brooklyn.core.effector.SampleManyTasksEffector
  launch.command: |
    echo hello | nc -l 4321 &
    echo $! > $PID_FILE
    ## to experiment with errors or sleeping
    # sleep 10
    # exit 3

</pre>

 */
public class SampleManyTasksEffector extends AddEffector {

    public static final ConfigKey<Integer> RANDOM_SEED = ConfigKeys.newIntegerConfigKey("random.seed");

    public SampleManyTasksEffector(ConfigBag params) {
        super(Effectors.effector(String.class, params.get(EFFECTOR_NAME)).name("eatand").impl(body(params)).build());
    }

    public Effector<?> getEffector() {
        return effector;
    }
    
    private static EffectorTaskFactory<String> body(ConfigBag params) {
        Integer seed = params.get(RANDOM_SEED);
        final Random random = seed!=null ? new Random(seed) : new Random();
        
        // NOTE: not nicely serializable
        return new EffectorTaskFactory<String>() {
            @Override
            public TaskAdaptable<String> newTask(Entity entity, Effector<String> effector, ConfigBag parameters) {
                return Tasks.<String>builder().displayName("eat-sleep-rave-repeat").addAll(tasks(0)).build();
            }
            List<Task<Object>> tasks(final int depth) {
                List<Task<Object>> result = MutableList.of();
                do {
                    TaskBuilder<Object> t = Tasks.builder();
                    double x = random.nextDouble();
                    if (depth>4) x *= random.nextDouble();
                    if (depth>6) x *= random.nextDouble();
                    if (x<0.3) {
                        t.displayName("eat").body(new Callable<Object>() { public Object call() { return "eat"; }});
                    } else if (x<0.6) {
                        final Duration time = Duration.millis(Math.round(10*1000*random.nextDouble()*random.nextDouble()*random.nextDouble()*random.nextDouble()*random.nextDouble()));
                        t.displayName("sleep").description("Sleeping "+time).body(new Callable<Object>() { public Object call() {
                            Tasks.setBlockingDetails("sleeping "+time);
                            Time.sleep(time);
                            return "slept "+time;
                        }});
                    } else if (x<0.8) {
                        t.displayName("rave").body(new Callable<Object>() { public Object call() {
                            List<Task<Object>> ts = tasks(depth+1);
                            for (Task<Object> tt: ts) {
                                DynamicTasks.queue(tt);
                            }
                            return "raved with "+ts.size()+" tasks";
                        }});
                    } else {
                        t.displayName("repeat").addAll(tasks(depth+1));
                    }
                    result.add(t.build());
                    
                } while (random.nextDouble()<0.8);
                return result;
            }
        };
    }

}
