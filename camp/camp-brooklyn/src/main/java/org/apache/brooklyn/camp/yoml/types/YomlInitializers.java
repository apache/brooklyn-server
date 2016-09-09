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
package org.apache.brooklyn.camp.yoml.types;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.yoml.BrooklynYomlTypeRegistry;
import org.apache.brooklyn.core.BrooklynVersion;
import org.apache.brooklyn.core.effector.ssh.SshCommandEffector;
import org.apache.brooklyn.core.sensor.ssh.SshCommandSensor;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.util.yoml.YomlSerializer;

import com.google.common.annotations.Beta;

/*

TYPES - effector
- script / bash / ssh
- invoke-effector
- publish-sensor
- initdish-effector
- parallel
- sequence

     - ssh
     - install-file
     - set-sensor
     - set-config


SEQ
* initd
* specify pre-conditions & post-conditions
* specify what must run before/after

a
b after a
c before b

EXTENSIONS:
* local task parameters -- could pass params to any subtask
* acquire-semaphore (cancel holder, timeout

* parallel
* run over entities -- concurrency:  16
* conditional
* jumping?
* set sensors/config
* access results of previous tasks


STYLE NOTES

  brooklyn.initializers:
  - type: org.apache.brooklyn.core.effector.ssh.SshCommandEffector
    brooklyn.config:
      name: sayHiNetcat
      description: Echo a small hello string to the netcat entity
      command: |
        echo $message | nc $TARGET_HOSTNAME 4321
      parameters:
        message:
          description: The string to pass to netcat
          defaultValue: hi netcat

effectors:
  say-hi:
    type: script
    env:
      name: $brooklyn:config("name")
      name: $brooklyn:${name}
    script:
      echo hello ${name:-world}

  publish-name:
    type: publish-sensor
    sensor: name
    value: $brooklyn:formatString("%s (%s)", config("name"), $("baz"))
    parameters:
      foo: # nothing
      bar: { description: The bar, default-value: B }
      baz: Z  # default

  start:
    type: initdish-effector
    impl:
            8.2-something:
              type: invoke-effector
              effector: say-hi
              parameters:
                name: Bob
            8.2-something:
              invoke-effector: say-hi
            8.2-something:
              invoke-effector:
                  effector: say-hi
                  parameters:
                    name: Bob
            8.2-something:
              "saying hi":
                  type: invoke-effector
                  effector: say-hi
                  parameters:
                    name: Bob


COMPARISON TO OLD

we used to say:

  brooklyn.initializers:
  - type: org.apache.brooklyn.core.effector.ssh.SshCommandEffector
    brooklyn.config:
      name: sayHiNetcat
      description: Echo a small hello string to the netcat entity
      command: |
        echo $message | nc $TARGET_HOSTNAME 4321
      parameters:
        message:
          description: The string to pass to netcat
          defaultValue: hi netcat
  - type: org.apache.brooklyn.core.sensor.ssh.SshCommandSensor
    brooklyn.config:
      name: output.last
      period: 1s
      command: tail -1 server-input

now we say:

  brooklyn.initializers:
    say-hi-netcat:
      type: ssh-effector
      description: Echo a small hello string to the netcat entity
      script: |
        echo $message | nc $TARGET_HOSTNAME 4321
      parameters:
        message:
          description: The string to pass to netcat
          default-value: hi netcat
    output.last:
      type: ssh-sensor
      period: 1s
      command: tail -1 server-input
      
benefits:
  - readable: more concise description and supports aliases, maps (which plays nice with merge)
  - extensible: *much* easier to define new items, including recursive
  - introspective: auto-generate docs and support code completion with descriptions
  - bi-directional: we can persist in the same formal



OTHER THOUGHTS

effectors:
 -  type: CommandSequenceEffector
    name: start
    impl:                                      
    - sequence_identifier: 0-provision  [optional]
      type: my.ProvisionEffectorTaskFactory
    - seq_id: 1-install
      type: script
      script: |
        echo foo ${entity.config.foo}

                    
            01-install:
                parallel:
                 -  bash: |
                    echo foo ${entity.config.foo}
                 -  copy:
                        from: URL
                        to: ${entity.sensor['run.dir']}/file.txt
            02-loop-entities:
                foreach:
                    expression: $brooklyn:component['x'].descendants
                    var: x
                    command:
                        effector:
                            name: restart
                            parameters:
                                restart_machine: auto
            03-condition:
                conditional:
                    if: $x
                    then:
                        bash: echo yup
                    else:
                    - if: $y
                      then:
                         bash: echo y
                    - bash: echo none
            04-jump:
                goto: 02-install

           
                   


catalog:
  id: my-basic-1
services:
- type: vanilla-software-process
  effectors:
    start:
      0-provision: get-machine
      1-install:
        - copy:
            from:  classpath://foo.tar
            to: /tmp/foo/foo.tar
        - bash:
          - cd /tmp/foo
          - unzip foo.tar
        - bash: foo.sh
      2-launch:
          - cd /tmp/foo ; ./foo.sh
     policy:
     - type: Hook1

    triggers:
    - on: $brooklyn:component("db").sensor("database_url")
       do:
        - bash:
            mysql install table
        - publish_sensor basic-ready true

----

services:
- type: my-basic-2
  effectors:
    start:
      1.5.11-post-install:
         bash: |
echo custom step

      1.7-another-post-install:
      
 */
public class YomlInitializers {
    
    /** 
     * Adds the given type to the registry. 
     * As the registry is not yet persisted this method must be called explicitly to initialize any management context using it.
     * This method will be deleted when there is a catalog-style init/persistence mechanism. */ 
    @Beta
    public static void addLocalType(ManagementContext mgmt, RegisteredType type) {
        ((BasicBrooklynTypeRegistry) mgmt.getTypeRegistry()).addToLocalUnpersistedTypeRegistry(type, false);
    }
    
    /** As {@link #addLocalType(ManagementContext, RegisteredType)} */ @Beta
    public static void addLocalBean(ManagementContext mgmt, Class<?> clazz) {
        addLocalType(mgmt, BrooklynYomlTypeRegistry.newYomlRegisteredType(RegisteredTypeKind.BEAN, null, BrooklynVersion.get(), clazz));
    }
    
    /** As {@link #addLocalType(ManagementContext, RegisteredType)} */ @Beta
    public static void addLocalBean(ManagementContext mgmt, String symbolicName, String planYaml,
            Class<?> javaConcreteType,
            Iterable<? extends Object> addlSuperTypesAsClassOrRegisteredType,
            Iterable<YomlSerializer> serializers) {
        addLocalType(mgmt, BrooklynYomlTypeRegistry.newYomlRegisteredType(RegisteredTypeKind.BEAN, null, BrooklynVersion.get(), 
            planYaml, javaConcreteType, addlSuperTypesAsClassOrRegisteredType, serializers));
    }
    
    public static void install(ManagementContext mgmt) {
        addLocalBean(mgmt, SshCommandEffector.class);
        addLocalBean(mgmt, SshCommandSensor.class);
    }
    
}
