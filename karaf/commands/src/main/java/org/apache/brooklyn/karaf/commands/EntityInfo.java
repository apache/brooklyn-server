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
package org.apache.brooklyn.karaf.commands;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.karaf.commands.completers.EntityIdCompleter;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Completion;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.ansi.SimpleAnsi;
import org.apache.karaf.shell.support.table.ShellTable;

import java.util.Collections;
import java.util.Optional;

@Command(scope = "brooklyn", name = "entity-info", description = "Display entity info")
@Service
public class EntityInfo implements Action {

    @Reference
    private ManagementContext managementContext;

    @Option(name = "-a", aliases = {"--all"}, description = "Display all information")
    private Boolean displayAll = false;

    @Option(name = "-c", aliases = {"--children"}, description = "Display child information")
    private Boolean displayChildren = false;

    @Option(name = "-s", aliases = {"--sensors"}, description = "Display sensor information")
    private Boolean displaySensors = false;

    @Option(name = "--cfg", aliases = {"--config"}, description = "Display config information")
    private Boolean displayConfig = false;

    @Option(name = "--blueprint", description = "Display blueprint")
    private Boolean displayBlueprint = false;

    @Argument(index = 0, name = "id", description = "The entity id", required = true)
    @Completion(EntityIdCompleter.class)
    private String id = null;

    @Override
    public Object execute() throws Exception {
        Optional<Entity> entity = Optional.ofNullable(managementContext.getEntityManager().getEntity(id));
        if(!entity.isPresent()){
            System.err.println(String.format("Entity [%s] not found", id));
            return null;
        }
        printHeader("Basic Information");
        final ShellTable infoTable = new ShellTable();
        infoTable.column("").bold();
        infoTable.column("");
        infoTable.noHeaders();
        infoTable.addRow().addContent("Application Id", entity.get().getApplicationId());
        infoTable.addRow().addContent("Application Name", entity.get().getApplication().getDisplayName());
        infoTable.addRow().addContent("Name", entity.get().getDisplayName());
        infoTable.addRow().addContent("Id", id);
        infoTable.addRow().addContent("UP", entity.get().sensors().get(Attributes.SERVICE_UP));
        infoTable.addRow().addContent("State", entity.get().sensors().get(Attributes.SERVICE_STATE_ACTUAL));
        infoTable.addRow().addContent("Expected State", entity.get().sensors().get(Attributes.SERVICE_STATE_EXPECTED));
        infoTable.print(System.out, true);
        if (displayChildren || displayAll) {
            printHeader("Child Information");
            final ShellTable childrenTable = new ShellTable();
            childrenTable.column("id");
            childrenTable.column("name");

            entity.get().getChildren().forEach(child -> childrenTable.addRow().addContent(child.getId(), child.getDisplayName()));

            childrenTable.print(System.out, true);
        }
        if (displaySensors || displayAll) {
            printHeader("Sensor Information");
            final ShellTable sensorTable = new ShellTable();
            sensorTable.column("name");
            sensorTable.column("value");
            sensorTable.column("description");

            entity.get().getEntityType().getSensors().stream()
                    .filter(AttributeSensor.class::isInstance).map(AttributeSensor.class::cast)
                    .forEach(sensor -> sensorTable.addRow().addContent(sensor.getName(), entity.get().getAttribute(sensor), sensor.getDescription()));

            sensorTable.print(System.out, true);
        }

        if (displayConfig || displayAll) {
            printHeader("Config Information");
            final ShellTable configTable = new ShellTable();
            configTable.column("name");
            configTable.column("value");
            configTable.column("description");

            entity.get().getEntityType().getConfigKeys().stream()
                    .forEach(configKey -> configTable.addRow().addContent(configKey.getName(), entity.get().getConfig(configKey), configKey.getDescription()));
            configTable.print(System.out, true);
        }

        if (displayBlueprint || displayAll) {
            final Optional<String> bluePrint = Optional.ofNullable(BrooklynTags.findFirst(BrooklynTags.YAML_SPEC_KIND, entity.get().tags().getTags()))
                    .map(BrooklynTags.NamedStringTag::getContents);
            if (bluePrint.isPresent()) {
                printHeader("Blueprint Information");
                System.out.println("---");
                System.out.print(bluePrint.get());
                System.out.println("...");
            }
        }

        System.out.println();
        return null;
    }

    private void printHeader(String header) {
        System.out.println("\n" + SimpleAnsi.COLOR_CYAN + SimpleAnsi.INTENSITY_BOLD + header + SimpleAnsi.INTENSITY_NORMAL + SimpleAnsi.COLOR_DEFAULT);
        System.out.println(SimpleAnsi.INTENSITY_BOLD + String.join("", Collections.nCopies(header.length(), "-")) + SimpleAnsi.INTENSITY_NORMAL);
    }
}
