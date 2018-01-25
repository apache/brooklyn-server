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
package org.apache.brooklyn.core.sensor.password;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddSensor;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Identifiers;

public class CreatePasswordSensor extends AddSensor<String> {

    public static final ConfigKey<Integer> PASSWORD_LENGTH = ConfigKeys.newIntegerConfigKey("password.length", "The length of the password to be created", 12);

    public static final ConfigKey<String> ACCEPTABLE_CHARS = ConfigKeys.newStringConfigKey("password.chars", "The characters allowed in password");

    public static final ConfigKey<List<String>> CHARACTER_GROUPS = ConfigKeys.newConfigKey(new TypeToken<List<String>>() {}, "password.character.groups", "A list of strings, where each string is a character group (such as letters, or numbers). The password will be constructed using only characters from these strings, and will use at least one character from each group. When using this option, `password.length` must be at least as long as the number of character groups given.");

    private Integer passwordLength;
    private String acceptableChars;
    private List<String> characterGroups;

    public CreatePasswordSensor(Map<String, String> params) {
        this(ConfigBag.newInstance(params));
    }

    public CreatePasswordSensor(ConfigBag params) {
        super(params);
        passwordLength = params.get(PASSWORD_LENGTH);
        acceptableChars = params.get(ACCEPTABLE_CHARS);
        characterGroups = params.get(CHARACTER_GROUPS);
    }

    @Override
    public void apply(EntityLocal entity) {
        super.apply(entity);

        boolean isCharacterGroupsPresent = characterGroups != null
                && characterGroups.size() > 0;
        boolean isCharacterGroupsValid = isCharacterGroupsPresent
                && !Iterables.contains(characterGroups, Predicates.isNull())
                && !Iterables.contains(characterGroups, Predicates.equalTo(""));
        boolean isAcceptableCharsPresentAndValid = acceptableChars != null
                && !acceptableChars.isEmpty();

        Preconditions.checkArgument(!isCharacterGroupsPresent || isCharacterGroupsValid, "password.character.groups config key was given but does not contain any valid groups");
        Preconditions.checkArgument(!(isCharacterGroupsPresent && isAcceptableCharsPresentAndValid), "password.chars and password.character.groups both provided - please provide only ONE of them");
        Preconditions.checkArgument(!isCharacterGroupsValid || characterGroups.size() <= passwordLength, "password.length must be longer than the number of entries in password.character.groups");

        String password;
        if (isCharacterGroupsValid) {
            password = Identifiers.makeRandomPassword(passwordLength, characterGroups.toArray(new String[0]));
        } else if (isAcceptableCharsPresentAndValid) {
            password = Identifiers.makeRandomPassword(passwordLength, acceptableChars);
        } else {
            password = Identifiers.makeRandomPassword(passwordLength);
        }

        entity.sensors().set(sensor, password);
    }
}
