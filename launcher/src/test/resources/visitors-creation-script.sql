--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--
create database visitors;
use visitors;


# Create two users: 'brooklyn'@'%' and 'brooklyn'@'localhost' with full access
# default password specified for compatibility with older blueprints that don't provide config;
# if your blueprint sets the password it can be removed here
grant usage on *.* to 'brooklyn'@'%' identified by '${config["creation.script.password"]!"br00k11n"}';
# useful if sockets work also
grant all privileges on *.* to 'brooklyn'@'localhost' identified by '${config["creation.script.password"]!"br00k11n"}';

# drop the anonymous users eg ''@'localhost' and sometimes other local hostnames
# (necessary for same-host access as these are more restrictive rules that trump the wildcard above)
delete from mysql.user where User='';

flush privileges;


# and now the table for visitors

CREATE TABLE MESSAGES (
        id BIGINT NOT NULL AUTO_INCREMENT,
        NAME VARCHAR(30) NOT NULL,
        MESSAGE VARCHAR(400) NOT NULL,
        PRIMARY KEY (ID)
    );

INSERT INTO MESSAGES values (default, 'Isaac Asimov', 'I grew up in Brooklyn' );
