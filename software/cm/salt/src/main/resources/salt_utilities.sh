#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


function edit_pillar_roots () {
    cat <<EOI
1
/^#.*srv.pillar
a
base:
  - /srv/pillar

.
wq
EOI
}

function init_pillar_top () {
cat > /srv/pillar/top.sls <<EOI
base:
  '*':
EOI
}

function add_pillar_to_top () {
    local pillar=$1
    { cat /srv/pillar/top.sls ; echo "    - $pillar" ; } > /tmp/top.sls
    mv /tmp/top.sls /srv/pillar/top.sls
}

function init_pillar_config () {
    mkdir -p /srv/pillar
    edit_pillar_roots | ed /etc/salt/minion
    init_pillar_top
}

function set_minion_id () {
    sed "s/^#id:/id: $1/" /etc/salt/minion > /tmp/minion
    mv /tmp/minion /etc/salt/minion
}

function salt_installed () {
    [ -f install_salt.sh ]
}

function state_exists () {
  salt-call --local state.sls test=True $1 2>/dev/null | grep Succeeded >/dev/null 2>&1
}

function verify_states () {
    echo verifying states "$@"
    for state in "$@" ; do
      state_exists $state || { >&2 echo state $state not found ; exit 1 ; }
    done
}

function find_states () {
    for state in "$@" ; do
      if state_exists $state ; then
        echo $state
      fi
    done
}

