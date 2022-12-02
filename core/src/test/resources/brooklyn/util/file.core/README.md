[//]: # (    Licensed to the Apache Software Foundation &#40;ASF&#41; under one)
[//]: # (    or more contributor license agreements.  See the NOTICE file)
[//]: # (    distributed with this work for additional information)
[//]: # (    regarding copyright ownership.  The ASF licenses this file)
[//]: # (    to you under the Apache License, Version 2.0 &#40;the)
[//]: # (    "License"&#41;; you may not use this file except in compliance)
[//]: # (    with the License.  You may obtain a copy of the License at)
[//]: # ()
[//]: # (     http://www.apache.org/licenses/LICENSE-2.0)
[//]: # ()
[//]: # (    Unless required by applicable law or agreed to in writing,)
[//]: # (    software distributed under the License is distributed on an)
[//]: # (    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY)
[//]: # (    KIND, either express or implied.  See the License for the)
[//]: # (    specific language governing permissions and limitations)
[//]: # (    under the License.)

# Evil zip files objective

The `evil*.zip` files in this directory contain a script, and they have been creating using a [pen test tool](https://github.com/jcabrerizo/evilarc) 
that allows prefixing the name of the file, the script in this example, with a number of `../` making the default 
extraction of them a risk for the underlying operating system, allowing to put any file in any path, or replacing 
exiting binary files, exposing the host to a remote command execution.

This vulnerability is named *Zip Slip*, you can find more information in here: https://security.snyk.io/research/zip-slip-vulnerability
