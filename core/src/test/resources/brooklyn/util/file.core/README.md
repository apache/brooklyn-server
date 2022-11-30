# Evil zip files objective

The `evil*.zip` files in this directory contain a script, and they have been creating using a [pen test tool](https://github.com/jcabrerizo/evilarc) 
that allows prefixing the name of the file, the script in this example, with a number of `../` making the default 
extraction of them a risk for the underlying operating system, allowing to put any file in any path, or replacing 
exiting binary files, exposing the host to a remote command execution.

This vulnerability is named *Zip Slip*, you can find more information in here: https://security.snyk.io/research/zip-slip-vulnerability
