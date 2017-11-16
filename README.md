
# [![**Brooklyn**](https://brooklyn.apache.org/style/img/apache-brooklyn-logo-244px-wide.png)](http://brooklyn.apache.org/)

### Apache Brooklyn Server Sub-Project

This repo contains the core elements to run a Brooklyn server,
from the API and utils through to the core implementation and the REST server.

### Building the project

2 methods are available to build this project: within a docker container or directly with maven.

#### Using docker

The project comes with a `Dockerfile` that contains everything you need to build this project.
First, build the docker image:

```bash
docker build -t brooklyn:server .
```

Then run the build:

```bash
docker run -i --rm --name brooklyn-server -v ${HOME}/.m2:/root/.m2 -v ${PWD}:/usr/build -w /usr/build brooklyn:server mvn clean install
```

### Using maven

Simply run:

```bash
mvn clean install
```