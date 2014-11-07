##About

ACP is Attribyte's open-source JDBC connection pool designed to support the high throughput, concurrency, tuning,
monitoring and reporting typically required to support production application servers.
It provides logical (JDBC) database connections to an application from
a pool of physical connections maintained by the pool.
Connection pools are composed of segments that are used, activated, and deactivated in sequence in
response to connection demand. When active, a segment provides logical connections
from a fixed-size pool of physical connections. Connection pools may be created and configured
programmatically, from Java properties, or [HOCON](https://github.com/typesafehub/config#using-hocon-the-json-superset)
format

##Building

The build uses [Apache Ant](http://ant.apache.org/) and
[Apache Ivy](https://ant.apache.org/ivy/) to resolve dependencies. The following ant tasks
are available:

* compile - Compiles the source
* dist - Resolves dependencies, compiles the source, and creates a jar in dist/lib. This is the default task.
* full-dist - Resolves dependencies, compiles the source, creates a jar in dist/lib, and copies dependencies to dist/extlib
* clean - Removes all build files and jars.

##Dependencies

* [Attribyte shared-base](https://github.com/attribyte/shared-base)
* [Apache commons-codec](http://commons.apache.org/proper/commons-codec/)
* [Google Guava](https://code.google.com/p/guava-libraries/)
* [Dropwizard Metrics](http://metrics.codahale.com/)
* [Typesafe Config](https://github.com/typesafehub/config)


##License

Copyright 2014 [Attribyte, LLC](https://attribyte.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
