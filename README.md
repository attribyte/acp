##About

ACP is Attribyte's open-source JDBC connection pool. It is designed to support the high throughput, concurrency, tuning,
monitoring and reporting typically required to support production application servers.
The connection pool provides logical database connections to an application from
a pool of physical connections maintained by the pool.
Connection pools are composed of segments that are used, activated, and deactivated in sequence in
response to connection demand. When active, a segment provides logical connections
from a fixed-size pool of physical connections. Connection pools may be created and configured
programmatically, from Java properties, or [HOCON](https://github.com/typesafehub/config#using-hocon-the-json-superset)
format

##Configuration

<dl>
 <dt>name</dt>
 <dd>The pool name. Required.</dd>
 <dt>minActiveSegments</dt>
 <dd>The minimum number of active segments. Default <tt>1</tt>.</dd>
 <dt>saturatedAcquireTimeout</dt>
 <dd>The maximum amount of time to block waiting for an available connection. Default <tt>0ms</tt>.</dd>
 <dt>idleCheckInterval</dt>
 <dd>The time between idle segment checks. Default <tt>60s</tt>.</dd>
 <dt>segments</dt>
 <dd>The number of segments. Default <tt>1</tt>.</dd>
 <dt>activeSegments</dt>
 <dd>The number of segments active on start. Default <tt>1</tt>.</dd>
 <dt>connection.user</dt>
 <dd>The database user.</dd>
 <dt>connection.password</dt>
 <dd>The database password.<dd>
 <dt>connection.url</dt>
 <dd>The database connection string.</dd>
 <dt>connection.testSQL</dt>
 <dd>SQL used for connection tests.</dd>
 <dt>connection.debug</dt>
 <dd>Is connection debug mode turned on?</dd>
 <dt>connection.testInterval</dt>
 <dd>The interval between connection tests. Default <tt>60s</tt>.</dd>
 <dt>connection.maxWait</dt>
 <dd>The maximum amount of time to wait for a database connection before giving up. Default <tt>0s</tt>.</dd>
 <dt>segment.size</dt>
 <dd>The number of connections in each segment. Required.</dd>
 <dt>segment.closeConcurrency</dt>
 <dd>The number of background threads processing connection close. If <tt>0</tt>,
 close blocks in the application thread. Default <tt>0</tt>.</dd>
 <dt>segment.reconnectConcurrency</dt>
 <dd>The maximum number of concurrent database reconnects. Default <tt>1</tt>.</dd>
 <dt>segment.testOnLogicalOpen</dt>
 <dd>Should connections be tested when they are acquired? Default <tt>false</tt>.</dd>
 <dt>segment.testOnLogicalClose</dt>
 <dd>Should connections be tested when they are released? Default <tt>false</tt>.</dd>
 <dt>segment.acquireTimeout</dt>
 <dd>The maximum amount of time to wait for a segment connection to become available. Default <tt>0ms</tt>.</dd>
 <dt>segment.activeTimeout</dt>
 <dd>The maximum amount of time a logical connection may be open before it is forcibly closed. Default <tt>5m</tt>.</dd>
 <dt>segment.connectionLifetime</dt>
 <dd>The maximum amount of time a physical connection may be open. Default <tt>1h</tt>.</dd>
 <dt>segment.maxReconnectWait</dt>
 <dd>The maximum amount of time to wait between physical connection attempts on failure. Default <tt>30s</tt>.</dd>
</dl>

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
