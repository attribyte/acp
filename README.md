##About

ACP is Attribyte's open-source JDBC connection pool. It is designed to support the high throughput, concurrency, tuning,
monitoring and reporting typically required to support production application servers.
Connection pools are composed of segments that are used, activated, and deactivated in sequence in
response to connection demand. When active, a segment provides logical connections
from a fixed-size pool of physical connections.


##History

ACP was originally created in 2010. I was motivated to create ACP
because, at the time, I couldn't find a pool that was instrumented the way I needed, or capable of being carefully tuned to
supply "backpressure" when flooded with connection requests.
(The best candidate, [BoneCP](https://github.com/wwadge/bonecp),
was just being developed. [HikariCP](https://github.com/brettwooldridge/HikariCP) is quite new.)
For several years it reliably provided connections for Gawker Media's publishing system.
It was migrated from Bitbucket in 2014. While moving it to Github, a few things have been tweaked:

* Now requires Java 7 or above (because <code>LinkedTransferQueue</code> is used internally).
* Added more control of close behavior when "activity timeout" is configured.
* Added ability to disable statement tracking.
* Removed XML/DOM-based configuration in favor of HOCON (or <code>Properties</code>).
* Simplified programmatic configuration.
* Better metrics.

##Performance
Here's the result of a "quick" run of the HikariCP benchmark. Not bad. The connection pool has never been a bottleneck
in any Attribyte system, so I have spent little time trying to optimize ACP. Note that pool instrumentation
is always enabled (timing, metring of connection acquisitions, etc.). Turning this off might improve the score,
but I've never experienced a situation where (possibly) better connection pool performance was more important than
pool monitoring.
<pre><code>
# Run complete. Total time: 00:09:32

Benchmark                                 (maxPoolSize)  (pool)   Mode  Samples       Score  Score error   Units
c.z.h.b.ConnectionBench.cycleCnnection               32  hikari  thrpt       16   10928.401      165.642  ops/ms
c.z.h.b.ConnectionBench.cycleCnnection               32    bone  thrpt       16    5797.225       64.904  ops/ms
c.z.h.b.ConnectionBench.cycleCnnection               32  tomcat  thrpt       16    1498.525      119.418  ops/ms
c.z.h.b.ConnectionBench.cycleCnnection               32    c3p0  thrpt       16      82.673        3.397  ops/ms
c.z.h.b.ConnectionBench.cycleCnnection               32   vibur  thrpt       16    4971.874       70.311  ops/ms
c.z.h.b.ConnectionBench.cycleCnnection               32     acp  thrpt       16    3189.405      101.285  ops/ms
c.z.h.b.StatementBench.cycleStatement                32  hikari  thrpt       16   73408.710     4079.799  ops/ms
c.z.h.b.StatementBench.cycleStatement                32    bone  thrpt        8   11467.816      436.378  ops/ms
c.z.h.b.StatementBench.cycleStatement                32  tomcat  thrpt       16   25277.900      495.015  ops/ms
c.z.h.b.StatementBench.cycleStatement                32    c3p0  thrpt       16    6696.266       54.554  ops/ms
c.z.h.b.StatementBench.cycleStatement                32   vibur  thrpt       16   13799.012      196.682  ops/ms
c.z.h.b.StatementBench.cycleStatement                32     acp  thrpt       16  237927.492     4201.623  ops/ms

processor	: 0
vendor_id	: GenuineIntel
cpu family	: 6
model		: 58
model name	: Intel(R) Core(TM) i7-3517U CPU @ 1.90GHz
stepping	: 9
microcode	: 0x15
cpu MHz		: 800.000
cache size	: 4096 KB
physical id	: 0
siblings	: 4
core id		: 0
cpu cores	: 2
apicid		: 0
initial apicid	: 0
fpu		: yes
fpu_exception	: yes
cpuid level	: 13
wp		: yes
flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx rdtscp lm constant_tsc arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc aperfmperf eagerfpu pni pclmulqdq dtes64 monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr pdcm pcid sse4_1 sse4_2 x2apic popcnt tsc_deadline_timer aes xsave avx f16c rdrand lahf_lm ida arat epb xsaveopt pln pts dtherm tpr_shadow vnmi flexpriority ept vpid fsgsbase smep erms
bogomips	: 4789.13
clflush size	: 64
cache_alignment	: 64
address sizes	: 36 bits physical, 48 bits virtual
power management:

</code></pre>


##Documentation

* [Javadoc](https://www.attribyte.org/projects/acp/javadoc/index.html)

##Configuration

Connection pools may be created and configured programmatically, from Java properties,
or [HOCON](https://github.com/typesafehub/config#using-hocon-the-json-superset) format.
The most common configuration settings are documented below and a
[sample properties file](https://github.com/attribyte/acp/blob/master/doc/config/sample.properties) is included.


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
