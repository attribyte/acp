## About

ACP is Attribyte's open-source JDBC connection pool. It is designed to support the high throughput, concurrency, tuning,
monitoring and reporting typically required to support production application servers.
Connection pools are composed of segments that are used, activated, and deactivated in sequence in
response to connection demand. When active, a segment provides logical connections
from a fixed-size pool of physical connections.


## History

ACP was originally created in 2010. I was motivated to create ACP
because, at the time, I couldn't find a pool that was instrumented the way I needed, or capable of being carefully tuned to
supply "backpressure" when flooded with connection requests.
(The best candidate, [BoneCP](https://github.com/wwadge/bonecp),
was just being developed. [HikariCP](https://github.com/brettwooldridge/HikariCP) was quite new and is, today, pretty much the default connection pool.)
For several years it reliably provided connections for Gawker Media's publishing system.

## Performance
Here's the result of a "quick" run of the HikariCP benchmark. Not bad. The connection pool has never been a bottleneck
in any Attribyte system, so I have spent little time trying to optimize ACP. Note that pool instrumentation
is always enabled (timing, metering of connection acquisitions, etc.). Turning this off might improve the score,
but I've never experienced a situation where (possibly) better connection pool performance was more important than
pool monitoring.
<pre><code><small>
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

</small></code></pre>


## Documentation

[Javadoc](https://attribyte.github.io/acp/apidocs/)

## Configuration

Connection pools may be created and configured programmatically, from Java properties,
or [HOCON](https://github.com/typesafehub/config#using-hocon-the-json-superset) format.
All configuration parameters are documented below. A
[sample HOCON config](https://github.com/attribyte/acp/blob/master/doc/config/sample.config) and
[sample properties file](https://github.com/attribyte/acp/blob/master/doc/config/sample.properties) are included.

Defaults are provided by `reference.conf` in the distributed jar. Parameters marked *Required*
must be supplied in your configuration.

### Connection Parameters

<dl>
 <dt>user</dt>
 <dd><em>Required.</em> The database user.</dd>
 <dt>password</dt>
 <dd><em>Required.</em> The database password.</dd>
 <dt>connectionString</dt>
 <dd><em>Required.</em> The JDBC connection string.</dd>
 <dt>testSQL</dt>
 <dd>SQL statement used to test connections. If empty, connections are not tested periodically.</dd>
 <dt>testInterval</dt>
 <dd>The interval between periodic connection tests. Default <tt>60s</tt>.</dd>
 <dt>createTimeout</dt>
 <dd>The maximum time to wait when creating a new physical connection. Default <tt>60s</tt>.</dd>
 <dt>debug</dt>
 <dd>When <tt>true</tt>, the call site of connection acquisitions is recorded and available for exceptions. Default <tt>false</tt>.</dd>
 <dt>properties</dt>
 <dd>A reference to a named set of global properties that are appended to the connection string as query parameters.</dd>
</dl>

### Pool Parameters

<dl>
 <dt>minActiveSegments</dt>
 <dd>The minimum number of segments that must remain active. Default <tt>1</tt>.</dd>
 <dt>startActiveSegments</dt>
 <dd>The number of segments activated on pool startup. Default <tt>1</tt>.</dd>
 <dt>saturatedAcquireTimeout</dt>
 <dd><em>Required.</em> The maximum time to block waiting for an available connection when all active segments are saturated. A value of <tt>0</tt> means no waiting.</dd>
 <dt>idleCheckInterval</dt>
 <dd>The interval between checks for idle segments that can be deactivated. Default <tt>30s</tt>.</dd>
</dl>

### Segment Parameters

Segments are defined either as a list under a `segments` key or as named objects within the pool configuration.
A segment can `clone` another segment, inheriting all of its properties with overrides.

<dl>
 <dt>name</dt>
 <dd><em>Required.</em> The segment name.</dd>
 <dt>size</dt>
 <dd><em>Required.</em> The number of physical connections in this segment.</dd>
 <dt>connectionName</dt>
 <dd><em>Required.</em> A reference to a named connection configuration.</dd>
 <dt>clone</dt>
 <dd>The name of another segment to copy configuration from. Properties specified alongside <tt>clone</tt> override the cloned values.</dd>
 <dt>acquireTimeout</dt>
 <dd><em>Required.</em> The maximum time to wait for a connection from this segment before trying the next segment.</dd>
 <dt>activeTimeout</dt>
 <dd>The maximum time a logical connection may be held open before the activity timeout policy is applied. Default <tt>60s</tt>.</dd>
 <dt>connectionLifetime</dt>
 <dd>The maximum lifetime of a physical connection before it is closed and replaced. Default <tt>15m</tt>.</dd>
 <dt>idleTimeBeforeShutdown</dt>
 <dd>The time a segment must be idle before it is eligible for deactivation. Default <tt>30s</tt>.</dd>
 <dt>minActiveTime</dt>
 <dd>The minimum time a segment must be active before it is eligible for deactivation. Default <tt>30s</tt>.</dd>
 <dt>closeConcurrency</dt>
 <dd>The number of background threads used to process connection close. If <tt>0</tt>,
 close blocks in the application thread. Default <tt>0</tt>.</dd>
 <dt>reconnectConcurrency</dt>
 <dd>The maximum number of concurrent physical reconnect attempts. Default <tt>2</tt>.</dd>
 <dt>reconnectMaxWaitTime</dt>
 <dd>The maximum delay between reconnect attempts after a connection failure. Uses exponential backoff up to this limit. Default <tt>1m</tt>.</dd>
 <dt>testOnLogicalOpen</dt>
 <dd>Test connections when they are acquired from the pool. Default <tt>false</tt>.</dd>
 <dt>testOnLogicalClose</dt>
 <dd>Test connections when they are returned to the pool. Default <tt>false</tt>.</dd>
 <dt>closeTimeLimit</dt>
 <dd>The maximum time to wait for a connection close operation to complete. Default <tt>10s</tt>.</dd>
 <dt>activeTimeoutMonitorFrequency</dt>
 <dd>The frequency at which active connections are checked for timeout. Default <tt>30s</tt>.</dd>
</dl>

### Policy Parameters

These control how the pool handles various edge cases. All are segment-level settings.

<dl>
 <dt>incompleteTransactionPolicy</dt>
 <dd>How to handle a connection returned with an incomplete transaction.
 Values: <tt>report</tt> (log a warning), <tt>commit</tt>, <tt>rollback</tt>. Default <tt>report</tt>.</dd>
 <dt>openStatementPolicy</dt>
 <dd>How to handle a connection returned with open statements.
 Values: <tt>none</tt> (don't track), <tt>silent</tt> (close silently), <tt>report</tt> (close and log). Default <tt>report</tt>.</dd>
 <dt>forceRealClosePolicy</dt>
 <dd>What to close when a physical connection is forcibly terminated.
 Values: <tt>none</tt>, <tt>connection</tt>, <tt>connectionWithLimit</tt> (close with time limit), <tt>statementsAndConnection</tt>. Default <tt>connectionWithLimit</tt>.</dd>
 <dt>activityTimeoutPolicy</dt>
 <dd>How to handle a connection that exceeds its active timeout.
 Values: <tt>log</tt> (log a warning), <tt>forceClose</tt> (forcibly close the connection). Default <tt>log</tt>.</dd>
</dl>

## Dependencies

* [Attribyte shared-base](https://github.com/attribyte/shared-base)
* [Google Guava](https://github.com/google/guava)
* [Dropwizard Metrics](https://metrics.dropwizard.io/)
* [Essem Metrics](https://github.com/attribyte/essem-reporter)
* [Typesafe Config](https://github.com/lightbend/config)


## License

Copyright 2010-2026 [Attribyte Labs, LLC](https://attribyte.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
