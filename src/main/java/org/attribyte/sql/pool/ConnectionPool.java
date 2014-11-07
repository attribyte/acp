/*
 * Copyright 2010,2012 Attribyte, LLC
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License.  
 * 
 */

package org.attribyte.sql.pool;

import java.io.InputStream;
import java.io.IOException;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.attribyte.api.InitializationException;
import org.attribyte.api.Logger;
import org.attribyte.sql.ConnectionSupplier;
import org.attribyte.util.DOMUtil;
import org.attribyte.util.InitUtil;
import org.attribyte.util.StringUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

/**
 * Provides logical database connections to an application from
 * a pool of physical connections that are maintained by the pool.
 * <p>
 * Connection pools are composed of segments that are used, activated and deactivated in sequence in
 * response to connection demand. When active, a segment provides logical connections
 * from a fixed-size pool of physical connections.
 * </p>
 * <p>
 * Connection pools may be created using XML or property-based configuration, as well as programmatically using an
 * <tt>Initializer</tt>.
 * </p>
 * <h3>Configuration Parameters</h3>
 * <p>
 * <dl>
 * <dt>name</dt>
 * <dd>The pool name. Required.</dd>
 * <dt>minActiveSegments</dt>
 * <dd>The minimum number of active segments. Default <tt>1</tt>.</dd>
 * <dt>saturatedAcquireTimeout</dt>
 * <dd>The maximum amount of time to block waiting for an available connection. Default <tt>0ms</tt>.</dd>
 * <dt>idleCheckInterval</dt>
 * <dd>The time between idle segment checks. Default <tt>60s</tt>.</dd>
 * <dt>segments</dt>
 * <dd>The number of segments. Default <tt>1</tt>.</dd>
 * <dt>activeSegments</dt>
 * <dd>The number of segments active on start. Default <tt>1</tt>.</dd>
 * <dt>connection.user</dt>
 * <dd>The database user.</dd>
 * <dt>connection.password</dt>
 * <dd>The database password.<dd>
 * <dt>connection.url</dt>
 * <dd>The database connection string.</dd>
 * <dt>connection.testSQL</dt>
 * <dd>SQL used for connection tests.</dd>
 * <dt>connection.debug</dt>
 * <dd>Is connection debug mode turned on?</dd>
 * <dt>connection.testInterval</dt>
 * <dd>The interval between connection tests. Default <tt>60s</tt>.</dd>
 * <dt>connection.maxWait</dt>
 * <dd>The maximum amount of time to wait for a database connection before giving up. Default <tt>0s</tt>.</dd>
 * <dt>segment.size</dt>
 * <dd>The number of connections in each segment. Required.</dd>
 * <dt>segment.closeConcurrency</dt>
 * <dd>The number of background threads processing connection close. If <tt>0</tt>,
 * close blocks in the application thread. Default <tt>0</tt>.</dd>
 * <dt>segment.reconnectConcurrency</dt>
 * <dd>The maximum number of concurrent database reconnects. Default <tt>1</tt>.</dd>
 * <dt>segment.testOnLogicalOpen</dt>
 * <dd>Should connections be tested when they are acquired? Default <tt>false</tt>.</dd>
 * <dt>segment.testOnLogicalClose</dt>
 * <dd>Should connections be tested when they are released? Default <tt>false</tt>.</dd>
 * <dt>segment.acquireTimeout</dt>
 * <dd>The maximum amount of time to wait for a segment connection to become available. Default <tt>0ms</tt>.</dd>
 * <dt>segment.activeTimeout</dt>
 * <dd>The maximum amount of time a logical connection may be open before it is forcibly closed. Default <tt>5m</tt>.</dd>
 * <dt>segment.connectionLifetime</dt>
 * <dd>The maximum amount of time a physical connection may be open. Default <tt>1h</tt>.</dd>
 * <dt>segment.maxReconnectWait</dt>
 * <dd>The maximum amount of time to wait between physical connection attempts on failure. Default <tt>30s</tt>.</dd>
 * </dl>
 * </p>
 * <h3>Sample XML Configuration File</h3>
 * <p>
 * Segments defined in the <tt>active</tt> element are active on startup.
 * Segments in the <tt>reserve</tt> element are activated as necessary.
 * Cloned segments inherit all properties, with any property overridden.
 * </p>
 * <p>
 * <pre>
 * &lt;acp>
 *
 * &lt;!-- Any JDBC drivers to be loaded -->
 *
 * &lt;drivers>
 * &lt;driver class="com.mysql.jdbc.Driver"/>
 * &lt;/drivers>
 *
 * &lt;logger class="org.attribyte.api.ConsoleLogger"/>
 *
 * &lt;connections>
 *
 * &lt;!-- Properties that may be cloned by all connections (one or more) -->
 *
 * &lt;properties name="std">
 * &lt;property name="useUnicode" value="true"/>
 * &lt;property name="characterEncoding" value="utf8"/>
 * &lt;property name="characterSetResults" value="utf8"/>
 * &lt;property name="zeroDateTimeBehavior" value="convertToNull"/>
 * &lt;/properties>
 *
 * &lt;!-- Connections (one or more) -->
 *
 * &lt;connection name="local" user="apprw" password="secret" connectionString="jdbc:mysql://127.0.0.1/attribyte"
 * testSQL="SELECT 1 FROM test" testInterval="30s" createTimeout="60s">
 * &lt;properties clone="std"/>
 * &lt;/connection>
 * &lt;/connections>
 *
 * &lt;!-- Pools (one or more) -->
 *
 * &lt;pool name="localPool">
 * &lt;active min="1">
 * &lt;segment name="s0" size="5" concurrency="1" testOnLogicalOpen="false" testOnLogicalClose="false">
 * &lt;connection name="local">
 * &lt;acquireTimeout time="5ms"/>
 * &lt;activeTimeout time="45s"/>
 * &lt;lifetime time="15m"/>
 * &lt;/connection>
 * &lt;reconnect concurrency="2" maxWaitTime="1m"/>
 * &lt;/segment>
 * &lt;segment name="s1" clone="s0">
 * &lt;acquireTimeout time="20ms"/>
 * &lt;/segment>
 * &lt;/active>
 * &lt;reserve>
 * &lt;segment name="s2" clone="s0">
 * &lt;acquireTimeout time="50ms"/>
 * &lt;/segment>
 * &lt;segment name="s3" clone="s0" size="20"/>
 * &lt;acquireTimeout time="500ms"/>
 * &lt;/reserve>
 * &lt;idleCheckInterval time="30s"/>
 * &lt;saturatedAcquireTimeout time="1s"/>
 * &lt;/pool>
 * &lt;/acp>
 * </pre>
 * </p>
 * <h3>Sample Properties Configuration File</h3>
 * <p>
 * <pre>
 * driver.mysql.class=com.mysql.jdbc.Driver
 *
 * logger.class=org.attribyte.api.ConsoleLogger
 *
 * property.std.useUnicode=true
 * property.std.characterEncoding=utf8
 *
 * connection.local.user=apprw
 * connection.local.password=secret
 * connection.local.connectionString=jdbc:mysql://127.0.0.1/attribyte
 * connection.local.testSQL=SELECT 1 FROM test
 * connection.local.testInterval=30s
 * connection.local.createTimeout=60s
 * connection.local.debug=true
 * connection.local.properties=std
 *
 * connection.remote.user=apprw
 * connection.remote.password=secret
 * connection.remote.connectionString=jdbc:mysql://127.0.0.1/attribyte
 * connection.remote.testSQL=SELECT 1 FROM test
 * connection.remote.testInterval=30s
 * connection.remote.createTimeout=60s
 * connection.remote.debug=true
 * connection.remote.properties=std
 *
 * pool.localPool.minActiveSegments=1
 * pool.localPool.startActiveSegments=2
 * pool.localPool.idleCheckInterval=30s
 * pool.localPool.saturatedAcquireTimeout=1s
 *
 * pool.localPool.segment0.size=5
 * pool.localPool.segment0.closeConcurrency=2
 * pool.localPool.segment0.testOnLogicalOpen=true
 * pool.localPool.segment0.testOnLogicalClose=true
 * pool.localPool.segment0.connectionName=local
 * pool.localPool.segment0.acquireTimeout=10ms
 * pool.localPool.segment0.activeTimeout=60s
 * pool.localPool.segment0.connectionLifetime=15m
 * pool.localPool.segment0.idleTimeBeforeShutdown=30s
 * pool.localPool.segment0.minActiveTime=30s
 * pool.localPool.segment0.reconnectConcurrency=2
 * pool.localPool.segment0.reconnectMaxWaitTime=1m
 * pool.localPool.segment0.activeTimeoutMonitorFrequency=30s
 *
 * pool.localPool.segment1.clone=segment0
 * pool.localPool.segment1.acquireTimeout=10ms
 * pool.localPool.segment1.size=10
 *
 * pool.localPool.segment2.clone=segment0
 * pool.localPool.segment2.acquireTimeout=50ms
 *
 * pool.remotePool.minActiveSegments=1
 * pool.remotePool.startActiveSegments=2
 * pool.remotePool.idleCheckInterval=30s
 * pool.remotePool.saturatedAcquireTimeout=1s
 *
 * pool.remotePool.segment0.size=5
 * pool.remotePool.segment0.closeConcurrency=2
 * pool.remotePool.segment0.testOnLogicalOpen=true
 * pool.remotePool.segment0.testOnLogicalClose=true
 * pool.remotePool.segment0.connectionName=local
 * pool.remotePool.segment0.acquireTimeout=10ms
 * pool.remotePool.segment0.activeTimeout=60s
 * pool.remotePool.segment0.connectionLifetime=15m
 * pool.remotePool.segment0.idleTimeBeforeShutdown=30s
 * pool.remotePool.segment0.minActiveTime=30s
 * pool.remotePool.segment0.reconnectConcurrency=2
 * pool.remotePool.segment0.reconnectMaxWaitTime=1m
 * pool.remotePool.segment0.activeTimeoutMonitorFrequency=30s
 *
 * pool.remotePool.segment1.clone=segment0
 * pool.remotePool.segment1.acquireTimeout=10ms
 * pool.remotePool.segment1.size=10
 * </pre>
 * </p>
 * @author Matt Hamer - Attribyte, LLC
 */
public class ConnectionPool implements ConnectionSupplier {

   /**
    * Record statistics samples.
    */
   public interface StatsSampler {

      /**
       * Accept a statistics sample.
       * @param stats The statistics.
       */
      public void acceptSample(Stats stats);

      /**
       * Initialize the sampler.
       * @param props The properties.
       * @param logger A logger.
       * @throws InitializationException on initialization error.
       */
      public void init(Properties props, Logger logger) throws InitializationException;

      /**
       * Gets the frequency at which statistics are sampled.
       * @return The sampling frequency.
       */
      public long getFrequencyMillis();
   }

   /**
    * Pool statistics.
    */
   public static class Stats {

      /**
       * Creates stats with all values specified.
       */
      private Stats(final String poolName,
                    final String connectionDescription,
                    final long connectionCount, final long connectionErrorCount, final long activeTimeoutCount,
                    final long failedAcquisitionCount, final long segmentExpansionCount, final int activeSegments,
                    final int activeConnections,
                    final int availableConnections,
                    final int maxConnections,
                    final long activeUnmanagedConnectionCount,
                    final double oneMinuteAcquisitionRate,
                    final double fiveMinuteAcquisitionRate,
                    final double fifteenMinuteAcquisitionRate,
                    final double oneMinuteFailedAcquisitionRate,
                    final double fiveMinuteFailedAcquisitionRate,
                    final double fifteenMinuteFailedAcquisitionRate) {
         this.poolName = poolName;
         this.connectionDescription = connectionDescription;
         this.connectionCount = connectionCount;
         this.connectionErrorCount = connectionErrorCount;
         this.activeTimeoutCount = activeTimeoutCount;
         this.failedAcquisitionCount = failedAcquisitionCount;
         this.segmentExpansionCount = segmentExpansionCount;
         this.activeSegments = activeSegments;
         this.activeConnections = activeConnections;
         this.availableConnections = availableConnections;
         this.maxConnections = maxConnections;
         this.activeUnmanagedConnectionCount = activeUnmanagedConnectionCount;
         this.oneMinuteAcquisitionRate = oneMinuteAcquisitionRate;
         this.fiveMinuteAcquisitionRate = fiveMinuteAcquisitionRate;
         this.fifteenMinuteAcquisitionRate = fifteenMinuteAcquisitionRate;
         this.oneMinuteFailedAcquisitionRate = oneMinuteFailedAcquisitionRate;
         this.fiveMinuteFailedAcquisitionRate = fiveMinuteFailedAcquisitionRate;
         this.fifteenMinuteFailedAcquisitionRate = fifteenMinuteFailedAcquisitionRate;
      }

      /**
       * Creates stats with all zero values.
       * @param pool The pool.
       * @return The stats.
       */
      private static Stats createZeroStats(final ConnectionPool pool) {
         return new Stats(pool.name, pool.connectionDescription, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0L, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f);
      }

      /**
       * Creates a snapshot of the current pool stats.
       * @param pool The pool.
       */
      private Stats(final ConnectionPool pool) {

         this.poolName = pool.name;
         this.connectionDescription = pool.connectionDescription;

         long _connectionErrorCount = 0L;
         long _activeTimeoutCount = 0L;

         for(ConnectionPoolSegment segment : pool.segments) {
            _connectionErrorCount += segment.stats.getFailedConnectionErrorCount();
            _activeTimeoutCount += segment.stats.getActiveTimeoutCount();
         }

         this.connectionCount = pool.acquisitions.getCount();
         this.connectionErrorCount = _connectionErrorCount;
         this.activeTimeoutCount = _activeTimeoutCount;
         this.failedAcquisitionCount = pool.failedAcquisitions.getCount();
         this.segmentExpansionCount = pool.segmentExpansions.get();
         this.activeSegments = pool.activeSegments;
         this.activeUnmanagedConnectionCount = pool.activeUnmanagedConnections.getCount();

         this.activeConnections = pool.getActiveConnections();
         this.availableConnections = pool.getAvailableConnections();
         this.maxConnections = pool.getMaxConnections();

         this.oneMinuteAcquisitionRate = pool.acquisitions.getOneMinuteRate();
         this.fiveMinuteAcquisitionRate = pool.acquisitions.getFiveMinuteRate();
         this.fifteenMinuteAcquisitionRate = pool.acquisitions.getFifteenMinuteRate();
         this.oneMinuteFailedAcquisitionRate = pool.failedAcquisitions.getOneMinuteRate();
         this.fiveMinuteFailedAcquisitionRate = pool.failedAcquisitions.getFiveMinuteRate();
         this.fifteenMinuteFailedAcquisitionRate = pool.failedAcquisitions.getFifteenMinuteRate();
      }

      /**
       * Subtracts other stats from this.
       * @param other The stats to subtract.
       * @return The result.
       */
      public Stats subtract(final Stats other) {

         return new Stats(this.poolName, this.connectionDescription,
                 this.connectionCount - other.connectionCount,
                 this.connectionErrorCount - other.connectionErrorCount,
                 this.activeTimeoutCount - other.activeTimeoutCount,
                 this.failedAcquisitionCount - other.failedAcquisitionCount,
                 this.segmentExpansionCount - other.segmentExpansionCount,
                 this.activeSegments - other.activeSegments,
                 this.activeConnections - other.activeConnections,
                 this.availableConnections - other.availableConnections,
                 this.maxConnections,
                 this.activeUnmanagedConnectionCount - other.activeUnmanagedConnectionCount,
                 this.oneMinuteAcquisitionRate - other.oneMinuteAcquisitionRate,
                 this.fiveMinuteAcquisitionRate - other.fiveMinuteAcquisitionRate,
                 this.fifteenMinuteAcquisitionRate - other.fifteenMinuteAcquisitionRate,
                 this.oneMinuteFailedAcquisitionRate - other.oneMinuteFailedAcquisitionRate,
                 this.fiveMinuteFailedAcquisitionRate - other.fiveMinuteFailedAcquisitionRate,
                 this.fifteenMinuteFailedAcquisitionRate - other.fifteenMinuteFailedAcquisitionRate);
      }

      /**
       * Gets the name of the pool.
       * @return The pool name.
       */
      public String getName() {
         return poolName;
      }

      /**
       * Gets a description of the database connections configured for the pool.
       * @return The description.
       */
      public String getConnectionDescription() {
         return connectionDescription;
      }

      /**
       * Gets the fraction of connections in active segments being used.
       * @return The fraction.
       */
      public float getActiveConnectionUtilization() {
         if(activeConnections >= availableConnections) {
            return 1.0f;
         } else {
            return (float)activeConnections / (float)availableConnections;
         }
      }

      /**
       * Gets the fraction of connections in all segments (active + reserve) being used.
       * @return The percent.
       */
      public float getAvailableConnectionUtilization() {
         if(activeConnections >= maxConnections) {
            return 1.0f;
         } else {
            return (float)activeConnections / (float)maxConnections;
         }
      }

      /**
       * Gets the number of connections that are currently active.
       * @return The number of active connections.
       */
      public int getActiveConnections() {
         return activeConnections;
      }

      /**
       * Gets the number of connections that are currently available.
       * @return The number of available.
       */
      public int getAvailableConnections() {
         return availableConnections;
      }

      /**
       * Gets the maximum number of connections.
       * @return The maximum number of connections.
       */
      public int getMaxConnections() {
         return maxConnections;
      }

      /**
       * Gets the timestamp when these stats were recorded.
       * @return The timestamp.
       */
      public long getTimestamp() {
         return createTime;
      }

      /**
       * Gets the connection count.
       * @return The connection count.
       */
      public long getConnectionCount() {
         return connectionCount;
      }

      /**
       * Gets the count of failed connection errors.
       * @return The connection error count.
       */
      public long getFailedConnectionErrorCount() {
         return connectionErrorCount;
      }

      /**
       * Gets the count of connections closed when active limit is reached.
       * @return The active timeout count.
       */
      public long getActiveTimeoutCount() {
         return activeTimeoutCount;
      }

      /**
       * Gets the count of failed acquisitions.
       * @return The failed acquisition count.
       */
      public long getFailedAcquisitionCount() {
         return failedAcquisitionCount;
      }

      /**
       * Gets the number of times the pool has expanded the number of active segments.
       * @return The segment expansion count.
       */
      public long getSegmentExpansionCount() {
         return segmentExpansionCount;
      }

      /**
       * Gets the number of active unmanaged connections.
       * @return The unmanaged connection count.
       */
      public long getActiveUnmanagedConnectionCount() {
         return activeUnmanagedConnectionCount;
      }

      /**
       * Gets the number of active segments.
       * @return The number of active segments.
       */
      public int getActiveSegments() {
         return activeSegments;
      }

      /**
       * Gets the ("per") unit for acquisition rates.
       * @return The rate.
       */
      public TimeUnit getAcquisitionRateUnit() {
         return TimeUnit.SECONDS;
      }

      /**
       * Gets the one minute connection acquisition rate.
       * @return The rate.
       */
      public double getOneMinuteAcquisitionRate() {
         return oneMinuteAcquisitionRate;
      }

      /**
       * Gets the five minute connection acquisition rate.
       * @return The rate.
       */
      public double getFiveMinuteAcquisitionRate() {
         return fiveMinuteAcquisitionRate;
      }

      /**
       * Gets the fifteen minute connection acquisition rate.
       * @return The rate.
       */
      public double getFifteenMinuteAcquisitionRate() {
         return fifteenMinuteAcquisitionRate;
      }

      /**
       * Gets the one minute acquisition failure rate.
       * @return The rate.
       */
      public double getOneMinuteFailedAcquisitionRate() {
         return oneMinuteFailedAcquisitionRate;
      }

      /**
       * Gets the five minute acquisition failure rate.
       * @return The rate.
       */
      public double getFiveMinuteFailedAcquisitionRate() {
         return fiveMinuteFailedAcquisitionRate;
      }

      /**
       * Gets the fifteen minute acquisition failure rate.
       * @return The rate.
       */
      public double getFifteenMinuteFailedAcquisitionRate() {
         return fifteenMinuteFailedAcquisitionRate;
      }

      private final String poolName;
      private final String connectionDescription;
      private final int activeConnections;
      private final int availableConnections;
      private final int maxConnections;
      private final long createTime = System.currentTimeMillis();
      private final long connectionCount;
      private final long connectionErrorCount;
      private final long activeTimeoutCount;
      private final long failedAcquisitionCount;
      private final long segmentExpansionCount;
      private final long activeUnmanagedConnectionCount;
      private final int activeSegments;
      private final double oneMinuteAcquisitionRate;
      private final double fiveMinuteAcquisitionRate;
      private final double fifteenMinuteAcquisitionRate;
      private final double oneMinuteFailedAcquisitionRate;
      private final double fiveMinuteFailedAcquisitionRate;
      private final double fifteenMinuteFailedAcquisitionRate;
   }

   /**
    * Initializes and creates connection pools.
    */
   public static class Initializer {

      /**
       * Sets the name of the pool.
       * @param name The name.
       * @return A self-reference.
       */
      public Initializer setName(final String name) {
         this.name = name;
         return this;
      }

      /**
       * Adds an active segment to the pool.
       * @param segment The segment to add.
       * @return A self-reference.
       */
      public Initializer addActiveSegment(final ConnectionPoolSegment segment) {
         activeSegments.add(segment);
         return this;
      }

      /**
       * Adds active segments to the pool.
       * @param segments The segments to add.
       * @return A self-reference.
       */
      public Initializer addActiveSegments(final List<ConnectionPoolSegment> segments) {
         activeSegments.addAll(segments);
         return this;
      }

      /**
       * Adds a reserve segment to the pool.
       * @param segment The segment to add.
       * @return A self-reference.
       */
      public Initializer addReserveSegment(final ConnectionPoolSegment segment) {
         reserveSegments.add(segment);
         return this;
      }

      /**
       * Adds reserve segments to the pool.
       * @param segments The segments to add.
       * @return A self-reference.
       */
      public Initializer addReserveSegments(final List<ConnectionPoolSegment> segments) {
         reserveSegments.addAll(segments);
         return this;
      }

      /**
       * Sets the minimum number of active statements.
       * @param minActiveSegments The minimum number of active segments.
       * @return A self-reference.
       */
      public Initializer setMinActiveSegments(final int minActiveSegments) {
         this.minActiveSegments = minActiveSegments;
         return this;
      }

      /**
       * Sets the interval between checks for idle segments.
       * @param idleCheckInterval The idle check interval.
       * @param idleCheckIntervalUnit The idle check interval units.
       * @return A self-reference.
       */
      public Initializer setIdleCheckInterval(final long idleCheckInterval, final TimeUnit idleCheckIntervalUnit) {
         this.idleCheckIntervalMillis = TimeUnit.MILLISECONDS.convert(idleCheckInterval, idleCheckIntervalUnit);
         return this;
      }

      /**
       * Sets the maximum amount of time to wait for an available connection when pool is saturated.
       * @param saturatedAcquireTimeout The saturated acquire timeout.
       * @param saturatedAcquireTimeoutUnit The saturated acquire timeout units.
       * @return A self-reference.
       */
      public Initializer setSaturatedAcquireTimeout(final long saturatedAcquireTimeout, final TimeUnit saturatedAcquireTimeoutUnit) {
         this.saturatedAcquireTimeoutMillis = TimeUnit.MILLISECONDS.convert(saturatedAcquireTimeout, saturatedAcquireTimeoutUnit);
         return this;
      }

      /**
       * Sets the logger.
       * @param logger The logger.
       * @return A self-reference.
       */
      public Initializer setLogger(final Logger logger) {
         this.logger = logger;
         return this;
      }


      /**
       * Sets the statistics sampler.
       * @param statsSampler The sampler.
       * @return A self-reference.
       */
      public Initializer setStatsSampler(final StatsSampler statsSampler) {
         this.statsSampler = statsSampler;
         return this;
      }

      /**
       * Adds an alias for this pool.
       * @param alias The alias name.
       * @return A self-reference.
       */
      public Initializer addAlias(final String alias) {
         this.aka.add(alias);
         return this;
      }

      /**
       * Creates a configured pool.
       * @return The pool.
       * @throws SQLException if pool could not be created.
       * @throws InitializationException if pool was improperly configured.
       */
      public ConnectionPool createPool() throws SQLException, InitializationException {

         for(ConnectionPoolSegment segment : activeSegments) {
            if(segment.pool != null) {
               throw new InitializationException("The segment, '" + segment.name + "' may not be added to multiple pools");
            }
         }

         for(ConnectionPoolSegment segment : reserveSegments) {
            if(segment.pool != null) {
               throw new InitializationException("The segment, '" + segment.name + "' may not be added to multiple pools");
            }
         }

         ConnectionPool pool = new ConnectionPool(name, activeSegments, reserveSegments, saturatedAcquireTimeoutMillis,
                 minActiveSegments, idleCheckIntervalMillis, statsSampler, logger);

         for(String alias : aka) {
            pool.aka.add(alias);
         }

         for(ConnectionPoolSegment segment : activeSegments) {
            segment.pool = pool;
            segment.startActiveTimeoutMonitor(pool.inactiveMonitorService);
         }

         for(ConnectionPoolSegment segment : reserveSegments) {
            segment.pool = pool;
            segment.startActiveTimeoutMonitor(pool.inactiveMonitorService);
         }

         return pool;
      }

      /**
       * Creates pool initializers from properties.
       * @param props The properties.
       * @return The initializer
       * @throws InitializationException if configuration is invalid.
       */
      public static final Initializer[] fromProperties(final Properties props) throws InitializationException {
         return fromProperties(props, null);
      }

      /**
       * Creates pool initializers from properties.
       * @param props The properties.
       * @param passwordSource A password source. May be <tt>null</tt>.
       * @return The initializer
       * @throws InitializationException if configuration is invalid.
       */
      public static final Initializer[] fromProperties(final Properties props,
                                                       final PasswordSource passwordSource) throws InitializationException {
         JDBConnection.initDrivers(props);
         Map<String, JDBConnection> connectionMap = JDBConnection.mapFromProperties(props);
         Properties poolProps = new InitUtil("pool.", props, false).getProperties();

         Logger globalLogger = null;
         if(props.getProperty("logger.class") != null) {
            try {
               globalLogger = (Logger)(Class.forName(props.getProperty("logger.class")).newInstance());
            } catch(Throwable e) {
               throw new InitializationException("Unable to initialize logger", e);
            }
         }

         Set<String> poolNameSet = Sets.newHashSet();
         List<Initializer> initializerList = Lists.newArrayListWithExpectedSize(8);
         for(Object k : poolProps.keySet()) {
            String key = (String)k;
            int index = key.indexOf('.');
            if(index > 0) {
               String poolName = key.substring(0, index);
               if(!poolNameSet.contains(poolName)) {
                  poolNameSet.add(poolName);
                  ConnectionPool.Initializer poolInitializer = fromProperties(poolName, poolName + ".", poolProps, connectionMap, passwordSource);
                  poolInitializer.setLogger(globalLogger);
                  poolInitializer.setName(poolName);
                  initializerList.add(poolInitializer);
               }
            }
         }

         return initializerList.toArray(new Initializer[initializerList.size()]);
      }

      /**
       * Initializes a pool from properties.
       * @param poolName The pool name.
       * @param prefix The property prefix (e.g. '.').
       * @param props The properties.
       * @param connectionMap A map of configured connections.
       * @param passwordSource A password source. May be <tt>null</tt>.
       * @return The initializer
       * @throws InitializationException if configuration is invalid.
       */
      public static final Initializer fromProperties(final String poolName,
                                                     final String prefix, final Properties props,
                                                     final Map<String, JDBConnection> connectionMap,
                                                     final PasswordSource passwordSource) throws InitializationException {
         InitUtil init = new InitUtil(prefix, props, false);
         Properties initProps = init.getProperties();

         Initializer poolInit = new Initializer();
         poolInit.setName(poolName);

         poolInit.setMinActiveSegments(init.getIntProperty("minActiveSegments", 1));

         poolInit.setSaturatedAcquireTimeout(InitUtil.millisFromTime(init.getProperty("saturatedAcquireTimeout", "0ms")), TimeUnit.MILLISECONDS);
         poolInit.setIdleCheckInterval(InitUtil.millisFromTime(init.getProperty("idleCheckInterval", "60s")), TimeUnit.MILLISECONDS);

         Set<String> segmentNameSet = Sets.newHashSet();
         for(Object k : initProps.keySet()) {
            String key = (String)k;
            if(key.startsWith("segment")) {
               int index = key.indexOf('.');
               if(index > 0) {
                  segmentNameSet.add(key.substring(0, index));
               }
            }
         }

         List<String> segmentNames = Lists.newArrayList(segmentNameSet);
         Collections.sort(segmentNames);

         Map<String, ConnectionPoolSegment.Initializer> initializerMap = Maps.newHashMap();

         for(String segmentName : segmentNames) {
            ConnectionPoolSegment.Initializer segmentInitializer;
            InitUtil segmentPropsInit = new InitUtil(segmentName + ".", initProps, false);
            String cloneSegmentName = segmentPropsInit.getProperty("clone");
            ConnectionPoolSegment.Initializer baseInitializer = initializerMap.get(cloneSegmentName);
            if(cloneSegmentName != null && baseInitializer == null) {
               throw new InitializationException("Forward 'clone' reference to " + cloneSegmentName);
            } else if(baseInitializer == null) {
               segmentInitializer = new ConnectionPoolSegment.Initializer();
            } else {
               segmentInitializer = new ConnectionPoolSegment.Initializer(baseInitializer);
            }

            Properties segmentInitProps = segmentPropsInit.getProperties();
            segmentInitProps.put("name", segmentName);
            segmentInitializer = ConnectionPoolSegment.Initializer.fromProperties(poolName, segmentInitializer, segmentInitProps, connectionMap);
            segmentInitializer.setPasswordSource(passwordSource);
            initializerMap.put(segmentName, segmentInitializer);
         }

         int startActiveSegments = init.getIntProperty("startActiveSegments", 1);
         for(int i = 0; i < segmentNames.size(); i++) {
            if(i < startActiveSegments) {
               poolInit.addActiveSegment(initializerMap.get(segmentNames.get(i)).createSegment());
            } else {
               poolInit.addReserveSegment(initializerMap.get(segmentNames.get(i)).createSegment());
            }
         }

         return poolInit;
      }

      /**
       * Creates pool initializers from a DOM element containing the XML configuration,
       * a connection definition map and a password source.
       * @param rootElem The XML root element.
       * @param connectionMap A map of connection vs. name. If <tt>null</tt>, connections must be configured through the XML <tt>connections</tt> element.
       * @param passwordSource A password source.
       * @return The initializers.
       * @throws InitializationException if configuration is invalid.
       */
      public static final Initializer[] fromXML(final Element rootElem,
                                                Map<String, JDBConnection> connectionMap,
                                                final PasswordSource passwordSource) throws InitializationException {

         JDBConnection.initDrivers(rootElem);

         Logger globalLogger = null;

         Element loggerElem = DOMUtil.getFirstChild(rootElem, "logger");
         if(loggerElem != null) {
            String loggerClassName = loggerElem.getAttribute("class");
            if(loggerClassName.length() > 0) {
               try {
                  globalLogger = (Logger)(Class.forName(loggerClassName).newInstance());
               } catch(Throwable e) {
                  throw new InitializationException("Unable to initialize logger", e);
               }
            }
         }

         if(connectionMap == null) {
            Element connectionsElem = DOMUtil.getFirstChild(rootElem, "connections");
            if(connectionsElem != null) {
               if(connectionsElem.getAttribute("import").length() > 0) {
                  File connectionsFile = new File(connectionsElem.getAttribute("import"));
                  try {
                     DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                     Document doc = builder.parse(connectionsFile);
                     connectionsElem = doc.getDocumentElement();
                  } catch(IOException ioe) {
                     throw new InitializationException("Unable to parse connections import, '" + connectionsFile.getAbsolutePath(), ioe);
                  } catch(ParserConfigurationException pe) {
                     throw new InitializationException("Unable to load XML parser", pe);
                  } catch(SAXException se) {
                     throw new InitializationException("Unable to parse connections import, '" + connectionsFile.getAbsolutePath(), se);
                  }
               }

               connectionMap = JDBConnection.mapFromXML(connectionsElem);
            }
         }

         NodeList poolList = rootElem.getElementsByTagName("pool");
         if(poolList.getLength() == 0) {
            throw new InitializationException("At least one 'pool' element must be present");
         }

         Initializer[] initializers = new Initializer[poolList.getLength()];

         for(int p = 0; p < poolList.getLength(); p++) {

            Map<String, ConnectionPoolSegment.Initializer> segmentInitializers = Maps.newHashMap();

            Element elem = (Element)poolList.item(p);
            Element connectionsElem = DOMUtil.getFirstChild(elem, "connections");
            Map<String, JDBConnection> poolConnectionMap = connectionMap;

            if(connectionsElem != null) {
               Map<String, JDBConnection> useMap = Maps.newHashMap();
               if(connectionMap != null) {
                  useMap.putAll(connectionMap);
               }

               poolConnectionMap = JDBConnection.mapFromXML(connectionsElem);
               if(poolConnectionMap != null) {
                  useMap.putAll(poolConnectionMap);
               }

               poolConnectionMap = useMap.size() == 0 ? null : useMap;
            }

            Initializer initializer = new Initializer();

            initializer.name = elem.getAttribute("name");
            initializer.logger = globalLogger;

            Element akaElem = DOMUtil.getFirstChild(elem, "aka");
            if(akaElem != null) {
               List<Element> aliasList = DOMUtil.getChildElementsByTagName(akaElem, "alias");
               for(Element alias : aliasList) {
                  String name = alias.getAttribute("name").trim();
                  if(name.length() > 0) {
                     initializer.aka.add(name);
                  }
               }
            }

            loggerElem = DOMUtil.getFirstChild(elem, "logger");
            if(loggerElem != null) {
               String loggerClassName = loggerElem.getAttribute("class");
               if(loggerClassName.length() > 0) {
                  try {
                     initializer.logger = (Logger)(Class.forName(loggerClassName).newInstance());
                  } catch(Throwable e) {
                     throw new InitializationException("Unable to initialize logger", e);
                  }
               } else {
                  throw new InitializationException("Unable to initialize logger - a 'class' must be specified");
               }
            }

            Element statsSamplerElem = DOMUtil.getFirstChild(elem, "sampler");
            if(statsSamplerElem != null) {
               String samplerClassName = statsSamplerElem.getAttribute("class");
               if(samplerClassName.length() > 0) {
                  try {
                     initializer.statsSampler = (StatsSampler)(Class.forName(samplerClassName).newInstance());
                     Element samplerProperties = DOMUtil.getFirstChild(statsSamplerElem, "properties");
                     Properties properties = new Properties();
                     if(samplerProperties != null) {
                        List<Element> propertyElements = DOMUtil.getChildElementsByTagName(samplerProperties, "property");
                        for(Element propElem : propertyElements) {
                           String name = propElem.getAttribute("name").trim();
                           String val = propElem.getAttribute("value").trim();
                           if(name.length() > 0) {
                              properties.setProperty(name, val);
                           }
                        }
                     }
                     initializer.statsSampler.init(properties, initializer.logger);
                  } catch(Throwable t) {
                     throw new InitializationException("Unable to initialize sampler", t);
                  }
               } else {
                  throw new InitializationException("Unable to initialize sampler - a 'class' must be specified");
               }
            }

            Element activeElem = DOMUtil.getFirstChild(elem, "active");
            if(activeElem == null) {
               throw new InitializationException("A single 'active' element must be present");
            } else {
               String minActive = activeElem.getAttribute("min");
               if(minActive.length() > 0) {
                  try {
                     initializer.minActiveSegments = Integer.parseInt(minActive);
                  } catch(Exception e) {
                     throw new InitializationException("The 'min' for the 'active' element must be an integer");
                  }
               } else {
                  initializer.minActiveSegments = 1;
               }

               NodeList segmentList = activeElem.getElementsByTagName("segment");
               if(segmentList.getLength() == 0) {
                  throw new InitializationException("At least one 'segment' must be defined in the 'active' element");
               } else {
                  for(int i = 0; i < segmentList.getLength(); i++) {
                     Element segmentElem = (Element)segmentList.item(i);
                     String segmentName = segmentElem.getAttribute("name");
                     if(segmentName.length() == 0) {
                        throw new InitializationException("A 'name' must be supplied for all segments.");
                     } else if(segmentInitializers.get(segmentName) != null) {
                        throw new InitializationException("The 'name' must be unique for all segmements in a pool");
                     }

                     String clone = segmentElem.getAttribute("clone");
                     ConnectionPoolSegment.Initializer segmentInitializer;
                     if(clone.length() > 0) {
                        segmentInitializer = segmentInitializers.get(clone);
                        if(segmentInitializer == null) {
                           throw new InitializationException("Unable to clone. Segment not found: '" + clone + "'");
                        }
                        ConnectionPoolSegment.Initializer.fromXML(initializer.name, segmentInitializer, segmentElem, poolConnectionMap);
                     } else {
                        segmentInitializer = ConnectionPoolSegment.Initializer.fromXML(initializer.name, segmentElem, poolConnectionMap);
                     }
                     segmentInitializer.validate(true);
                     segmentInitializer.setLogger(initializer.logger);
                     segmentInitializer.setPasswordSource(passwordSource);
                     segmentInitializers.put(segmentName, segmentInitializer);
                     initializer.activeSegments.add(segmentInitializer.createSegment());
                  }
               }
            }

            Element reserveElem = DOMUtil.getFirstChild(elem, "reserve");
            if(reserveElem != null) {
               NodeList segmentList = reserveElem.getElementsByTagName("segment");
               if(segmentList.getLength() > 0) {
                  for(int i = 0; i < segmentList.getLength(); i++) {
                     Element segmentElem = (Element)segmentList.item(i);
                     String segmentName = segmentElem.getAttribute("name");
                     if(segmentName.length() == 0) {
                        throw new InitializationException("A 'name' must be supplied for all segments.");
                     } else if(segmentInitializers.get(segmentName) != null) {
                        throw new InitializationException("The 'name' must be unique for all segmements in a pool");
                     }
                     String clone = segmentElem.getAttribute("clone");
                     ConnectionPoolSegment.Initializer segmentInitializer;
                     if(clone.length() > 0) {
                        segmentInitializer = segmentInitializers.get(clone);
                        if(segmentInitializer == null) {
                           throw new InitializationException("Unable to clone. Segment not found: '" + clone + "'");
                        }
                        ConnectionPoolSegment.Initializer.fromXML(initializer.name, segmentInitializer, segmentElem, poolConnectionMap);
                     } else {
                        segmentInitializer = ConnectionPoolSegment.Initializer.fromXML(initializer.name, segmentElem, poolConnectionMap);
                     }
                     segmentInitializer.validate(true);
                     segmentInitializer.setLogger(initializer.logger);
                     segmentInitializer.setPasswordSource(passwordSource);
                     segmentInitializers.put(segmentName, segmentInitializer);
                     initializer.reserveSegments.add(segmentInitializer.createSegment());
                  }
               }
            }

            if(initializer.minActiveSegments > 1) {
               initializer.idleCheckIntervalMillis = Util.millisFromElem(elem, "idleCheckInterval", 60000L);
               if(initializer.idleCheckIntervalMillis < 1L) {
                  throw new InitializationException("The 'time' for 'idleCheckInterval' must be > 0");
               }
            }
            initializer.saturatedAcquireTimeoutMillis = Util.millisFromElem(elem, "saturatedAcquireTimeout", 0L);
            initializers[p] = initializer;
         }

         return initializers;
      }

      String name;
      final Set<String> aka = Sets.newHashSet();
      final List<ConnectionPoolSegment> activeSegments = Lists.newArrayListWithExpectedSize(4);
      final List<ConnectionPoolSegment> reserveSegments = Lists.newArrayListWithExpectedSize(4);
      int minActiveSegments = 0;
      long saturatedAcquireTimeoutMillis;
      long idleCheckIntervalMillis = 60000L;
      Logger logger;
      ConnectionPool.StatsSampler statsSampler = null;
   } //Initializer

   /**
    * Creates an empty initializer.
    * @return The initializer.
    */
   public static Initializer newInitializer() {
      return new Initializer();
   }

   /**
    * Creates pools from a DOM element containing the XML configuration.
    * @param elem The root configuration element.
    * @return The pools.
    * @throws SQLException on SQL error while creating the pool.
    * @throws InitializationException if initialization elements are missing or incorrect.
    */
   public static ConnectionPool[] createPools(final Element elem) throws SQLException, InitializationException {

      Initializer[] initializers = ConnectionPool.Initializer.fromXML(elem, null, null);
      ConnectionPool[] pools = new ConnectionPool[initializers.length];
      for(int i = 0; i < pools.length; i++) {
         pools[i] = initializers[i].createPool();
      }

      return pools;
   }

   /**
    * Creates a pool from a DOM element containing the XML configuration and a connectdion definition map.
    * @param elem The element.
    * @param connectionMap A map of <tt>JDBCConnection</tt> vs. name. May be <tt>null</tt>.
    * @return The pool.
    * @throws SQLException on SQL error while creating the pool.
    * @throws InitializationException if initialization elements are missing or incorrect.
    */
   public static ConnectionPool[] createPools(Element elem,
                                              Map<String, JDBConnection> connectionMap) throws SQLException, InitializationException {
      return createPools(elem, connectionMap, null);
   }

   /**
    * Creates a pool from a DOM element containing the XML configuration, a connection definition map and a password source.
    * @param elem The element.
    * @param connectionMap A map of <tt>JDBCConnection</tt> vs. name. May be <tt>null</tt>.
    * @param passwordSource The password source.
    * @return The pool.
    * @throws SQLException on SQL error while creating the pool.
    * @throws InitializationException if initialization elements are missing or incorrect.
    */
   public static ConnectionPool[] createPools(final Element elem,
                                              final Map<String, JDBConnection> connectionMap,
                                              final PasswordSource passwordSource) throws SQLException, InitializationException {

      Initializer[] initializers = ConnectionPool.Initializer.fromXML(elem, connectionMap, passwordSource);
      ConnectionPool[] pools = new ConnectionPool[initializers.length];
      for(int i = 0; i < pools.length; i++) {
         pools[i] = initializers[i].createPool();
      }

      return pools;
   }

   /**
    * Creates pools from a configuration file containing the XML configuration.
    * @param file The configuration file.
    * @return The pools.
    * @throws IOException on file parse error.
    * @throws SQLException on SQL error while creating the pool.
    * @throws InitializationException if initialization elements are missing or incorrect.
    */
   public static ConnectionPool[] createPools(final File file) throws IOException, SQLException, InitializationException {
      return createPools(file, null, null);
   }

   /**
    * Creates pools from a configuration file containing the XML configuration and a connection definition map.
    * @param file The configuration file.
    * @param connectionMap A map of <tt>JDBCConnection</tt> vs. name. May be <tt>null</tt>.
    * @return The pools.
    * @throws IOException on file parse error.
    * @throws SQLException on SQL error while creating the pool.
    * @throws InitializationException if initialization elements are missing or incorrect.
    */
   public static ConnectionPool[] createPools(final File file,
                                              final Map<String, JDBConnection> connectionMap) throws IOException, SQLException, InitializationException {
      return createPools(file, connectionMap, null);
   }

   /**
    * Creates pools from a configuration file containing the XML configuration, a connection definition map
    * and a password source.
    * @param file The configuration file.
    * @param connectionMap A map of <tt>JDBCConnection</tt> vs. name. May be <tt>null</tt>.
    * @param passwordSource A password source.
    * @return The pools.
    * @throws IOException on file parse error.
    * @throws SQLException on SQL error while creating the pool.
    * @throws InitializationException if initialization elements are missing or incorrect.
    */
   public static ConnectionPool[] createPools(final File file,
                                              final Map<String, JDBConnection> connectionMap,
                                              final PasswordSource passwordSource) throws IOException, SQLException, InitializationException {
      if(!file.exists()) {
         throw new InitializationException("The configuration file, " + file.getAbsolutePath() + " does not exist");
      } else if(!file.canRead()) {
         throw new InitializationException("The configuration file, " + file.getAbsolutePath() + " can't be read");
      }

      try {
         DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
         Document doc = builder.parse(file);
         Element elem = doc.getDocumentElement();
         return createPools(elem, connectionMap, passwordSource);
      } catch(ParserConfigurationException pe) {
         throw new InitializationException("An XML parser was not found", pe);
      } catch(SAXException se) {
         throw new IOException("The configuration file could not be parsed", se);
      }
   }

   /**
    * Creates pools from a stream opened on XML configuration data.
    * @param is The stream.
    * @return The pools.
    * @throws IOException on file parse error.
    * @throws SQLException on SQL error while creating the pool.
    * @throws InitializationException if initialization elements are missing or incorrect.
    */
   public static ConnectionPool[] createPools(final InputStream is) throws IOException, SQLException, InitializationException {
      return createPools(is, null, null);
   }

   /**
    * Creates pools from a stream opened on XML configuration data and a connection definition map.
    * @param is The stream.
    * @param connectionMap A map of <tt>JDBCConnection</tt> vs. name. May be <tt>null</tt>.
    * @return The pools.
    * @throws IOException on file parse error.
    * @throws SQLException on SQL error while creating the pool.
    * @throws InitializationException if initialization elements are missing or incorrect.
    */
   public static ConnectionPool[] createPools(final InputStream is,
                                              final Map<String, JDBConnection> connectionMap) throws IOException, SQLException, InitializationException {
      return createPools(is, connectionMap, null);
   }

   /**
    * Creates pools from a stream opened on XML configuration data, a connection definition map and a password source.
    * @param is The stream.
    * @param connectionMap A map of <tt>JDBCConnection</tt> vs. name. May be <tt>null</tt>.
    * @param passwordSource A password source.
    * @return The pools.
    * @throws IOException on file parse error.
    * @throws SQLException on SQL error while creating the pool.
    * @throws InitializationException if initialization elements are missing or incorrect.
    */
   public static ConnectionPool[] createPools(final InputStream is,
                                              final Map<String, JDBConnection> connectionMap,
                                              final PasswordSource passwordSource) throws IOException, SQLException, InitializationException {

      try {
         DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
         Document doc = builder.parse(is);
         Element elem = doc.getDocumentElement();
         return createPools(elem, connectionMap, passwordSource);
      } catch(ParserConfigurationException pe) {
         throw new InitializationException("An XML parser was not found", pe);
      } catch(SAXException se) {
         throw new IOException("The configuration file could not be parsed", se);
      }
   }

   /**
    * Creates a pool.
    * <p>
    * A segment object may not be added to the active or reserve
    * list more than once.
    * </p>
    * @param name The pool name.
    * @param activeSegments The list of active segments. Must be > 0.
    * @param reserveSegments The list of reserve segments. May be empty.
    * @param saturatedAcquireTimeoutMillis The timeout before failure when pool is saturated (all connections busy).
    * @param minActiveSegments The minimum number of active segments.
    * @param idleCheckIntervalMillis The frequency of idle pool checks.
    * @param statsSampler The stats sampler.
    * @param logger The logger.
    * @throws SQLException if connections could not be created.
    * @throws InitializationException if pool could not be created.
    */
   private ConnectionPool(final String name,
                          final List<ConnectionPoolSegment> activeSegments,
                          final List<ConnectionPoolSegment> reserveSegments,
                          final long saturatedAcquireTimeoutMillis,
                          int minActiveSegments,
                          final long idleCheckIntervalMillis,
                          final StatsSampler statsSampler,
                          final Logger logger) throws SQLException, InitializationException {

      this.name = name;

      this.acquisitions = new Timer();
      this.failedAcquisitions = new Meter();
      this.activeUnmanagedConnections = new Counter();
      this.activeSegmentsGauge = new Gauge<Integer>() {
         @Override
         public Integer getValue() {
            return ConnectionPool.this.activeSegments;
         }
      };
      this.segmentExpansionGauge = new Gauge<Long>() {
         @Override
         public Long getValue() {
            return ConnectionPool.this.segmentExpansions.get();
         }
      };
      this.activeConnectionUtilizationGauge = new Gauge<Float>() {
         @Override
         public Float getValue() {
            final int avail = getAvailableConnections();
            final int active = getActiveConnections();
            if(active >= avail) {
               return 1.0f;
            } else {
               return (float)(active) / (float)avail;
            }
         }
      };
      this.availableConnectionUtilizationGauge = new Gauge<Float>() {
         @Override
         public Float getValue() {
            final int max = getMaxConnections();
            final int active = getActiveConnections();
            if(active >= max) {
               return 1.0f;
            } else {
               return (float)active / (float)max;
            }
         }
      };
      this.metrics = new MetricSet() {
         @Override
         public Map<String, Metric> getMetrics() {
            ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
            builder.put("acquisitions", acquisitions);
            builder.put("failed-acquisitions", failedAcquisitions);
            builder.put("active-unmanaged-connections", activeUnmanagedConnections);
            builder.put("active-segments", activeSegmentsGauge);
            builder.put("segment-expansions", segmentExpansionGauge);
            builder.put("active-connections-utilized", activeConnectionUtilizationGauge);
            builder.put("available-connections-utilized", availableConnectionUtilizationGauge);
            return builder.build();
         }
      };

      this.SATURATED_MESSAGE = "Connection pool '" + name + "' is saturated";
      this.logger = logger;
      this.statsSampler = statsSampler;

      if(activeSegments == null || activeSegments.size() == 0) {
         throwInitException("The list of active segments must not be empty");
      }

      if(minActiveSegments == 0) {
         minActiveSegments = 1;
      }

      List<ConnectionPoolSegment> segments = Lists.newArrayList(activeSegments);

      if(reserveSegments != null) {
         segments.addAll(reserveSegments);
      }

      //Make sure all segments are unique...

      Set<ConnectionPoolSegment> segmentSet = Sets.newHashSet();
      for(ConnectionPoolSegment segment : segments) {
         if(segment == null) {
            throwInitException("A null segment was detected in the segment list");
         } else if(segmentSet.contains(segment)) {
            throwInitException("Segments must be unique in the segment list");
         } else {
            segmentSet.add(segment);
         }
      }

      int maxSegments = segments.size();

      if(minActiveSegments > activeSegments.size()) {
         throwInitException("The 'minActiveSegments' must be <= the total number of active segments");
      }

      this.minActiveSegments = minActiveSegments;
      this.segments = new ConnectionPoolSegment[maxSegments];
      int pos = 0;
      for(ConnectionPoolSegment segment : segments) {
         this.segments[pos++] = segment;
      }

      this.saturatedAcquireTimeoutMillis = saturatedAcquireTimeoutMillis;

      this.activeSegments = activeSegments.size();
      if(minActiveSegments > 0) {
         for(ConnectionPoolSegment segment : activeSegments) {
            try {
               segment.activate();
            } catch(SQLException se) {
               throw new InitializationException("Problem activating segment '" + segment.name + "' in pool '" + name + "'", se);
            }
         }
      }

      if(this.segments.length > 1 && this.minActiveSegments != this.segments.length) {
         segmentSignalQueue = new ArrayBlockingQueue<Object>(1);
         String signalMonitorThreadName = StringUtil.hasContent(name) ? (name + ":SignalMonitor") : "SignalMonitor";
         segmentSignalMonitorThread = new Thread(new SegmentSignalMonitor(), "ACP:" + signalMonitorThreadName);
         segmentSignalMonitorThread.start();
         idleSegmentMonitorService = Executors.newScheduledThreadPool(1, Util.createThreadFactoryBuilder(name, "IdleMonitor"));
         idleSegmentMonitorService.scheduleWithFixedDelay(new IdleSegmentMonitor(), idleCheckIntervalMillis, idleCheckIntervalMillis, TimeUnit.MILLISECONDS);
      } else {
         segmentSignalQueue = null;
         segmentSignalMonitorThread = null;
         idleSegmentMonitorService = null;
      }

      inactiveMonitorService = new ScheduledThreadPoolExecutor(1, Util.createThreadFactoryBuilder(name, "InactiveMonitor"));
      inactiveMonitorService.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
      inactiveMonitorService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

      if(statsSampler != null) {
         this.statsSamplerService = Executors.newScheduledThreadPool(1, Util.createThreadFactoryBuilder(name, "StatsSampler"));
         this.statsSamplerService.scheduleWithFixedDelay(
                 new Runnable() {
                    public void run() {
                       try {
                          statsSampler.acceptSample(new Stats(ConnectionPool.this));
                       } catch(Throwable t) {
                          statsSampler.acceptSample(Stats.createZeroStats(ConnectionPool.this));
                          logError("Problem sampling statistics", t);
                       }
                    }
                 },
                 statsSampler.getFrequencyMillis(), statsSampler.getFrequencyMillis(), TimeUnit.MILLISECONDS);

      } else {
         this.statsSamplerService = null;
      }

      this.connectionDescription = buildConnectionDescription();

      int maxCount = 0;
      for(ConnectionPoolSegment segment : segments) {
         maxCount += segment.getMaxConnections();
      }
      this.maxConnections = maxCount;

      StringBuilder buf = new StringBuilder("Created with ");
      buf.append(activeSegments.size()).append(activeSegments.size() == 1 ? " active segment, " : " active segments, ");
      buf.append(reserveSegments == null ? 0 : reserveSegments.size())
              .append((reserveSegments == null || reserveSegments.size() > 1) ? " reserve segments, " : " reserve segment, ");
      buf.append(minActiveSegments).append(" always active");
      logInfo(buf.toString());
   }

   /**
    * Gets a new connection that is <em>not managed by the pool</em>.
    * <p>
    * The connection is created directly by the driver/datasource as configured
    * for the first segment. Caller must call <tt>closeUnmanagedConnection</tt>
    * for this connection when complete.
    * </p>
    * @return The unmanaged connection.
    * @throws SQLException If no connection is available.
    */
   public final Connection getUnmanagedConnection() throws SQLException {
      activeUnmanagedConnections.inc();
      return segments[0].createRealConnection();
   }

   /**
    * Closes an unumanged connection.
    * @param conn The conneciton.
    * @throws SQLException If close fails.
    */
   public final void closeUnmanagedConnection(final Connection conn) throws SQLException {
      activeUnmanagedConnections.dec();
      if(!conn.isClosed()) {
         conn.close();
      }
   }

   /**
    * Gets a connection.
    * @return The connection
    * @throws SQLException If no connection is available.
    */
   public final Connection getConnection() throws SQLException {

      final Timer.Context ctx = acquisitions.time();

      try {

         Connection conn;
         for(int i = 0; i < activeSegments; i++) {
            conn = segments[i].open();
            if(conn != null) {
               return conn;
            }
         }

         //All connections for active segments were in-use.
         //Send signal to attempt to activate a new segment then
         //try the active segments in-order (again) with the acquire time-out.

         signalActivateSegment();

         try {

            for(int i = 0; i < activeSegments; i++) {
               ConnectionPoolSegment segment = segments[i];
               conn = segment.open(segment.acquireTimeoutMillis, TimeUnit.MILLISECONDS);
               if(conn != null) {
                  return conn;
               }
            }

         } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new SQLException("Connection pool, '" + name + "' interrupted during acquire", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
         }

         //Try all pools again with maximum acquire timeout.

         long maxTimeoutMillis = saturatedAcquireTimeoutMillis / activeSegments;

         try {

            for(int i = 0; i < activeSegments; i++) {
               ConnectionPoolSegment segment = segments[i];
               conn = segment.open(maxTimeoutMillis, TimeUnit.MILLISECONDS);
               if(conn != null) {
                  return conn;
               }
            }

         } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new SQLException("Connection pool, '" + name + "' interrupted during acquire", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
         }

         //Done trying

         failedAcquisitions.mark();

         throw new SQLException(SATURATED_MESSAGE, JDBConnection.SQLSTATE_CONNECTION_FAILURE);

      } finally {
         ctx.stop();
      }
   }

   /**
    * Signals that a new segment should be activated.
    */
   final void signalActivateSegment() {
      if(segmentSignalQueue != null) {
         segmentSignalQueue.offer(ACTIVATE_SEGMENT_SIGNAL);
      }
   }

   /**
    * Shutdown the pool.
    */
   public final void shutdown() {

      logInfo("Shutting down...");

      if(statsSamplerService != null) {
         logInfo("Shutting down sampler service...");
         statsSamplerService.shutdownNow();
      }

      if(idleSegmentMonitorService != null) {
         logInfo("Shutting down idle segment monitor service...");
         idleSegmentMonitorService.shutdownNow();
      }

      if(segmentSignalQueue != null) {
         segmentSignalQueue.clear();
      }

      if(segmentSignalMonitorThread != null) {
         logInfo("Shutting down segment signal monitor thread...");
         segmentSignalMonitorThread.interrupt();
      }

      logInfo("Shutting down all segments...");
      for(ConnectionPoolSegment segment : segments) {
         segment.shutdown();
      }

      logInfo("Shutting down inactive monitor service...");
      inactiveMonitorService.shutdownNow();

      logInfo("Shut down");
   }

   /**
    * Periodically signals check of the last activated segment to
    * see if it can be shutdown.
    */
   private final class IdleSegmentMonitor implements Runnable {

      public void run() {
         segmentSignalQueue.offer(IDLE_SEGMENT_CHECK_SIGNAL);
      }
   }

   /**
    * Monitors the segment signal queue for segment activate/deactivate.
    */
   private final class SegmentSignalMonitor implements Runnable {

      private long lastActivateTime = System.currentTimeMillis();
      private long lastSegmentIdleStart;

      public void run() {
         try {
            while(true) {
               Object signal = segmentSignalQueue.take();
               if(signal == ACTIVATE_SEGMENT_SIGNAL && activeSegments < segments.length) {
                  try {
                     segments[activeSegments].activate();
                     activeSegments++; //Increment only if activate does not throw exception.
                     lastActivateTime = System.currentTimeMillis();
                     lastSegmentIdleStart = 0L;
                     segmentExpansions.incrementAndGet();
                     logInfo("Activated segment " + segments[activeSegments - 1].name);
                  } catch(SQLException se) {
                     logError("Problem activating segment", se);
                     segments[activeSegments].deactivate();
                  }
               } else if(activeSegments > minActiveSegments) {
                  if(signal == DEACTIVATE_SEGMENT_SIGNAL) {
                     ConnectionPoolSegment toDeactivate = segments[--activeSegments];
                     logInfo("Deactivated segment " + toDeactivate.name);
                     boolean deactivated = toDeactivate.deactivate();
                     if(!deactivated) {
                        toDeactivate.deactivateNow();
                     }
                     lastSegmentIdleStart = 0L;
                  } else if(signal == IDLE_SEGMENT_CHECK_SIGNAL) {
                     ConnectionPoolSegment lastActiveSegment = segments[activeSegments - 1];
                     long currentTimeMillis = System.currentTimeMillis();
                     if(lastActiveSegment.isIdle()) {
                        if(lastSegmentIdleStart == 0L) {
                           lastSegmentIdleStart = currentTimeMillis;
                        } else if((currentTimeMillis - lastSegmentIdleStart) > lastActiveSegment.idleTimeBeforeShutdownMillis &&
                                (currentTimeMillis - lastActivateTime) > lastActiveSegment.minActiveTimeMillis) {
                           segmentSignalQueue.offer(DEACTIVATE_SEGMENT_SIGNAL);
                        }
                     } else {
                        lastSegmentIdleStart = 0L;
                     }
                  }
               }
            }
         } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
         } catch(Throwable t) {
            logError("Unable to perform segment operation", t);
         }
      }
   }

   /**
    * Gets the name of the pool.
    * @return The pool name.
    */
   public String getName() {
      return name;
   }

   /**
    * Gets an unmodifiable set of alias names for this pool.
    * @return The set of names.
    */
   public Set<String> getAKA() {
      return Collections.unmodifiableSet(aka);
   }

   /**
    * Gets the current number of active segments.
    * @return The number of active segments.
    */
   public int getActiveSegments() {
      return activeSegments;
   }

   /**
    * Gets the total number of segments (active + reserve).
    * @return The total number of segments.
    */
   public int getTotalSegments() {
      return segments.length;
   }

   /**
    * Gets the stats sampler, if any.
    * @return The sampler.
    */
   public StatsSampler getStatsSampler() {
      return statsSampler;
   }

   /**
    * Gets the minimum number of active segments.
    * @return The minimum number of active segments.
    */
   public int getMinActiveSegments() {
      return minActiveSegments;
   }

   /**
    * Gets the maximum number of connections.
    * @return The maximum number of connections.
    */
   private int getMaxConnections() {
      return maxConnections;
   }

   /**
    * Gets the number of connections that are currently active.
    * @return The number of active connections.
    */
   public int getActiveConnections() {
      int count = 0;
      for(ConnectionPoolSegment segment : segments) {
         count += segment.getActiveConnectionCount();
      }
      return count;
   }

   /**
    * Gets the number of connections that are currently available.
    * @return The number of available.
    */
   public int getAvailableConnections() {

      int count = 0;
      for(ConnectionPoolSegment segment : segments) {
         count += segment.getAvailableConnectionCount();
      }
      return count;
   }

   /**
    * Is the pool currently idle?
    * @return Is the pool idle.
    */
   public boolean isIdle() {
      for(ConnectionPoolSegment segment : segments) {
         if(!segment.isIdle()) {
            return false;
         }
      }
      return true;
   }

   /**
    * Gets a snapshot of statistics for this pool.
    * @return The statistics.
    */
   public Stats getStats() {
      return new Stats(this);
   }

   /**
    * Gets the metrics for this pool.
    * @return The metrics.
    */
   public MetricSet getMetrics() {
      return metrics;
   }

   /**
    * Registers all metrics for this cache with the prefix 'org.attribyte.sql.pool.ConnectionPool.[pool name]'.
    * @param registry The registry to which metrics are added.
    * @return The input registry.
    */
   public MetricRegistry registerMetrics(final MetricRegistry registry) {
      registry.register(MetricRegistry.name(ConnectionPool.class, name), metrics);
      return registry;
   }

   /**
    * The logger.
    */
   private final Logger logger;

   /**
    * The pool name.
    */
   private final String name;

   /**
    * A set of alternative names for this pool.
    */
   private final Set<String> aka = Sets.newHashSet();

   /**
    * Minimum number of active segments.
    */
   private final int minActiveSegments;

   /**
    * Method used for testing to access segments.
    * @return The segments.
    */
   final ConnectionPoolSegment[] getSegmentsForTest() {
      return segments;
   }

   /**
    * All segments, in order of use.
    */
   private final ConnectionPoolSegment[] segments;

   /**
    * The current number of active segments.
    */
   private volatile int activeSegments; //Note: Set only by segment signal monitor.

   /**
    * Periodically queues an idle segment check.
    */
   private final ScheduledExecutorService idleSegmentMonitorService;

   /**
    * Periodically samples pool statistics.
    */
   private final ScheduledExecutorService statsSamplerService;

   /**
    * The stats sampler, if any.
    */
   private final StatsSampler statsSampler;

   /**
    * Service used by segments to check for inactive, logically open, connections.
    */
   private final ScheduledThreadPoolExecutor inactiveMonitorService;

   /**
    * Monitors the segment state change queue.
    */
   private final Thread segmentSignalMonitorThread;

   /**
    * Queue for segment signals.
    */
   private final ArrayBlockingQueue<Object> segmentSignalQueue;

   /**
    * Measures acquisition attempts.
    */
   private final Timer acquisitions;

   /**
    * Measures the rate of failed acquisition attempts.
    */
   private final Meter failedAcquisitions;

   /**
    * The current number of active segments.
    */
   private final Gauge<Integer> activeSegmentsGauge;

   /**
    * The current count of segment expansions.
    */
   private final Gauge<Long> segmentExpansionGauge;

   /**
    * The fraction of active connections currently used.
    */
   private final Gauge<Float> activeConnectionUtilizationGauge;


   /**
    * The fraction of available connections currently used.
    */
   private final Gauge<Float> availableConnectionUtilizationGauge;

   /**
    * The immutable set of metrics.
    */
   private final MetricSet metrics;

   /**
    * Total number of times this pool has expanded by adding a segment.
    */
   private final AtomicLong segmentExpansions = new AtomicLong(0);

   /**
    * Current number of unmanaged connections in use.
    */
   private final Counter activeUnmanagedConnections;

   /**
    * Offered to signal queue to indicate segment should be activated, if available.
    */
   private static final Object ACTIVATE_SEGMENT_SIGNAL = new Object();

   /**
    * Offered to signal queue to indicate segment should be deactivated, if possible.
    */
   private static final Object DEACTIVATE_SEGMENT_SIGNAL = new Object();

   /**
    * Offered to signal queue to indicate last-activated segment should be checked for idle.
    */
   private static final Object IDLE_SEGMENT_CHECK_SIGNAL = new Object();

   /**
    * The exception message returned when pool is saturated.
    */
   private final String SATURATED_MESSAGE;

   /**
    * The maximum amount of time to wait for a connection to become available when pool is saturated.
    */
   final long saturatedAcquireTimeoutMillis;

   /**
    * A description of the configured connections.
    */
   final String connectionDescription;

   /**
    * The maximum number of connections.
    */
   final int maxConnections;

   /**
    * Logs an info message.
    * @param message The message.
    */
   private void logInfo(final String message) {
      if(logger != null) {
         StringBuilder buf = new StringBuilder(name);
         buf.append(":");
         buf.append(message);
         logger.info(buf.toString());
      }
   }

   /**
    * Logs an error message.
    * @param message The message.
    * @param t A <tt>Throwable</tt>.
    */
   private void logError(final String message, final Throwable t) {
      if(logger != null) {
         StringBuilder buf = new StringBuilder(name);
         buf.append(":");
         buf.append(message);
         logger.error(buf.toString(), t);
      }
   }

   /**
    * Throws an initialization exception after logging.
    * @param message The message.
    * @throws InitializationException The exception.
    */
   private void throwInitException(final String message) throws InitializationException {
      logError(message, null);
      throw new InitializationException(message);
   }

   /**
    * Gets a description of the (real) database connection.
    * @return The connection description.
    */
   private String buildConnectionDescription() {

      List<String> descList = Lists.newArrayListWithExpectedSize(4);
      for(ConnectionPoolSegment segment : segments) {
         String desc = segment.dbConnection.getConnectionDescription();
         if(!descList.contains(desc)) {
            descList.add(desc);
         }
      }

      return Joiner.on(',').join(descList);
   }
}