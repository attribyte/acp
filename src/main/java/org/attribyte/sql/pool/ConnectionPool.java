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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.attribyte.api.InitializationException;
import org.attribyte.api.Logger;
import org.attribyte.essem.metrics.Timer;
import org.attribyte.sql.ConnectionSupplier;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides logical database connections to an application from
 * a pool of physical connections that are maintained by the pool.
 *
 * Connection pools are composed of segments that are used, activated and deactivated in sequence in
 * response to connection demand. When active, a segment provides logical connections
 * from a fixed-size pool of physical connections.
 *
 * Connection pools may be created using XML or property-based configuration, as well as programmatically using an
 * {@code Initializer}.
 *
 * <h3>Configuration Parameters</h3>
 * <dl>
 * <dt>name</dt>
 * <dd>The pool name. Required.</dd>
 * <dt>minActiveSegments</dt>
 * <dd>The minimum number of active segments. Default {@code 1}.</dd>
 * <dt>saturatedAcquireTimeout</dt>
 * <dd>The maximum amount of time to block waiting for an available connection. Default {@code 0ms}.</dd>
 * <dt>idleCheckInterval</dt>
 * <dd>The time between idle segment checks. Default {@code 60s}.</dd>
 * <dt>segments</dt>
 * <dd>The number of segments. Default {@code 1}.</dd>
 * <dt>activeSegments</dt>
 * <dd>The number of segments active on start. Default {@code 1}.</dd>
 * <dt>connection.user</dt>
 * <dd>The database user.</dd>
 * <dt>connection.password</dt>
 * <dd>The database password.</dd>
 * <dt>connection.url</dt>
 * <dd>The database connection string.</dd>
 * <dt>connection.testSQL</dt>
 * <dd>SQL used for connection tests.</dd>
 * <dt>connection.debug</dt>
 * <dd>Is connection debug mode turned on?</dd>
 * <dt>connection.testInterval</dt>
 * <dd>The interval between connection tests. Default {@code 60s}.</dd>
 * <dt>connection.maxWait</dt>
 * <dd>The maximum amount of time to wait for a database connection before giving up. Default {@code 0s}.</dd>
 * <dt>segment.size</dt>
 * <dd>The number of connections in each segment. Required.</dd>
 * <dt>segment.closeConcurrency</dt>
 * <dd>The number of background threads processing connection close. If {@code 0},
 * close blocks in the application thread. Default {@code 0}.</dd>
 * <dt>segment.reconnectConcurrency</dt>
 * <dd>The maximum number of concurrent database reconnects. Default {@code 1}.</dd>
 * <dt>segment.testOnLogicalOpen</dt>
 * <dd>Should connections be tested when they are acquired? Default {@code false}.</dd>
 * <dt>segment.testOnLogicalClose</dt>
 * <dd>Should connections be tested when they are released? Default {@code false}.</dd>
 * <dt>segment.acquireTimeout</dt>
 * <dd>The maximum amount of time to wait for a segment connection to become available. Default {@code 0ms}.</dd>
 * <dt>segment.activeTimeout</dt>
 * <dd>The maximum amount of time a logical connection may be open before it is forcibly closed. Default {@code 5m}.</dd>
 * <dt>segment.connectionLifetime</dt>
 * <dd>The maximum amount of time a physical connection may be open. Default {@code 1h}.</dd>
 * <dt>segment.maxReconnectWait</dt>
 * <dd>The maximum amount of time to wait between physical connection attempts on failure. Default {@code 30s}.</dd>
 * </dl>
 *
 * <h3>Sample Properties Configuration File</h3>
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
 * @author Matt Hamer - Attribyte, LLC
 */
public class ConnectionPool implements ConnectionSupplier {

   /**
    * Pool statistics.
    */
   public static class Stats {

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
       * Create an {@code Initializer} for each configured pool.
       * @param config The configuration.
       * @param passwordSource The password source. May be {@code null}.
       * @param logger The logger. May be {@code null}.
       * @return The list of {@code Initializers}.
       * @throws InitializationException on invalid configuration.
       */
      public static final List<ConnectionPool.Initializer> fromConfig(final Config config,
                                                                      final PasswordSource passwordSource,
                                                                      final Logger logger) throws InitializationException {
         return TypesafeConfig.poolsFromConfig(config, passwordSource, logger);
      }

      /**
       * Parse a config file to create an {@code Initializer} for each configured pool.
       * <p>
       * File format is <a href="https://github.com/typesafehub/config/blob/master/HOCON.md">HOCON</a>.
       * Configuration must appear in the path: {@code acp}
       * </p>
       * @param configFile The config file.
       * @param passwordSource The password source. May be {@code null}.
       * @param logger The logger. May be {@code null}.
       * @return The list of {@code Initializers}.
       * @throws InitializationException on invalid file or configuration.
       */
      public static final List<ConnectionPool.Initializer> fromConfigFile(final File configFile,
                                                                          final PasswordSource passwordSource,
                                                                          final Logger logger) throws InitializationException {
         Config rootConfig = ConfigFactory.parseFile(configFile);
         return fromConfig(rootConfig.getConfig("acp"), passwordSource, logger);
      }

      /**
       * Parse properties to create an {@code Initializer} for each configured pool.
       * @param props The properties.
       * @param passwordSource The password source. May be {@code null}.
       * @param logger The logger. May be {@code null}.
       * @return The list of {@code Initializers}.
       * @throws InitializationException on invalid configuration.
       */
      public static final List<Initializer> fromProperties(final Properties props,
                                                           final PasswordSource passwordSource,
                                                           final Logger logger) throws InitializationException {
         Config rootConfig = ConfigFactory.parseProperties(props);
         return fromConfig(rootConfig, passwordSource, logger);
      }

      /**
       * Parse properties to create an {@code Initializer} for each configured pool.
       * @param propsFile A properties file.
       * @param passwordSource The password source. May be {@code null}.
       * @param logger The logger. May be {@code null}.
       * @return The list of {@code Initializers}.
       * @throws InitializationException on invalid file or configuration.
       */
      public static final List<ConnectionPool.Initializer> fromPropertiesFile(final File propsFile,
                                                                              final PasswordSource passwordSource,
                                                                              final Logger logger) throws InitializationException {
         Properties props = new Properties();
         FileInputStream fis = null;
         try {
            try {
               fis = new FileInputStream(propsFile);
               props.load(fis);
            } finally {
               if(fis != null) fis.close();
            }
         } catch(IOException ioe) {
            throw new InitializationException("Problem loading properties", ioe);
         }

         return fromProperties(props, passwordSource, logger);
      }

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
       * Sets the minimum delay between segment expansions in milliseconds.
       * @param minSegmentExpansionDelayMillis The delay.
       * @return A self-reference.
       */
      public Initializer setMinSegmentExpansionDelay(final long minSegmentExpansionDelayMillis) {
         this.minSegmentExpansionDelayMillis = minSegmentExpansionDelayMillis;
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
       * @throws InitializationException if pool was improperly configured.
       * @throws SQLException if a connection exception is raised during create.
       */
      public ConnectionPool createPool() throws InitializationException, SQLException {

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

         ConnectionPool pool = new ConnectionPool(name, aka, activeSegments, reserveSegments, saturatedAcquireTimeoutMillis,
                 minActiveSegments, idleCheckIntervalMillis, minSegmentExpansionDelayMillis, logger);

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

      String name;
      final Set<String> aka = Sets.newHashSet();
      final List<ConnectionPoolSegment> activeSegments = Lists.newArrayListWithExpectedSize(4);
      final List<ConnectionPoolSegment> reserveSegments = Lists.newArrayListWithExpectedSize(4);
      int minActiveSegments;
      long minSegmentExpansionDelayMillis = 1000L;
      long saturatedAcquireTimeoutMillis;
      long idleCheckIntervalMillis = 60000L;
      Logger logger;
   } //Initializer

   /**
    * Creates an empty initializer.
    * @return The initializer.
    */
   public static Initializer newInitializer() {
      return new Initializer();
   }

   /**
    * Creates a pool.
    * <p>
    * A segment object may not be added to the active or reserve
    * list more than once.
    * </p>
    * @param name The pool name.
    * @param aka A collection of alias names for this pool.
    * @param activeSegments The list of active segments. Must be > 0.
    * @param reserveSegments The list of reserve segments. May be empty.
    * @param saturatedAcquireTimeoutMillis The timeout before failure when pool is saturated (all connections busy).
    * @param minActiveSegments The minimum number of active segments.
    * @param idleCheckIntervalMillis The frequency of idle pool checks.
    * @param minSegmentExpansionDelayMillis The minimum amount of time that must elapse between segment expansion.
    * @param logger The logger.
    * @throws InitializationException if pool could not be created.
    */
   private ConnectionPool(final String name,
                          final Collection<String> aka,
                          final List<ConnectionPoolSegment> activeSegments,
                          final List<ConnectionPoolSegment> reserveSegments,
                          final long saturatedAcquireTimeoutMillis,
                          final int minActiveSegments,
                          final long idleCheckIntervalMillis,
                          final long minSegmentExpansionDelayMillis,
                          final Logger logger) throws InitializationException {

      this.name = name;
      this.aka = aka != null ? ImmutableSet.copyOf(aka) : ImmutableSet.of();
      this.acquisitions = new Timer();
      this.failedAcquisitions = new Meter();
      this.activeUnmanagedConnections = new Counter();
      this.activeSegmentsGauge = () -> ConnectionPool.this.activeSegments;
      this.segmentExpansionGauge = ConnectionPool.this.segmentExpansions::get;
      this.activeConnectionUtilizationGauge = () -> {
         final int avail = getAvailableConnections();
         final int active = getActiveConnections();
         if(active >= avail) {
            return 1.0f;
         } else {
            return (float)(active) / (float)avail;
         }
      };
      this.availableConnectionUtilizationGauge = () -> {
         final int max = getMaxConnections();
         final int active = getActiveConnections();
         if(active >= max) {
            return 1.0f;
         } else {
            return (float)active / (float)max;
         }
      };
      this.metrics = () -> {
         ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
         builder.put("acquisitions", acquisitions);
         builder.put("failed-acquisitions", failedAcquisitions);
         builder.put("active-unmanaged-connections", activeUnmanagedConnections);
         builder.put("active-segments", activeSegmentsGauge);
         builder.put("segment-expansions", segmentExpansionGauge);
         builder.put("active-connections-utilized", activeConnectionUtilizationGauge);
         builder.put("available-connections-utilized", availableConnectionUtilizationGauge);
         return builder.build();
      };

      this.SATURATED_MESSAGE = "Connection pool '" + name + "' is saturated";
      this.logger = logger;

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

      if(minActiveSegments > segments.size()) {
         throwInitException("The 'minActiveSegments' must be <= the total number of segments");
      }

      if(minActiveSegments < 1) {
         throwInitException("The 'minActiveSegments' must be >= 1");
      }

      this.minActiveSegments = minActiveSegments;
      this.minSegmentExpansionDelayMillis = minSegmentExpansionDelayMillis;
      this.segments = new ConnectionPoolSegment[maxSegments];
      int pos = 0;
      for(ConnectionPoolSegment segment : segments) {
         this.segments[pos++] = segment;
      }

      this.saturatedAcquireTimeoutMillis = saturatedAcquireTimeoutMillis;

      this.activeSegments = activeSegments.size();

      for(ConnectionPoolSegment segment : activeSegments) {
         try {
            segment.activate();
         } catch(SQLException se) {
            throw new InitializationException("Problem activating segment '" + segment.name + "' in pool '" + name + "'", se);
         }
      }

      if(this.segments.length > 1 && this.minActiveSegments != this.segments.length) {
         segmentSignalQueue = new ArrayBlockingQueue<>(1);
         String signalMonitorThreadName = Strings.isNullOrEmpty(name) ? "SignalMonitor" : (name + ":SignalMonitor");
         segmentSignalMonitorThread = new Thread(new SegmentSignalMonitor(), "ACP:" + signalMonitorThreadName);
         segmentSignalMonitorThread.start();
         idleSegmentMonitorService = MoreExecutors.getExitingScheduledExecutorService(
                 new ScheduledThreadPoolExecutor(1,
                         Util.createThreadFactoryBuilder(name, "IdleMonitor"))
         );
         idleSegmentMonitorService.scheduleWithFixedDelay(new IdleSegmentMonitor(), idleCheckIntervalMillis, idleCheckIntervalMillis, TimeUnit.MILLISECONDS);
      } else {
         segmentSignalQueue = null;
         segmentSignalMonitorThread = null;
         idleSegmentMonitorService = null;
      }

      inactiveMonitorService = MoreExecutors.getExitingScheduledExecutorService(
              new ScheduledThreadPoolExecutor(1,
                      Util.createThreadFactoryBuilder(name, "InactiveMonitor"))
      );

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
    * for the first segment. Caller must call {@code closeUnmanagedConnection}
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
    * Gets a connection future.
    * See <a href="https://code.google.com/p/guava-libraries/wiki/ListenableFutureExplained">Listenable Future - Explained</a>
    * @param executor The executor service used to complete the future.
    * @return The (listenable) connection future.
    */
   public final ListenableFuture<Connection> getFutureConnection(final ListeningExecutorService executor) {
      return executor.submit(this::getConnection);
   }

   /**
    * Signals that a new segment should be activated.
    */
   final void signalActivateSegment() throws SQLException {

      /*
         Note that 'offer' does so only if it can be done
         immediately without exceeding the queue capacity.
         The queue capacity is '1', so this means that
         signals may be ignored. Either way, this message
         will never block.
       */

      if(segmentSignalQueue != null) {
         segmentSignalQueue.offer(ACTIVATE_SEGMENT_SIGNAL);
      } else if(activeSegments == 0) { //Single segment, no monitor, started with the one segment deactivated...
         synchronized(this) {
            if(activeSegments == 0) { //... and DCL works here because activeSegments is volatile.
               segments[0].activate();
               activeSegments = 1;
            }
         }
      }
   }

   /**
    * Shutdown the pool.
    */
   public final void shutdown() {

      if(isShuttingDown.compareAndSet(false, true)) {

         logInfo("Shutting down...");

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

         Clock.shutdown();

         logInfo("Shutting down the time limiter...");
         Util.shutdownTimeLimiter();

         logInfo("Shutdown complete!");
      }
   }

   /**
    * Shutdown the pool without waiting for any in-progress
    * operations to complete.
    */
   public final void shutdownNow() {

      if(isShuttingDown.compareAndSet(false, true)) {

         logInfo("Shutting down...");

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
            segment.shutdownNow();
         }

         logInfo("Shutting down inactive monitor service...");
         inactiveMonitorService.shutdownNow();

         Clock.shutdown();

         logInfo("Shutting down the time limiter...");
         List<Runnable> waiting = Util.shutdownTimeLimiterNow();
         if(!waiting.isEmpty()) {
            logInfo(String.format("Time limiter had %d waiting tasks!", waiting.size()));
         }
         logInfo("Shutdown complete!");
      }
   }

   /**
    * Periodically signals check of the last activated segment to
    * see if it can be shutdown.
    */
   private final class IdleSegmentMonitor implements Runnable {

      @SuppressWarnings("all")
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

      @SuppressWarnings("all")
      public void run() {
         try {
            while(true) {
               Object signal = segmentSignalQueue.take();
               if(signal == ACTIVATE_SEGMENT_SIGNAL && activeSegments < segments.length) {
                  if(lastActivateTime == 0L || System.currentTimeMillis() - lastActivateTime > minSegmentExpansionDelayMillis) {
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
                  } //else ignore this signal
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
   public ImmutableSet<String> getAKA() {
      return aka;
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
   private final ImmutableSet<String> aka;

   /**
    * Minimum number of active segments.
    */
   private final int minActiveSegments;

   /**
    * The minimum amount of time that must elapse between segment expansion.
    */
   private final long minSegmentExpansionDelayMillis;

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
    * Service used by segments to check for inactive, logically open, connections.
    */
   private final ScheduledExecutorService inactiveMonitorService;

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
    * Disallow concurrent shutdown.
    */
   private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

   /**
    * Logs an info message.
    * @param message The message.
    */
   private void logInfo(final String message) {
      if(logger != null) {
         logger.info(name + ":" + message);
      }
   }

   /**
    * Logs an error message.
    * @param message The message.
    * @param t A {@code Throwable}.
    */
   private void logError(final String message, final Throwable t) {
      if(logger != null) {
         logger.error(name + ":" + message, t);
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

   /**
    * Sets the log writer for any segments that are configured
    * with a {@code DataSource} for connections.
    * @param writer The writer.
    */
   void setDataSourceLogWriter(PrintWriter writer) throws SQLException {
      for(ConnectionPoolSegment segment : segments) {
         if(segment.dbConnection.datasource != null) {
            segment.dbConnection.datasource.setLogWriter(writer);
         }
      }
   }

   /**
    * Sets the login timeout for any segments that are configured
    * with a {@code DataSource} for connections.
    * @param seconds The timeout in seconds.
    */
   void setDataSourceLoginTimeout(int seconds) throws SQLException {
      for(ConnectionPoolSegment segment : segments) {
         if(segment.dbConnection.datasource != null) {
            segment.dbConnection.datasource.setLoginTimeout(seconds);
         }
      }
   }
}
