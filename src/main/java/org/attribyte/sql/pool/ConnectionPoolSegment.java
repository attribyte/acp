/*
 * Copyright 2010 Attribyte, LLC 
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.w3c.dom.Element;
import org.attribyte.api.InitializationException;
import org.attribyte.api.ConsoleLogger;
import org.attribyte.api.Logger;
import org.attribyte.util.DOMUtil;
import org.attribyte.util.StringUtil;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;


/**
 * A collection of connections that are managed as a single unit as part of a
 * pool.
 * <p>
 * A segment contains a fixed number of physical database connections in one of the following states: available, open,
 * closing, reopening, or disconnected.
 * </p>
 * @author Matt Hamer - Attribyte, LLC
 */
public class ConnectionPoolSegment {

   /**
    * Statistics for this segment.
    */
   public class Stats {

      private Stats() {
      }

      /**
       * Gets a snapshot of the current stats.
       * @return The stats.
       */
      public Stats getSnapshot() {
         return new Stats(this);
      }

      /**
       * Creates a copy.
       * @param other The stats to copy.
       */
      private Stats(Stats other) {
         connectionCount.set(other.connectionCount.get());
         connectionErrorCount.set(other.connectionErrorCount.get());
         activeTimeoutCount.set(other.activeTimeoutCount.get());
         lastActivatedTime = other.lastActivatedTime;
         cumulativeActiveTimeMillis = other.cumulativeActiveTimeMillis;
         lastDeactivateTime = other.lastDeactivateTime;
         active = other.active;
      }

      /**
       * Gets the name of the segment.
       * @return The segment name.
       */
      public String getSegmentName() {
         return name;
      }

      /**
       * Gets the connection count.
       * @return The connection count.
       */
      public long getConnectionCount() {
         return connectionCount.get();
      }

      /**
       * Total number of connections served since startup.
       */
      private final AtomicLong connectionCount = new AtomicLong(0);

      /**
       * Gets the count of failed connection errors.
       * @return The connection error count.
       */
      public long getFailedConnectionErrorCount() {
         return connectionErrorCount.get();
      }

      /**
       * Total number of errors raised while creating connections.
       */
      private final AtomicLong connectionErrorCount = new AtomicLong(0);

      /**
       * Gets the count of connections closed when active limit is reached.
       * @return The active timeout count.
       */
      public long getActiveTimeoutCount() {
         return activeTimeoutCount.get();
      }

      /**
       * Total number of connections closed automatically when active too long.
       */
      private final AtomicLong activeTimeoutCount = new AtomicLong(0);

      /**
       * Gets the time this segment was last activated.
       * @return The last activated time.
       */
      public long getLastActivatedTime() {
         return lastActivatedTime;
      }

      /**
       * The time this segment was last activated.
       */
      private volatile long lastActivatedTime = 0L;

      /**
       * Gets the cumulative time this segment has been active.
       * @return The cumulative active time.
       */
      public long getCumulativeActiveTimeMillis() {
         return cumulativeActiveTimeMillis;
      }

      /**
       * The total time this segment has been active since startup.
       */
      private volatile long cumulativeActiveTimeMillis = 0L;

      /**
       * Gets the last time this segment was deactivated.
       * @return The last deactivate time.
       */
      public long getLastDeactivateTime() {
         return lastDeactivateTime;
      }

      /**
       * The time this segment was last deactivated.
       */
      private volatile long lastDeactivateTime = 0L;

      /**
       * Determines if this segment is currently active.
       * @return Is the segment active?
       */
      public boolean isActive() {
         return active;
      }

      /**
       * Is the pool currently active?
       */
      private volatile boolean active = false;

      /**
       * Gets the fraction of the total uptime this segment has been active.
       * @return The fraction.
       */
      public final double getUptimeActiveFraction() {

         if(active && cumulativeActiveTimeMillis == 0L) { //Active and never deactivated.
            return 1.0;
         } else {
            long totalTimeMillis = System.currentTimeMillis() - createTime;
            return (double)cumulativeActiveTimeMillis / (double)totalTimeMillis;
         }
      }

      /**
       * Gets the transactions (number of connections opened) per second when the segment is active.
       * @return The transactions per second.
       */
      public final double getActiveTPS() {

         long totalTimeSeconds = 0L;
         if(active && cumulativeActiveTimeMillis == 0) { //Active and never deactivated.
            totalTimeSeconds = (System.currentTimeMillis() - createTime) / 1000L;
         } else if(cumulativeActiveTimeMillis > 0) {
            totalTimeSeconds = cumulativeActiveTimeMillis / 1000L;
         }

         if(totalTimeSeconds == 0L) {
            totalTimeSeconds = 1L;
         }

         return (double)connectionCount.get() / (double)totalTimeSeconds;
      }

      /**
       * Gets the transactions (number of connections opened) per second since the pool was opened.
       * <p>
       * Includes time that segment is disabled.
       * </p>
       * @return The transactions per second.
       */
      public final double getAverageTPS() {
         long totalTimeSeconds = (System.currentTimeMillis() - createTime) / 1000L;
         if(totalTimeSeconds == 0L) {
            totalTimeSeconds = 1L;
         }
         return (double)connectionCount.get() / (double)totalTimeSeconds;
      }

      /**
       * Record activate.
       */
      private void activate() {
         active = true;
         lastActivatedTime = System.currentTimeMillis();
      }

      /**
       * Record deactivate.
       */
      private void deactivate() {
         active = false;
         long currTime = System.currentTimeMillis();
         lastDeactivateTime = currTime;
         long activeTimeMillis = currTime - lastActivatedTime;
         cumulativeActiveTimeMillis += activeTimeMillis;
      }
   }

   /**
    * Initialize and create a segment.
    */
   public static class Initializer {

      /**
       * Sets the name of the pool.
       * @param name The pool name.
       * @return A self-reference.
       */
      public Initializer setName(final String name) {
         this.name = name;
         return this;
      }

      /**
       * Sets the logger for this segment.
       * @param logger The logger.
       * @return A self-reference.
       */
      public Initializer setLogger(final Logger logger) {
         this.logger = logger;
         return this;
      }

      /**
       * Sets the number of connections in the segment.
       * @param size The segment size.
       * @return A self-reference.
       */
      public Initializer setSize(final int size) {
         this.size = size;
         return this;
      }

      /**
       * Sets the maximum amount of time to wait for a connection if all are busy.
       * @param timeout The timeout value.
       * @param timeoutUnit The timeout units.
       * @return A self-reference.
       */
      public Initializer setAcquireTimeout(final long timeout, final TimeUnit timeoutUnit) {
         this.acquireTimeoutMillis = TimeUnit.MILLISECONDS.convert(timeout, timeoutUnit);
         return this;
      }

      /**
       * Sets the maximum amount of time a connection may be active before
       * automatically closed.
       * @param timeout The timeout value.
       * @param timeoutUnit The timeout units.
       * @return A self-reference.
       */
      public Initializer setActiveTimeout(final long timeout, final TimeUnit timeoutUnit) {
         this.activeTimeoutMillis = TimeUnit.MILLISECONDS.convert(timeout, timeoutUnit);
         return this;
      }

      /**
       * Sets the frequency the active timeout monitor runs.
       * @param timeout The frequency value.
       * @param timeoutUnit The frequency units.
       * @return A self-reference.
       */
      public Initializer setActiveTimeoutMonitorFrequency(final long timeout, final TimeUnit timeoutUnit) {
         this.activeTimeoutMonitorFrequencySeconds = TimeUnit.SECONDS.convert(timeout, timeoutUnit);
         return this;
      }

      /**
       * Sets the lifetime of (real) connections before automatic close/reopen.
       * @param connectionLife The connection life.
       * @param connectionLifeUnit The connection life units.
       * @return A self-reference.
       */
      public Initializer setConnectionLifetime(final long connectionLife, final TimeUnit connectionLifeUnit) {
         this.connectionLifetimeMillis = TimeUnit.MILLISECONDS.convert(connectionLife, connectionLifeUnit);
         return this;
      }

      /**
       * Sets the database connection.
       * @param jdbcConnection The connection.
       * @return A self-reference.
       */
      public Initializer setConnection(final JDBConnection jdbcConnection) {
         this.jdbcConnection = jdbcConnection;
         return this;
      }

      /**
       * Sets the number of threads handling connection close.
       * Default is <tt>0</tt>.
       * <p>
       * If concurrency is <tt>0</tt> (logical) close operations will be performed (and block)
       * in the calling thread. Higher concurrency allows close to return immediately
       * after queuing the connection, with this number of threads monitoring the queue.
       * </p>
       * @param closerConcurrency The number of threads.
       * @return A self-reference.
       */
      public Initializer setCloseConcurrency(final int closerConcurrency) {
         this.numCloserThreads = closerConcurrency;
         return this;
      }

      /**
       * Sets the maximum number of simultaneous database reconnect attempts.
       * @param maxConcurrentReconnects The maximum number of concurrent reconnects.
       * @return A self-reference.
       */
      public Initializer setMaxConcurrentReconnects(final int maxConcurrentReconnects) {
         this.maxConcurrentReconnects = maxConcurrentReconnects;
         return this;
      }

      /**
       * Sets the maximum delay (after failure) between reconnect attempts.
       * @param time The time.
       * @param timeUnit The time unit.
       * @return A self-reference.
       */
      public Initializer setMaxReconnectDelay(final long time, final TimeUnit timeUnit) {
         this.maxReconnectDelayMillis = TimeUnit.MILLISECONDS.convert(time, timeUnit);
         return this;
      }

      /**
       * Sets segment to test connections when they are logically opened.
       * @param testOnLogicalOpen Should connections be tested on logical open?.
       * @return A self-reference.
       */
      public Initializer setTestOnLogicalOpen(final boolean testOnLogicalOpen) {
         this.testOnLogicalOpen = testOnLogicalOpen;
         return this;
      }

      /**
       * Sets segment to test connections when they are logically closed.
       * @param testOnLogicalClose Should connections be tested on logical close?.
       * @return A self-reference.
       */
      public Initializer setTestOnLogicalClose(final boolean testOnLogicalClose) {
         this.testOnLogicalClose = testOnLogicalClose;
         return this;
      }

      /**
       * Sets the time this segment must be idle before it is shutdown.
       * @param time The time.
       * @param timeUnit The time unit.
       * @return A self-reference.
       */
      public Initializer setIdleTimeBeforeShutdown(final long time, final TimeUnit timeUnit) {
         this.idleTimeBeforeShutdownMillis = TimeUnit.MILLISECONDS.convert(time, timeUnit);
         return this;
      }

      /**
       * Sets the minimum time this segment must be active before it is eligible for shutdown.
       * @param time The time.
       * @param timeUnit The time unit.
       * @return A self-reference.
       */
      public Initializer setMinActiveTime(final long time, final TimeUnit timeUnit) {
         this.minActiveTimeMillis = TimeUnit.MILLISECONDS.convert(time, timeUnit);
         return this;
      }

      /**
       * Sets a password source for this segment.
       * @param passwordSource The password source.
       * @return A self-reference.
       */
      public Initializer setPasswordSource(final PasswordSource passwordSource) {
         this.passwordSource = passwordSource;
         return this;
      }

      /**
       * Creates a segment after configuration.
       * @return The segment.
       * @throws InitializationException if configuration is invalid.
       */
      public ConnectionPoolSegment createSegment() throws InitializationException {

         validate(false);

         return new ConnectionPoolSegment(name, size,
                 acquireTimeoutMillis, activeTimeoutMillis, activeTimeoutMonitorFrequencySeconds,
                 connectionLifetimeMillis,
                 idleTimeBeforeShutdownMillis,
                 minActiveTimeMillis,
                 jdbcConnection,
                 numCloserThreads,
                 maxConcurrentReconnects, maxReconnectDelayMillis,
                 testOnLogicalOpen, testOnLogicalClose,
                 incompleteTransactionPolicy,
                 openStatementPolicy,
                 forceRealClosePolicy,
                 passwordSource, logger, closeTimeLimitMillis);
      }

      /**
       * Creates an initializer from XML configuration.
       * @param poolName The pool name.
       * @param elem The DOM element.
       * @param connectionMap A map of connection vs. name.
       * @return The initializer.
       * @throws InitializationException if configuration is invalid.
       */
      public static final Initializer fromXML(String poolName, Element elem, Map<String, JDBConnection> connectionMap) throws InitializationException {
         Initializer initializer = new Initializer();
         return fromXML(poolName, initializer, elem, connectionMap);
      }

      /**
       * Sets initializer properties from XML configuration.
       * @param poolName The pool name.
       * @param initializer An initializer.
       * @param elem The DOM element.
       * @param connectionMap A map of connection vs. name.
       * @return The initializer.
       * @throws InitializationException if configuration is invalid.
       */
      public static final Initializer fromXML(String poolName, Initializer initializer, Element elem, Map<String, JDBConnection> connectionMap) throws InitializationException {

         initializer.name = elem.getAttribute("name");
         if(poolName != null) {
            initializer.name = poolName + ":" + initializer.name;
         }

         String size = elem.getAttribute("size");
         if(size.length() > 0) {
            initializer.size = Integer.parseInt(size);
         }

         String numClosers = elem.getAttribute("concurrency");
         if(numClosers.length() > 0) {
            initializer.numCloserThreads = Integer.parseInt(numClosers);
         }

         String testOnLogicalOpen = elem.getAttribute("testOnLogicalOpen");
         if(testOnLogicalOpen.length() > 0) {
            initializer.testOnLogicalOpen = testOnLogicalOpen.equalsIgnoreCase("true");
         }

         String testOnLogicalClose = elem.getAttribute("testOnLogicalClose");
         if(testOnLogicalClose.length() > 0) {
            initializer.testOnLogicalClose = testOnLogicalClose.equalsIgnoreCase("true");
         }

         String incompleteTransactionPolicy = elem.getAttribute("incompleteTransactionPolicy");
         initializer.incompleteTransactionPolicy =
                 ConnectionPoolConnection.IncompleteTransactionPolicy.fromString(incompleteTransactionPolicy, ConnectionPoolConnection.IncompleteTransactionPolicy.REPORT);

         String openStatementPolicy = elem.getAttribute("openStatementPolicy");
         initializer.openStatementPolicy = ConnectionPoolConnection.OpenStatementPolicy.fromString(openStatementPolicy, ConnectionPoolConnection.OpenStatementPolicy.SILENT);

         String forceRealClosePolicy = elem.getAttribute("forceRealClosePolicy");
         initializer.forceRealClosePolicy = ConnectionPoolConnection.ForceRealClosePolicy.fromString(forceRealClosePolicy, ConnectionPoolConnection.ForceRealClosePolicy.CONNECTION);
         String closeTimeLimitMillis = elem.getAttribute("closeTimeLimitMillis");
         if(StringUtil.hasContent(closeTimeLimitMillis)) {
            initializer.closeTimeLimitMillis = Long.parseLong(closeTimeLimitMillis);
         }

         Element connectionElem = DOMUtil.getFirstChild(elem, "connection");
         if(connectionElem != null) {
            String connectionName = connectionElem.getAttribute("name");
            if(connectionName.length() == 0) {
               initializer.jdbcConnection = new JDBConnection(connectionElem);
            } else if(connectionMap != null) {
               JDBConnection connection = connectionMap.get(connectionName);
               if(connection != null) {
                  initializer.jdbcConnection = connection;
               } else {
                  throw new InitializationException("No connection defined with name '" + connectionName + "'");
               }
            } else {
               throw new InitializationException("No connection defined with name '" + connectionName + "'");
            }

            long acquireTimeoutMillis = Util.millisFromElem(connectionElem, "acquireTimeout", Long.MIN_VALUE);
            if(acquireTimeoutMillis != Long.MIN_VALUE) {
               initializer.acquireTimeoutMillis = acquireTimeoutMillis;
            }

            long activeTimeoutMillis = Util.millisFromElem(connectionElem, "activeTimeout", Long.MIN_VALUE);
            if(activeTimeoutMillis != Long.MIN_VALUE) {
               initializer.activeTimeoutMillis = activeTimeoutMillis;
               if(initializer.activeTimeoutMillis < 1L) {
                  throw new InitializationException("The 'time' for 'activeTimeout' must be > 0");
               }
            }

            long connectionLifetimeMillis = Util.millisFromElem(connectionElem, "lifetime", Long.MIN_VALUE);
            if(connectionLifetimeMillis != Long.MIN_VALUE) {
               initializer.connectionLifetimeMillis = connectionLifetimeMillis;
               if(initializer.connectionLifetimeMillis < 1L) {
                  throw new InitializationException("The 'time' for 'lifetime' must be > 0");
               }
            }

         } else { //Allow overrides without connection element.

            long acquireTimeoutMillis = Util.millisFromElem(elem, "acquireTimeout", Long.MIN_VALUE);
            if(acquireTimeoutMillis != Long.MIN_VALUE) {
               initializer.acquireTimeoutMillis = acquireTimeoutMillis;
            }

            long activeTimeoutMillis = Util.millisFromElem(elem, "activeTimeout", Long.MIN_VALUE);
            if(activeTimeoutMillis != Long.MIN_VALUE) {
               initializer.activeTimeoutMillis = activeTimeoutMillis;
               if(initializer.activeTimeoutMillis < 1L) {
                  throw new InitializationException("The 'time' for 'activeTimeout' must be > 0");
               }
            }

            long connectionLifetimeMillis = Util.millisFromElem(elem, "lifetime", Long.MIN_VALUE);
            if(connectionLifetimeMillis != Long.MIN_VALUE) {
               initializer.connectionLifetimeMillis = connectionLifetimeMillis;
               if(initializer.connectionLifetimeMillis < 1L) {
                  throw new InitializationException("The 'time' for 'lifetime' must be > 0");
               }
            }
         }

         Element reconnectElem = DOMUtil.getFirstChild(elem, "reconnect");
         if(reconnectElem != null) {

            String maxConcurrentReconnects = reconnectElem.getAttribute("concurrency");
            if(maxConcurrentReconnects.length() > 0) {
               initializer.maxConcurrentReconnects = Integer.parseInt(maxConcurrentReconnects);
            }

            if(initializer.maxConcurrentReconnects < 1) {
               throw new InitializationException("The 'concurrency' for 'reconnect' must be > 0");
            }

            initializer.maxReconnectDelayMillis = Util.millisFromElem(elem, "maxWaitTime", "reconnect", 0L);
            if(initializer.maxReconnectDelayMillis < 1L) {
               throw new InitializationException("The 'time' for 'reconnect' must be > 0");
            }
         }

         long activeTimeoutMonitorFrequencyMillis = Util.millisFromElem(elem, "frequency", "activeTimeoutMonitor", Long.MIN_VALUE);
         if(activeTimeoutMonitorFrequencyMillis != Long.MIN_VALUE) {
            initializer.activeTimeoutMonitorFrequencySeconds = activeTimeoutMonitorFrequencyMillis / 1000L;
            if(initializer.activeTimeoutMonitorFrequencySeconds < 1L) {
               throw new InitializationException("The 'frequency' for 'activeTimeoutMonitor' must be > 0");
            }
         }

         return initializer;
      }

      /**
       * Sets initializer properties from properties.
       * @param poolName The pool name.
       * @param initializer An initializer.
       * @param props The properties.
       * @param connectionMap A map of connection vs. name.
       * @return The initializer.
       * @throws InitializationException if configuration is invalid.
       */
      public static final Initializer fromProperties(String poolName, Initializer initializer,
                                                     Properties props, Map<String, JDBConnection> connectionMap) throws InitializationException {

         initializer.name = props.getProperty("name");

         if(!StringUtil.hasContent(initializer.name)) {
            throw new InitializationException("Segments must have a 'name'");
         }

         if(poolName != null) {
            initializer.name = poolName + ":" + initializer.name;
         }

         String size = props.getProperty("size", "").trim();
         if(size.length() > 0) {
            initializer.size = Integer.parseInt(size);
         }

         String numClosers = props.getProperty("closeConcurrency", "").trim();
         if(numClosers.length() > 0) {
            initializer.numCloserThreads = Integer.parseInt(numClosers);
         }

         String testOnLogicalOpen = props.getProperty("testOnLogicalOpen", "").trim();
         if(testOnLogicalOpen.length() > 0) {
            initializer.testOnLogicalOpen = testOnLogicalOpen.equalsIgnoreCase("true");
         }

         String testOnLogicalClose = props.getProperty("testOnLogicalClose", "").trim();
         if(testOnLogicalClose.length() > 0) {
            initializer.testOnLogicalClose = testOnLogicalClose.equalsIgnoreCase("true");
         }

         String incompleteTransactionPolicy = props.getProperty("incompleteTransactionPolicy");
         initializer.incompleteTransactionPolicy =
                 ConnectionPoolConnection.IncompleteTransactionPolicy.fromString(incompleteTransactionPolicy, ConnectionPoolConnection.IncompleteTransactionPolicy.REPORT);

         String openStatementPolicy = props.getProperty("openStatementPolicy");
         initializer.openStatementPolicy = ConnectionPoolConnection.OpenStatementPolicy.fromString(openStatementPolicy, ConnectionPoolConnection.OpenStatementPolicy.SILENT);

         String forceRealClosePolicy = props.getProperty("forceRealClosePolicy");
         initializer.forceRealClosePolicy = ConnectionPoolConnection.ForceRealClosePolicy.fromString(forceRealClosePolicy, ConnectionPoolConnection.ForceRealClosePolicy.CONNECTION);
         initializer.closeTimeLimitMillis = Long.parseLong(props.getProperty("closeTimeLimitMillis", "5000"));

         String connectionName = props.getProperty("connectionName", "").trim();
         JDBConnection conn = null;
         if(connectionName.length() > 0) {
            conn = connectionMap.get(connectionName);
            if(conn == null) {
               throw new InitializationException("No connection defined with name '" + connectionName + "'");
            }
         }

         if(conn == null && initializer.jdbcConnection == null) {
            throw new InitializationException("A 'connectionName' must be specified for segment, '" + initializer.name + "'");
         } else if(conn != null) {
            initializer.jdbcConnection = conn;
         }

         long acquireTimeoutMillis = Util.millis(props.getProperty("acquireTimeout"), Long.MIN_VALUE);

         if(acquireTimeoutMillis != Long.MIN_VALUE) {
            initializer.acquireTimeoutMillis = acquireTimeoutMillis;
         }

         long activeTimeoutMillis = Util.millis(props.getProperty("activeTimeout"), Long.MIN_VALUE);
         if(activeTimeoutMillis != Long.MIN_VALUE) {
            initializer.activeTimeoutMillis = activeTimeoutMillis;
            if(initializer.activeTimeoutMillis < 1L) {
               throw new InitializationException("The 'activeTimeout' must be > 0");
            }
         }

         long connectionLifetimeMillis = Util.millis(props.getProperty("connectionLifetime"), Long.MIN_VALUE);
         if(connectionLifetimeMillis != Long.MIN_VALUE) {
            initializer.connectionLifetimeMillis = connectionLifetimeMillis;
            if(initializer.connectionLifetimeMillis < 1L) {
               throw new InitializationException("The 'connectionLifetime' must be > 0");
            }
         }

         long idleTimeBeforeShutdownMillis = Util.millis(props.getProperty("idleTimeBeforeShutdown"), Long.MIN_VALUE);
         if(idleTimeBeforeShutdownMillis != Long.MIN_VALUE) {
            initializer.idleTimeBeforeShutdownMillis = idleTimeBeforeShutdownMillis;
            if(initializer.idleTimeBeforeShutdownMillis < 1L) {
               throw new InitializationException("The 'idleTimeBeforeShutdown' must be > 0");
            }
         }

         long minActiveTimeMillis = Util.millis(props.getProperty("minActiveTime"), Long.MIN_VALUE);
         if(minActiveTimeMillis != Long.MIN_VALUE) {
            initializer.minActiveTimeMillis = minActiveTimeMillis;
            if(initializer.minActiveTimeMillis < 1L) {
               throw new InitializationException("The 'minActiveTime' must be > 0");
            }
         }

         String maxConcurrentReconnects = props.getProperty("reconnectConcurrency", "").trim();
         if(maxConcurrentReconnects.length() > 0) {
            initializer.maxConcurrentReconnects = Integer.parseInt(maxConcurrentReconnects);
         }

         if(initializer.maxConcurrentReconnects < 1) {
            throw new InitializationException("The 'reconnectConcurrency' must be > 0");
         }

         if(props.getProperty("reconnectMaxWaitTime", "").trim().length() > 0) {
            initializer.maxReconnectDelayMillis = Util.millis(props.getProperty("reconnectMaxWaitTime"), 0L);
         }

         if(initializer.maxReconnectDelayMillis < 1L) {
            throw new InitializationException("The 'reconnectMaxWaitTime' must be > 0");
         }

         long activeTimeoutMonitorFrequencyMillis = Util.millis(props.getProperty("activeTimeoutMonitorFrequency"), Long.MIN_VALUE);
         if(activeTimeoutMonitorFrequencyMillis != Long.MIN_VALUE) {
            initializer.activeTimeoutMonitorFrequencySeconds = activeTimeoutMonitorFrequencyMillis / 1000L;
         }

         if(initializer.activeTimeoutMonitorFrequencySeconds < 1L) {
            throw new InitializationException("The 'activeTimeoutMonitorFrequency' must be > 0");
         }

         return initializer;
      }

      /**
       * Verify that all required initialization variables are set.
       * @param withDefaults If <tt>true</tt> defaults will be supplied if possible.
       * @throws InitializationException If initialization is invalid.
       */
      public void validate(final boolean withDefaults) throws InitializationException {

         if(!StringUtil.hasContent(name)) {
            throw new InitializationException("A 'name' is required");
         }

         if(jdbcConnection == null) {
            throw new InitializationException("A connection must be specified");
         } else {
            jdbcConnection.validate();
         }

         if(size < 1) {
            throw new InitializationException("The 'size' must be > 0");
         }

         if(maxConcurrentReconnects < 1) {
            if(withDefaults) {
               maxConcurrentReconnects = size;
            } else {
               throw new InitializationException("The 'maxConcurrentReconnects' must be > 0");
            }
         }

         if(logger == null) {
            if(withDefaults) {
               logger = new ConsoleLogger();
            }
         }
      }

      /**
       * Creates an empty initializer.
       */
      public Initializer() {
      }

      /**
       * Creates an initializer from another.
       * <p>
       * Does not copy the name segment name.
       * </p>
       * @param other The other initializer.
       */
      public Initializer(final Initializer other) {
         this.size = other.size;
         this.acquireTimeoutMillis = other.acquireTimeoutMillis;
         this.activeTimeoutMillis = other.activeTimeoutMillis;
         this.activeTimeoutMonitorFrequencySeconds = other.activeTimeoutMonitorFrequencySeconds;
         this.connectionLifetimeMillis = other.connectionLifetimeMillis;
         this.idleTimeBeforeShutdownMillis = other.idleTimeBeforeShutdownMillis;
         this.minActiveTimeMillis = other.minActiveTimeMillis;
         this.jdbcConnection = other.jdbcConnection;
         this.logger = other.logger;
         this.numCloserThreads = other.numCloserThreads;
         this.maxConcurrentReconnects = other.maxConcurrentReconnects;
         this.maxReconnectDelayMillis = other.maxReconnectDelayMillis;
         this.testOnLogicalOpen = other.testOnLogicalOpen;
         this.testOnLogicalClose = other.testOnLogicalClose;
         this.passwordSource = other.passwordSource;
         this.incompleteTransactionPolicy = other.incompleteTransactionPolicy;
         this.openStatementPolicy = other.openStatementPolicy;
         this.forceRealClosePolicy = other.forceRealClosePolicy;
         this.closeTimeLimitMillis = other.closeTimeLimitMillis;
      }

      private String name;
      private int size;
      private long acquireTimeoutMillis;
      private long activeTimeoutMillis;
      private long activeTimeoutMonitorFrequencySeconds = 30;
      private long connectionLifetimeMillis;
      private long idleTimeBeforeShutdownMillis;
      private long minActiveTimeMillis;
      private JDBConnection jdbcConnection;
      private Logger logger;
      private int numCloserThreads = 0;
      private int maxConcurrentReconnects;
      private long maxReconnectDelayMillis;
      private boolean testOnLogicalOpen = false;
      private boolean testOnLogicalClose = false;
      private PasswordSource passwordSource = null;
      private ConnectionPoolConnection.IncompleteTransactionPolicy incompleteTransactionPolicy = ConnectionPoolConnection.IncompleteTransactionPolicy.REPORT;
      private ConnectionPoolConnection.OpenStatementPolicy openStatementPolicy = ConnectionPoolConnection.OpenStatementPolicy.SILENT;
      private ConnectionPoolConnection.ForceRealClosePolicy forceRealClosePolicy = ConnectionPoolConnection.ForceRealClosePolicy.CONNECTION;
      private long closeTimeLimitMillis = 5000L;
   }


   /**
    * Monitors the queue for closed connections.
    */
   private final class Closer implements Runnable {

      /**
       * Logically closes the connection and performs checks.
       * @param conn The connection.
       */
      final void close(final ConnectionPoolConnection conn) {

         if(conn.state.compareAndSet(ConnectionPoolConnection.STATE_OPEN, ConnectionPoolConnection.STATE_CLOSING)) { //Active timeout monitor may check concurrently
            stats.connectionCount.incrementAndGet();
            try {
               long currTimeMillis = Clock.currTimeMillis;
               if((currTimeMillis - conn.realOpenTime) > connectionLifetimeMillis) {
                  logDebug("Connection lifetime reached for " + conn.id);
                  conn.state.set(ConnectionPoolConnection.STATE_REOPENING); //No other thread can change state from STATE_CLOSING.
                  conn.logicalCloseException = null;
                  reopen(conn);
               } else if((currTimeMillis - conn.lastTestTime) > dbConnection.testIntervalMillis) {
                  logDebug("Performing connection test for " + conn.id);
                  conn.lastTestTime = currTimeMillis;
                  conn.logicalClose(true); //Force test
                  conn.state.set(ConnectionPoolConnection.STATE_AVAILABLE);
                  availableQueue.add(conn);
               } else {
                  conn.logicalClose(testOnLogicalClose);
                  conn.state.set(ConnectionPoolConnection.STATE_AVAILABLE);
                  availableQueue.add(conn);
               }
            } catch(SQLException se) { //Refresh failed - queue for a reopen.
               conn.state.set(ConnectionPoolConnection.STATE_REOPENING);
               conn.logicalCloseException = null;
               stats.connectionErrorCount.incrementAndGet();
               logError("Connection test failed for " + conn.id, se);
               reopen(conn);
            } catch(Throwable t) {
               logErrorWithTrace("Unexpected exception during close", t); //Queue for a reopen.
               conn.state.set(ConnectionPoolConnection.STATE_REOPENING);
               conn.logicalCloseException = null;
               stats.connectionErrorCount.incrementAndGet();
               reopen(conn);
            }
         }
      }

      public void run() {

         try {
            while(true) {
               ConnectionPoolConnection conn = closeQueue.take();
               close(conn);
            }
         } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
         }
      }
   }

   /**
    * Reopens a connection.
    * <p>
    * Closes the real connection and attempts to reopen it.
    * If successful, returns the connection to the available queue.
    * </p>
    */
   private final class Reopener implements Runnable {

      /**
       * Creates the reopener.
       * @param conn The connection to reopen.
       */
      Reopener(final ConnectionPoolConnection conn) {
         this.conn = conn;
      }

      private final ConnectionPoolConnection conn;

      public void run() {

         if(isActive) {
            try {
               if(conn.state.compareAndSet(ConnectionPoolConnection.STATE_REOPENING, ConnectionPoolConnection.STATE_AVAILABLE)) {
                  conn.forceRealClose();
                  try {
                     conn.realOpen();
                     availableQueue.add(conn);
                     logDebug("Reopening " + conn.id);
                  } catch(SQLException se) {
                     stats.connectionErrorCount.incrementAndGet();
                     conn.state.set(ConnectionPoolConnection.STATE_REOPENING);
                     logError("Failed to reopen " + conn.id + " (" + conn.reopenAttempts + (conn.reopenAttempts == 1 ? " try)" : " tries)"), se);
                     reopen(conn);
                  }
               }
            } catch(Throwable t) {
               logErrorWithTrace("Unexpected exception in reopener", t);
            }
         }
      }
   }

   /**
    * Schedule connection reopen.
    * @param conn The connection.
    */
   private void reopen(final ConnectionPoolConnection conn) {

      if(isActive) {
         if(conn.reopenAttempts == 0) {
            conn.reopenAttempts++;
            reopenService.execute(new Reopener(conn));
         } else {
            long delayMillis = 100L * conn.reopenAttempts;
            if(delayMillis > maxReconnectDelayMillis) {
               delayMillis = maxReconnectDelayMillis;
            }
            conn.reopenAttempts++;
            reopenService.schedule(new Reopener(conn), delayMillis, TimeUnit.MILLISECONDS);
         }
      }
   }

   /**
    * Iterates all connections and queues for reopen if their active time exceeds
    * the configured maximum.
    */
   private final class ActiveTimeoutMonitor implements Runnable {
      public void run() {
         if(isActive) {
            try {
               long currTimeMillis = System.currentTimeMillis();
               for(ConnectionPoolConnection conn : connections) {
                  int state = conn.state.get();
                  switch(state) {
                     case ConnectionPoolConnection.STATE_AVAILABLE: {
                        long elapsedTimeMillis = currTimeMillis - conn.realOpenTime;
                        if(connectionLifetimeMillis > 0 && elapsedTimeMillis > connectionLifetimeMillis) {
                           boolean removed = availableQueue.remove(conn);
                           if(removed) {
                              logDebug("Connection lifetime reached for " + conn.id);
                              conn.state.set(ConnectionPoolConnection.STATE_REOPENING);
                              conn.logicalCloseException = null;
                              reopen(conn);
                           }
                        }
                        break;
                     }
                     case ConnectionPoolConnection.STATE_OPEN:
                        long elapsedTimeMillis = currTimeMillis - conn.openTime;
                        if(elapsedTimeMillis > activeTimeoutMillis) {
                           //Make sure connection hasn't been closed in the time since we checked.
                           //Closing out-from-under the app will cause some type of exception.
                           //Presumably the app will close the connection in a "finally"
                           //When this happens - closer will do nothing as state will be changed here.
                           if(conn.state.compareAndSet(ConnectionPoolConnection.STATE_OPEN, ConnectionPoolConnection.STATE_REOPENING)) {
                              if(conn.getTrace() != null) {
                                 logError("Open connection, '" + conn.id + "' inactive for " + elapsedTimeMillis + " ms. Trace: " + conn.getTrace());
                              } else {
                                 logError("Open connection, '" + conn.id + "' inactive for " + elapsedTimeMillis + " ms.");
                              }
                              stats.activeTimeoutCount.incrementAndGet();
                              conn.logicalCloseException = null;
                              reopen(conn);
                           }
                        }

                        break;
                  }
               }
            } catch(Throwable t) {
               logErrorWithTrace("Unexpected exception in active timeout monitor", t);
            }
         }
      }
   }

   /**
    * Creates a pool segment.
    * @param name The segment name.
    * @param size The number of connections in the segment.
    * @param acquireTimeoutMillis The maximum amount of time to wait for a connection to become available.
    * @param activeTimeoutMillis The maximum amount of time a connection may be in the "open" state.
    * @param activeTimeoutMonitorFrequencySeconds The active timeout monitor frequency.
    * @param connectionLifetimeMillis The maximum amount of time a real connection may be open.
    * @param idleTimeBeforeShutdownMillis The amount of time this segment must be idle before shutdown.
    * @param minActiveTimeMillis The amount of time this segment must remain active after activation.
    * @param dbConnection The DB connection string.
    * @param numCloserThreads The number of threads monitoring the close queue. Default 1.
    * @param maxConcurrentReconnects The maximum number of simultaneous reconnects.
    * @param maxReconnectDelayMillis The maximum amount of delay between attempts when reconnect fails.
    * @param testOnLogicalOpen Should connections be tested when logically opened?
    * @param testOnLogicalClose Should connections be tested on logical close?
    * @param incompleteTransactionPolicy The incomplete transaction policy. Default: IncompleteTransactionPolicy.REPORT
    * @param openStatementPolicy The open statement (on close) policy. Default: OpenStatementPolicy.SILENT
    * @param forceRealClosePolicy The policy when connection is forcibly closed. Default: ForceRealClosePolicy.CONNECTION (Statements are closed by the driver).
    * @param passwordSource A password source.
    * @param logger A logger.
    */
   private ConnectionPoolSegment(
           final String name,
           final int size,
           final long acquireTimeoutMillis,
           final long activeTimeoutMillis,
           final long activeTimeoutMonitorFrequencySeconds,
           final long connectionLifetimeMillis,
           final long idleTimeBeforeShutdownMillis,
           final long minActiveTimeMillis,
           final JDBConnection dbConnection,
           final int numCloserThreads,
           final int maxConcurrentReconnects,
           final long maxReconnectDelayMillis,
           final boolean testOnLogicalOpen,
           final boolean testOnLogicalClose,
           final ConnectionPoolConnection.IncompleteTransactionPolicy incompleteTransactionPolicy,
           final ConnectionPoolConnection.OpenStatementPolicy openStatementPolicy,
           final ConnectionPoolConnection.ForceRealClosePolicy forceRealClosePolicy,
           final PasswordSource passwordSource,
           final Logger logger,
           final long closeTimeLimitMillis) {


      this.name = name;
      this.acquireTimeoutMillis = acquireTimeoutMillis;
      this.activeTimeoutMillis = activeTimeoutMillis;
      if(connectionLifetimeMillis < 1) {
         this.connectionLifetimeMillis = 3600L * 1000L; //1 hour
      } else {
         this.connectionLifetimeMillis = connectionLifetimeMillis;
      }
      this.idleTimeBeforeShutdownMillis = idleTimeBeforeShutdownMillis;
      this.minActiveTimeMillis = minActiveTimeMillis;
      this.dbConnection = dbConnection;
      this.testOnLogicalOpen = testOnLogicalOpen;
      this.testOnLogicalClose = testOnLogicalClose;
      this.logger = logger;

      this.connections = new ConnectionPoolConnection[size];
      ArrayList<ConnectionPoolConnection> connections = new ArrayList<ConnectionPoolConnection>(size);
      for(int i = 0; i < size; i++) {
         ConnectionPoolConnection conn = new ConnectionPoolConnection(this, name + ":connection-" + i, dbConnection.testSQL, dbConnection.debug,
                 incompleteTransactionPolicy, openStatementPolicy, forceRealClosePolicy, closeTimeLimitMillis);
         conn.state.set(ConnectionPoolConnection.STATE_AVAILABLE);
         connections.add(conn);
         this.connections[i] = conn;
      }

      availableQueue = new ArrayBlockingQueue<ConnectionPoolConnection>(size, false, connections);

      if(numCloserThreads > 0) {

         String closerThreadNameBase = StringUtil.hasContent(name) ? ("ACP:" + name + ":") : "ACP:";

         closeQueue = new ArrayBlockingQueue<ConnectionPoolConnection>(size);
         closerThreads = new Thread[numCloserThreads];
         for(int i = 0; i < closerThreads.length; i++) {
            closerThreads[i] = new Thread(new Closer(), closerThreadNameBase + "Closer-" + i);
            closerThreads[i].start();
         }
         closer = null;
      } else {
         closeQueue = null;
         closerThreads = new Thread[0];
         closer = new Closer();
      }

      this.activeTimeoutMonitorFrequencySeconds = activeTimeoutMonitorFrequencySeconds;

      reopenService = new ScheduledThreadPoolExecutor(maxConcurrentReconnects == 0 ? 1 : maxConcurrentReconnects,
              Util.createThreadFactoryBuilder(name, "Reopener"));
      reopenService.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
      reopenService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

      this.maxReconnectDelayMillis = maxReconnectDelayMillis;
      this.passwordSource = passwordSource;

      logInfo("Created with " + size + " connections, saturated pool wait " + acquireTimeoutMillis + " ms");
      logInfo("Connection: " + dbConnection.toString());
   }

   /**
    * Starts the active (too long) timeout monitor.
    * @param inactiveMonitorService The service.
    */
   final void startActiveTimeoutMonitor(final ScheduledThreadPoolExecutor inactiveMonitorService) {
      inactiveMonitorService.scheduleWithFixedDelay(new ActiveTimeoutMonitor(),
              activeTimeoutMonitorFrequencySeconds, activeTimeoutMonitorFrequencySeconds, TimeUnit.SECONDS);
   }

   /**
    * Adds a connection to the close queue.
    * @param conn The connection.
    */
   final void close(final ConnectionPoolConnection conn) {
      if(closeQueue != null) {
         closeQueue.add(conn);
      } else {
         closer.close(conn);
      }
   }

   /**
    * Opens a connection by obtaining one from the available queue.
    * <p>
    * If no connection becomes available before the specified wait time,
    * <tt>null</tt> is returned. If specified wait time < 0, waits indefinitely
    * for a connection to become available.
    * </p>
    * @param timeout The timeout value.
    * @param timeoutUnit The timeout units.
    * @return A connection or <tt>null</tt> if none was available in the specified wait time.
    * @throws InterruptedException on thread interruption.
    */
   final ConnectionPoolConnection open(final long timeout, final TimeUnit timeoutUnit) throws InterruptedException {

      ConnectionPoolConnection conn = timeout >= 0 ? availableQueue.poll(timeout, timeoutUnit) : availableQueue.take();

      if(conn != null) {
         if(!testOnLogicalOpen) {
            conn.logicalOpen();
            conn.state.set(ConnectionPoolConnection.STATE_OPEN);
         } else {
            try {
               conn.logicalOpenWithTest();
               conn.state.set(ConnectionPoolConnection.STATE_OPEN);
            } catch(SQLException se) {
               conn.state.set(ConnectionPoolConnection.STATE_REOPENING);
               stats.connectionErrorCount.incrementAndGet();
               reopen(conn);
               return open(); //Attempt to open another connection.
            }
         }
      }

      return conn;
   }


   /**
    * Opens a connection by obtaining one from the available queue.
    * <p>
    * If no connection is immediately, <tt>null</tt> is returned.
    * </p>
    * @return A connection or <tt>null</tt> if none was immediately available.
    */
   final ConnectionPoolConnection open() {
      ConnectionPoolConnection conn = availableQueue.poll();
      if(conn != null) {
         if(!testOnLogicalOpen) {
            conn.logicalOpen();
            conn.state.set(ConnectionPoolConnection.STATE_OPEN);
         } else {
            try {
               conn.logicalOpenWithTest();
               conn.state.set(ConnectionPoolConnection.STATE_OPEN);
            } catch(SQLException se) {
               conn.state.set(ConnectionPoolConnection.STATE_REOPENING);
               stats.connectionErrorCount.incrementAndGet();
               reopen(conn);
               return open(); //Attempt to open another connection
            }
         }
      }
      return conn;
   }

   /**
    * The time the pool was created.
    */
   private static final long createTime = System.currentTimeMillis();

   /**
    * Timeout value for connection wait when all pool connections are in-use.
    */
   final long acquireTimeoutMillis;

   /**
    * Time a segment must be idle before shutdown.
    */
   final long idleTimeBeforeShutdownMillis;

   /**
    * Time a segment must be active before eligible for shutdown.
    */
   final long minActiveTimeMillis;

   /**
    * Should connection be tested when logically opened?
    */
   final boolean testOnLogicalOpen;

   /**
    * Should connection be tested when logically closed?
    */
   final boolean testOnLogicalClose;

   /**
    * The segment name.
    */
   final String name;

   /**
    * The logger.
    */
   private final Logger logger;

   /**
    * The maximum amount of time a logical connection may be active before it is automatically closed.
    */
   final long activeTimeoutMillis;

   /**
    * The maximum amount of time a physical connection may remain open before it is closed and reopened.
    */
   final long connectionLifetimeMillis;

   /**
    * The real-database connection parameters.
    */
   final JDBConnection dbConnection;

   /**
    * Connections in this segment.
    */
   private final ConnectionPoolConnection[] connections;

   /**
    * Connections waiting to be closed and returned to the available queue.
    */
   private final ArrayBlockingQueue<ConnectionPoolConnection> closeQueue;

   /**
    * Available connections.
    */
   private final ArrayBlockingQueue<ConnectionPoolConnection> availableQueue;

   /**
    * Threads in which <tt>Closers</tt> run.
    */
   private final Thread[] closerThreads;

   /**
    * Use to close logical connections when closer concurrency is zero.
    */
   private final Closer closer;

   /**
    * The frequency at which connections are monitored for inactivity.
    */
   private final long activeTimeoutMonitorFrequencySeconds;

   /**
    * Service for connection reopen.
    */
   private final ScheduledThreadPoolExecutor reopenService;

   /**
    * A source for connection passwords.
    */
   private final PasswordSource passwordSource;

   /**
    * The maximum amount of time to delay between reconnect attempts (on failure).
    */
   final long maxReconnectDelayMillis;

   /**
    * Indicates if the pool is active.
    */
   private volatile boolean isActive = false;

   /**
    * Statistics for the segment.
    */
   final Stats stats = new Stats();

   /**
    * The pool this segment is part of.
    */
   ConnectionPool pool;

   /**
    * A time-limiter - used for obtaining database connections.
    */
   private static final SimpleTimeLimiter connectionTimeLimiter = new SimpleTimeLimiter();

   /**
    * Gets the active-for-too-long monitor frequency.
    * @return The frequency.
    */
   final long getActiveTimeoutMonitorFrequencySeconds() {
      return activeTimeoutMonitorFrequencySeconds;
   }

   /**
    * Gets the number of threads for close.
    * @return The thread count.
    */
   final int getCloserThreadCount() {
      return closerThreads.length;
   }

   /**
    * Gets the maximum number of concurrent database reconnect attempts.
    * @return The maximum number of reconnects.
    */
   final int getMaxConcurrentReconnects() {
      return reopenService.getCorePoolSize();
   }

   /**
    * Creates a real database connection.
    * @return The connection.
    * @throws SQLException on connection problem.
    */
   final Connection createRealConnection() throws SQLException {
      return createRealConnection(dbConnection.createTimeoutMillis);
   }

   /**
    * Gets the password for connection creation.
    * @return The password.
    */
   final String getPassword() {
      if(passwordSource == null) {
         return dbConnection.password;
      } else {
         String usePassword = passwordSource.getPassword(dbConnection.name);
         if(StringUtil.hasContent(usePassword)) {
            return usePassword;
         } else {
            usePassword = passwordSource.getPassword(dbConnection.connectionString, dbConnection.user);
            return StringUtil.hasContent(usePassword) ? usePassword : dbConnection.password;
         }
      }
   }

   /**
    * Creates a real database connection, failing if one is not obtained in the specified time.
    * @param timeoutMillis The timeout in milliseconds.
    * @return The connection.
    * @throws SQLException on connection problem or timeout waiting for connection.
    */
   private Connection createRealConnection(final long timeoutMillis) throws SQLException {

      if(timeoutMillis < 1L) {

         String usePassword = getPassword();

         Connection conn = dbConnection.datasource == null ?
                 DriverManager.getConnection(dbConnection.connectionString, dbConnection.user, usePassword) :
                 dbConnection.datasource.getConnection(dbConnection.user, usePassword);
         if(conn != null) {
            return conn;
         } else {
            throw new SQLException("Unable to create connection: driver/datasource returned [null]");
         }

      } else {

         try {
            return connectionTimeLimiter.callWithTimeout(new Callable<Connection>() {
               public Connection call() throws Exception {
                  return createRealConnection(0L);
               }
            }, timeoutMillis, TimeUnit.MILLISECONDS, true);
         } catch(UncheckedTimeoutException ute) {
            throw new SQLException("Unable to create connection after waiting " + timeoutMillis + " ms");
         } catch(Exception e) {
            if(e instanceof SQLException) {
               throw (SQLException)e;
            } else {
               throw new SQLException("Unable to create connection: driver/datasource", e);
            }
         }
      }
   }

   /**
    * Activates the segment by opening and testing all connections.
    * @throws SQLException if connections could not be created.
    */
   final void activate() throws SQLException {
      for(ConnectionPoolConnection conn : connections) {
         conn.forceRealClose();
         conn.realOpen();
         conn.logicalClose(true); //Tests the connection
      }

      if(closeQueue != null) {
         closeQueue.clear();
      }

      availableQueue.clear();
      reopenService.getQueue().clear();

      ArrayList<ConnectionPoolConnection> connections = new ArrayList<ConnectionPoolConnection>(this.connections.length);
      for(ConnectionPoolConnection connection : this.connections) {
         connections.add(connection);
         connection.state.set(ConnectionPoolConnection.STATE_AVAILABLE);
      }

      isActive = true;
      stats.activate();
      availableQueue.addAll(connections);
   }

   /**
    * Determine if this segment is currently idle.
    * @return Is the segment idle?
    */
   final boolean isIdle() {
      for(ConnectionPoolConnection conn : connections) {
         if(conn.state.get() == ConnectionPoolConnection.STATE_OPEN) {
            return false;
         }
      }
      return true;
   }

   /**
    * Deactivates the pool.
    * <p>
    * As connections are returned to the pool, the real
    * database connections will be closed.
    * </p>
    * @return Were all connections deactivated?
    * @throws InterruptedException if interrupted.
    */
   final boolean deactivate() throws InterruptedException {

      isActive = false; //Stop any reopen operations
      stats.deactivate();

      availableQueue.clear(); //No more open connections...

      reopenService.getQueue().clear(); //Clear all active reopeners - don't care...

      //Wait for all open connections to be closed & connections in the process of closing to finish
      //Reopening connections don't matter

      long maxWaitMillis = activeTimeoutMillis;
      if(maxWaitMillis < 1L) {
         maxWaitMillis = 60000L;
      }

      long elapsedWaitMillis = 0L;

      outer:
      while(elapsedWaitMillis < maxWaitMillis) {

         for(ConnectionPoolConnection conn : connections) {
            int state = conn.state.get();
            if(state == ConnectionPoolConnection.STATE_OPEN ||
                    state == ConnectionPoolConnection.STATE_CLOSING) {
               Thread.sleep(500L);
               elapsedWaitMillis += 500L;
               continue outer;
            } else {
               conn.forceRealClose();
               conn.state.set(ConnectionPoolConnection.STATE_DISCONNECTED);
            }
         }

         return true;
      }

      return false;
   }

   /**
    * Deactivates the pool immediately - real closing all connections out from under the logical connections.
    */
   final void deactivateNow() {
      isActive = false;
      stats.deactivate();
      availableQueue.clear();
      reopenService.getQueue().clear();
      for(ConnectionPoolConnection conn : connections) {
         conn.forceRealClose();
         conn.state.set(ConnectionPoolConnection.STATE_DISCONNECTED);
      }
   }

   /**
    * Gets statistics for the segment.
    * @return The segment statistics.
    */
   public Stats getStats() {
      return stats;
   }

   /**
    * Gets the number of active connections.
    * @return The number of active connections.
    */
   public final int getActiveConnectionCount() {

      if(!isActive) {
         return 0;
      }

      int count = 0;
      for(ConnectionPoolConnection conn : connections) {
         if(conn.state.get() != ConnectionPoolConnection.STATE_AVAILABLE) {
            count++;
         }
      }

      return count;
   }

   /**
    * Gets the number of available connections.
    * @return The number of available connections.
    */
   public final int getAvailableConnectionCount() {

      if(!isActive) {
         return 0;
      }

      int count = 0;
      for(ConnectionPoolConnection conn : connections) {
         if(conn.state.get() == ConnectionPoolConnection.STATE_AVAILABLE) {
            count++;
         }
      }
      return count;
   }

   /**
    * Gets the maximum number of connections.
    * @return The maximum number of connections.
    */
   public int getMaxConnections() {
      return connections.length;
   }

   /**
    * Gets the current size of the available queue.
    * @return The available queue size.
    */
   final int getAvailableQueueSize() {
      return availableQueue.size();
   }

   /**
    * Shutdown the segment.
    * <p>
    * <ul>
    * <li>Deactivate</li>
    * <li>Shutdown the reopen service.</li>
    * <li>Interrupts the closer threads, if any.</li>
    * </ul>
    * </p>
    */
   final void shutdown() {

      boolean deactivated = false;
      try {
         deactivated = deactivate();
      } catch(InterruptedException ie) {
         Thread.currentThread().interrupt();
      }

      if(!deactivated) {
         deactivateNow();
      }

      reopenService.shutdownNow();

      for(Thread closerThread : closerThreads) {
         closerThread.interrupt();
      }
   }

   /**
    * Logs an info message.
    * @param message The message.
    */
   void logDebug(final String message) {
      if(logger != null) {
         try {
            StringBuilder buf = new StringBuilder(name);
            buf.append(": ");
            buf.append(message);
            logger.debug(buf.toString());
         } catch(Throwable t) {
            //Ignore - logging should not kill anything
         }
      }
   }

   /**
    * Logs an info message.
    * @param message The message.
    */
   void logInfo(final String message) {
      if(logger != null) {
         try {
            StringBuilder buf = new StringBuilder(name);
            buf.append(": ");
            buf.append(message);
            logger.info(buf.toString());
         } catch(Throwable t) {
            //Ignore - logging should not kill anything
         }
      }
   }

   /**
    * Logs an error message.
    * @param message The message.
    */
   void logError(final String message) {
      if(logger != null) {
         try {
            StringBuilder buf = new StringBuilder(name);
            buf.append(":");
            buf.append(message);
            logger.error(buf.toString());
         } catch(Throwable t) {
            //Ignore - logging should not kill anything
         }
      }
   }

   /**
    * Logs an error message.
    * @param message The message.
    * @param t A <tt>Throwable</tt> related to the error.
    */
   void logError(final String message, final Throwable t) {
      if(logger != null) {
         try {
            StringBuilder buf = new StringBuilder(name);
            buf.append(":");
            buf.append(message);
            buf.append(":").append(t.getMessage());
            logger.error(buf.toString());
         } catch(Throwable t2) {
            //Ignore - logging should not kill anything
         }
      }
   }

   /**
    * Logs an error message.
    * @param message The message.
    * @param t A <tt>Throwable</tt> related to the error.
    */
   void logErrorWithTrace(final String message, final Throwable t) {
      if(logger != null) {
         try {
            StringBuilder buf = new StringBuilder(name);
            buf.append(":");
            buf.append(message);
            logger.error(buf.toString(), t);
         } catch(Throwable t2) {
            //Ignore - logging should not kill anything
         }
      }
   }
}