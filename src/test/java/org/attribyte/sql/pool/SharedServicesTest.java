/*
 * Copyright 2026 Attribyte Labs, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.attribyte.sql.pool;

import org.attribyte.api.Logger;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Pools share a time limiter and a clock. One pool shutting down must not take them from another:
 * an application with two pools shuts them down in turn, and the second must still be able to close
 * its connections — and a pool that is still running must still be able to open them.
 */
public class SharedServicesTest {

   /** Remembers what was logged as an error. */
   private static final class ErrorLog implements Logger {
      final List<String> errors = new CopyOnWriteArrayList<>();
      @Override public void debug(String msg) {}
      @Override public void info(String msg) {}
      @Override public void warn(String msg) {}
      @Override public void warn(String msg, Throwable t) {}
      @Override public void error(String msg) { errors.add(msg); }
      @Override public void error(String msg, Throwable t) { errors.add(msg + ": " + t); }
   }

   private static ConnectionPool pool(final String name, final Logger logger) throws Exception {
      JDBConnection jdbcConnection = new JDBConnection("test", "", "", new TestDataSource(0L, 0L, false),
              0L, "", 0L, false);
      ConnectionPoolSegment segment = new ConnectionPoolSegment.Initializer()
              .setName("segment-0")
              .setConnection(jdbcConnection)
              .setAcquireTimeout(100, TimeUnit.MILLISECONDS)
              .setActiveTimeout(5, TimeUnit.MINUTES)
              .setActiveTimeoutMonitorFrequency(5, TimeUnit.SECONDS)
              .setCloseConcurrency(1)
              .setConnectionLifetime(1, TimeUnit.HOURS)
              .setMaxConcurrentReconnects(1)
              .setMaxReconnectDelay(1, TimeUnit.SECONDS)
              .setSize(4)
              .setTestOnLogicalClose(false)
              .setTestOnLogicalOpen(false)
              // Closes go through the shared time limiter, as they do for a pool built from properties.
              .setForceRealClosePolicy(ConnectionPoolConnection.ForceRealClosePolicy.CONNECTION_WITH_LIMIT)
              .setLogger(logger)
              .createSegment();
      return new ConnectionPool.Initializer()
              .setName(name)
              .addActiveSegment(segment)
              .setMinActiveSegments(1)
              .setLogger(logger)
              .createPool();
   }

   /** Opens every connection in the pool's segment, so there is something real to close. */
   private static void use(final ConnectionPool pool) throws Exception {
      Connection[] held = new Connection[4];
      for(int i = 0; i < held.length; i++) held[i] = pool.getConnection();
      for(Connection conn : held) conn.close();
   }

   @Test
   public void theSecondPoolToShutDownCanStillCloseItsConnections() throws Exception {
      ErrorLog firstLog = new ErrorLog(), secondLog = new ErrorLog();
      ConnectionPool first = pool("first", firstLog);
      ConnectionPool second = pool("second", secondLog);
      use(first);
      use(second);

      first.shutdown();
      second.shutdown();

      assertTrue("the first pool closed cleanly: " + firstLog.errors, firstLog.errors.isEmpty());
      assertTrue("the second pool closed cleanly: " + secondLog.errors, secondLog.errors.isEmpty());
   }

   @Test
   public void aPoolStillRunningCanOpenConnectionsAfterAnotherShutsDown() throws Exception {
      ErrorLog log = new ErrorLog();
      ConnectionPool first = pool("first", new ErrorLog());
      ConnectionPool second = pool("second", log);
      try {
         first.shutdown();
         Connection conn = second.getConnection();
         assertNotNull(conn);
         conn.close();
         assertTrue("the running pool logged no errors: " + log.errors, log.errors.isEmpty());
      } finally {
         second.shutdown();
      }
   }

   @Test
   public void aPoolCreatedAfterAllOthersShutDownWorks() throws Exception {
      ErrorLog log = new ErrorLog();
      pool("gone", new ErrorLog()).shutdown();
      ConnectionPool later = pool("later", log);
      use(later);
      later.shutdown();
      assertTrue("the later pool worked and closed cleanly: " + log.errors, log.errors.isEmpty());
   }
}
