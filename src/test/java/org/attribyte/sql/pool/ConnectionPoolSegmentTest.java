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

import org.attribyte.api.ConsoleLogger;
import org.junit.After;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Segment tests.
 * <p>
 * Many of these tests are performed with methods and variables
 * that have only package visibility.
 * </p>
 */
public class ConnectionPoolSegmentTest {

   private ConnectionPoolSegment segment = null;
   private final TestDataSource datasource = new TestDataSource(0L, 0L, false);
   private final TestDataSource brokenDatasource = new TestDataSource(60000L, 0L, false);

   private static final int SEGMENT_SIZE = 2;

   private static final int MAX_THREADS = 5;
   private final ExecutorService testService = Executors.newFixedThreadPool(MAX_THREADS);
   private static ScheduledThreadPoolExecutor inactiveMonitorService = new ScheduledThreadPoolExecutor(1);

   private ConnectionPoolSegment createSegment() throws Exception {
      return createSegment(datasource);
   }

   private ConnectionPoolSegment createBrokenSegment() throws Exception {
      return createSegment(brokenDatasource);
   }

   private ConnectionPoolSegment createSegment(TestDataSource datasource) throws Exception {

      if(segment != null) {
         segment.shutdown();
         segment = null;
      }

      JDBConnection jdbcConnection = new JDBConnection("conn1", "root", "", "jdbc:mysql://127.0.0.1/test", 40000L, "SELECT 1 FROM test", 200L, false);

      ConnectionPoolSegment.Initializer init = new ConnectionPoolSegment.Initializer();
      segment = init.setName("segment-test")
              .setConnection(jdbcConnection)
              .setAcquireTimeout(10, TimeUnit.MILLISECONDS)
              .setActiveTimeout(30, TimeUnit.SECONDS)
              .setActiveTimeoutMonitorFrequency(5, TimeUnit.SECONDS)
              .setCloseConcurrency(5)
              .setConnectionLifetime(1, TimeUnit.HOURS)
              .setMaxConcurrentReconnects(1)
              .setMaxReconnectDelay(1, TimeUnit.SECONDS)
              .setSize(SEGMENT_SIZE)
              .setTestOnLogicalClose(false)
              .setTestOnLogicalOpen(false)
              .setLogger(new ConsoleLogger())
              .createSegment();
      segment.startActiveTimeoutMonitor(inactiveMonitorService);
      return segment;
   }

   private ConnectionPoolSegment createSegmentWithRandomFail() throws Exception {

      if(segment != null) {
         segment.shutdown();
         segment = null;
      }

      JDBConnection jdbcConnectionWithFail = new JDBConnection("conn1", "root", "", "jdbc:mysql://127.0.0.1/test", 40000L, "RANDOM FAIL", 200L, false);

      ConnectionPoolSegment.Initializer init = new ConnectionPoolSegment.Initializer();
      segment = init.setName("segment-test")
              .setConnection(jdbcConnectionWithFail)
              .setAcquireTimeout(10, TimeUnit.MILLISECONDS)
              .setActiveTimeout(120, TimeUnit.SECONDS)
              .setActiveTimeoutMonitorFrequency(5, TimeUnit.SECONDS)
              .setCloseConcurrency(0)
              .setConnectionLifetime(1, TimeUnit.HOURS)
              .setMaxConcurrentReconnects(2)
              .setMaxReconnectDelay(1, TimeUnit.SECONDS)
              .setSize(200)
              .setTestOnLogicalClose(true)
              .setTestOnLogicalOpen(false)
              .setLogger(new ConsoleLogger())
              .createSegment();
      segment.startActiveTimeoutMonitor(inactiveMonitorService);
      return segment;
   }

   @After
   public void shutdown() {
      if(segment != null) {
         segment.shutdown();
      }
      segment = null;
   }

   @Test
   public void connectionStateTransition() throws Exception {
      ConnectionPoolSegment segment = createSegment();
      segment.activate();
      Connection conn = segment.open();
      assertNotNull(conn);
      ConnectionPoolConnection poolConn = (ConnectionPoolConnection)conn;
      assertEquals(ConnectionPoolConnection.STATE_OPEN, poolConn.state.get());
      assertEquals(1, segment.getActiveConnectionCount());
      assertEquals(1, segment.getAvailableQueueSize());
      assertTrue(poolConn.openTime > 0);
      assertTrue(poolConn.realOpenTime > 0);
      assertEquals(SEGMENT_SIZE - 1, segment.getAvailableConnectionCount());

      conn.close();
      conn.close(); //Should not throw exception

      int attempts = 0;
      while(attempts++ < 10 && poolConn.state.get() != ConnectionPoolConnection.STATE_AVAILABLE) {
         Thread.sleep(10L);
      }
      assertEquals(ConnectionPoolConnection.STATE_AVAILABLE, poolConn.state.get());
      assertTrue(segment.isIdle());
      assertEquals(SEGMENT_SIZE, segment.getAvailableQueueSize());
   }

   @Test(expected = java.sql.SQLException.class)
   public void connectionAfterCloseErrors() throws Exception {
      ConnectionPoolSegment segment = createSegment();
      segment.activate();
      Connection conn = segment.open();
      assertNotNull(conn);
      ConnectionPoolConnection poolConn = (ConnectionPoolConnection)conn;
      conn.close();
      conn.close(); //Should not throw exception
      conn.createStatement();
   }

   @Test
   public void connectionAcquireWaitStateTransition() throws Exception {
      ConnectionPoolSegment segment = createSegment();
      segment.activate();
      Connection conn = segment.open(100, TimeUnit.MILLISECONDS);
      assertNotNull(conn);
      ConnectionPoolConnection poolConn = (ConnectionPoolConnection)conn;
      assertEquals(ConnectionPoolConnection.STATE_OPEN, poolConn.state.get());
      assertEquals(1, segment.getActiveConnectionCount());
      assertEquals(SEGMENT_SIZE - 1, segment.getAvailableConnectionCount());
      conn.close();
      int attempts = 0;
      while(attempts++ < 10 && segment.getAvailableQueueSize() != SEGMENT_SIZE) {
         Thread.sleep(10L);
      }
      assertEquals(SEGMENT_SIZE, segment.getAvailableQueueSize());
   }

   @Test
   public void multiThread() throws Exception {

      ConnectionPoolSegment segment = createSegment();
      segment.activate();

      int threadCount = 2;

      for(int i = 0; i < threadCount; i++) {
         Thread.sleep(500L);
         TestWorker worker = new TestWorker(segment, 100L, 100, 10L, true); //Wait up to 100ms for connection, work up to 10ms
         Thread.sleep(500L);
         testService.execute(worker);
      }

      int attempts = 0;
      while(attempts++ < 10 && !segment.isIdle()) {
         Thread.sleep(1000L);
      }

      ConnectionPoolSegment.Stats stats = segment.getStats();

      assertTrue(segment.isIdle());
      assertEquals(0, stats.getFailedConnectionErrorCount());
      assertEquals(100 * threadCount, stats.getConnectionCount());
   }

   @Test
   public void allBusy() throws Exception {

      ConnectionPoolSegment segment = createSegment();
      segment.activate();

      List<Connection> connList = new ArrayList<Connection>();

      try {

         for(int i = 0; i < SEGMENT_SIZE; i++) {
            Connection conn = segment.open();
            assertNotNull(conn);
            connList.add(conn);
         }

         assertEquals(0, segment.getAvailableConnectionCount());
         assertEquals(SEGMENT_SIZE, segment.getActiveConnectionCount());

         Connection conn2 = segment.open();
         assertNull(conn2);

         conn2 = segment.open(5L, TimeUnit.SECONDS);
         assertNull(conn2);
      } finally {
         for(Connection conn : connList) {
            conn.close();
         }
      }

      int attempts = 0;
      while(attempts++ < 10 && segment.getAvailableQueueSize() != SEGMENT_SIZE) {
         Thread.sleep(10L);
      }
      assertEquals(SEGMENT_SIZE, segment.getAvailableQueueSize());
   }

   @Test
   public void connectionActiveLimit() throws Exception {

      ConnectionPoolSegment segment = createSegment();
      segment.activate();

      Connection conn = segment.open();
      assertNotNull(conn);
      ConnectionPoolConnection poolConn = (ConnectionPoolConnection)conn;
      assertEquals(ConnectionPoolConnection.STATE_OPEN, poolConn.state.get());

      int attempts = 0;
      while(attempts++ < 60 && poolConn.state.get() == ConnectionPoolConnection.STATE_OPEN) {
         Thread.sleep(1000L);
      }

      assertEquals(ConnectionPoolConnection.STATE_AVAILABLE, poolConn.state.get());
      assertEquals(1, segment.getStats().getActiveTimeoutCount());
      conn.close();
      assertTrue(segment.isIdle());
   }

   @Test
   public void connectionOpenTimesSet() throws Exception {
      ConnectionPoolSegment segment = createSegment();
      segment.activate();
      Connection conn = segment.open();
      assertNotNull(conn);
      ConnectionPoolConnection poolConn = (ConnectionPoolConnection)conn;
      assertTrue(poolConn.openTime > 0);
      assertTrue(poolConn.realOpenTime > 0);
      conn.close();
   }

   @Test
   public void segmentStatsSet() throws Exception {

      ConnectionPoolSegment segment = createSegment();
      segment.activate();

      for(int i = 0; i < 100; i++) {
         Connection conn = segment.open(100, TimeUnit.MILLISECONDS);
         Thread.sleep(10L);
         assertNotNull(conn);
         conn.close();
      }

      int attempts = 0;
      while(!segment.isIdle() && attempts++ < 100) {
         Thread.sleep(10L);
      }
      assertTrue(segment.isIdle()); //

      //Note that stats are recorded when connection is closed.
      //When segment is idle, all stats must be recorded.

      ConnectionPoolSegment.Stats stats = segment.getStats();
      assertTrue(stats.isActive());
      assertEquals(100, stats.getConnectionCount());
      assertTrue(stats.getLastActivatedTime() > 0L);

      //Deactivate records the cumulative active time
      segment.deactivate();

      //Wait for a bit while inactive
      Thread.sleep(2000L);

      assertTrue(stats.getCumulativeActiveTimeMillis() > 1000L);
      assertFalse(stats.isActive());
   }
}