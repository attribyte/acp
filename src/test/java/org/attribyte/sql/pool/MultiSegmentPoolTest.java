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
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Pool tests.
 * <p>
 * Some of these tests are performed with methods and variables
 * that have only package visibility.
 * </p>
 */
public class MultiSegmentPoolTest {

   private ConnectionPoolSegment segment0;
   private ConnectionPoolSegment segment1;
   private ConnectionPoolSegment segment2;

   private ConnectionPool pool;
   private final TestDataSource datasource = new TestDataSource(0L, 0L, false);
   private static final int MAX_THREADS = 3;
   private final ExecutorService testService = Executors.newFixedThreadPool(MAX_THREADS * 5);

   @Before
   public void createPool() {

      try {
         JDBConnection jdbcConnection = new JDBConnection("test", "", "", datasource, 0L, "SELECT 1 FROM test", 400L, false);

         ConnectionPoolSegment.Initializer segmentInit = new ConnectionPoolSegment.Initializer();
         segment0 = segmentInit.setName("segment-0")
                 .setConnection(jdbcConnection)
                 .setAcquireTimeout(10, TimeUnit.MILLISECONDS)
                 .setActiveTimeout(5, TimeUnit.SECONDS)
                 .setActiveTimeoutMonitorFrequency(30, TimeUnit.SECONDS)
                 .setCloseConcurrency(0)
                 .setConnectionLifetime(1, TimeUnit.HOURS)
                 .setMaxConcurrentReconnects(1)
                 .setMaxReconnectDelay(1, TimeUnit.SECONDS)
                 .setSize(MAX_THREADS)
                 .setTestOnLogicalClose(false)
                 .setTestOnLogicalOpen(false)
                 .createSegment();

         segment1 = segmentInit.setName("segment-1")
                 .setConnection(jdbcConnection)
                 .setAcquireTimeout(100, TimeUnit.MILLISECONDS)
                 .setActiveTimeout(30, TimeUnit.SECONDS)
                 .setActiveTimeoutMonitorFrequency(5, TimeUnit.SECONDS)
                 .setCloseConcurrency(0)
                 .setConnectionLifetime(1, TimeUnit.HOURS)
                 .setMaxConcurrentReconnects(1)
                 .setMaxReconnectDelay(1, TimeUnit.SECONDS)
                 .setSize(MAX_THREADS)
                 .setTestOnLogicalClose(false)
                 .setTestOnLogicalOpen(false)
                 .setIdleTimeBeforeShutdown(2L, TimeUnit.SECONDS)
                 .setMinActiveTime(1L, TimeUnit.SECONDS)
                 .createSegment();

         segment2 = segmentInit.setName("segment-2")
                 .setConnection(jdbcConnection)
                 .setAcquireTimeout(100, TimeUnit.MILLISECONDS)
                 .setActiveTimeout(30, TimeUnit.SECONDS)
                 .setActiveTimeoutMonitorFrequency(5, TimeUnit.SECONDS)
                 .setCloseConcurrency(0)
                 .setConnectionLifetime(1, TimeUnit.HOURS)
                 .setMaxConcurrentReconnects(1)
                 .setMaxReconnectDelay(1, TimeUnit.SECONDS)
                 .setSize(MAX_THREADS)
                 .setTestOnLogicalClose(false)
                 .setTestOnLogicalOpen(false)
                 .setIdleTimeBeforeShutdown(2L, TimeUnit.SECONDS)
                 .setMinActiveTime(1L, TimeUnit.SECONDS)
                 .createSegment();

         ConnectionPool.Initializer poolInit = new ConnectionPool.Initializer();
         pool = poolInit.setName("test-pool")
                 .addActiveSegment(segment0)
                 .addActiveSegment(segment1)
                 .addReserveSegment(segment2)
                 .setMinActiveSegments(1)
                 .setMinSegmentExpansionDelay(0L)
                 .setIdleCheckInterval(3, TimeUnit.SECONDS)
                 .setLogger(new ConsoleLogger()).createPool();
      } catch(Exception e) {
         System.err.println("Problem starting pool");
         e.printStackTrace();
      }
   }

   @After
   public void destroySegment() throws Exception {

      if(pool != null) {
         try {
            pool.shutdown();
         } catch(Exception e) {
            System.out.println("Problem shutting down pool");
            e.printStackTrace();
            throw e;
         }
      }
   }

   @Test
   public void expandContract() throws Exception {
      assertEquals(2, pool.getActiveSegments());
      assertTrue(segment0.getStats().isActive());
      assertTrue(segment1.getStats().isActive());
      assertFalse(segment2.getStats().isActive());
      assertEquals(2 * MAX_THREADS, pool.getStats().getAvailableConnections());

      List<Connection> connections = new ArrayList<Connection>();
      for(int i = 0; i < 9; i++) {
         connections.add(pool.getConnection());
      }

      assertEquals(3, pool.getActiveSegments());
      assertTrue(segment0.getStats().isActive());
      assertTrue(segment1.getStats().isActive());
      assertTrue(segment2.getStats().isActive());
      assertEquals(0, pool.getStats().getAvailableConnections());

      for(Connection conn : connections) {
         conn.close();
      }

      Thread.sleep(10000L);

      assertEquals(2, pool.getActiveSegments());

      Thread.sleep(10000L);

      assertEquals(1, pool.getActiveSegments());

   }

   @Test
   public void multiThreadNoFail() throws Exception {

      assertEquals(2 * MAX_THREADS, pool.getStats().getAvailableConnections());


      int threadCount = 6;

      for(int i = 0; i < threadCount; i++) {
         TestWorker worker = new TestWorker(pool, 100, 10L, true); //Work up to 10ms
         Thread.sleep(1000L);
         testService.execute(worker);
      }

      int attempts = 0;
      while(attempts++ < 10 && !pool.isIdle()) {
         Thread.sleep(1000L);
      }

      System.out.println("segment0 count: " + segment0.getStats().getConnectionCount());
      System.out.println("segment1 count: " + segment1.getStats().getConnectionCount());


      assertTrue(pool.isIdle());
      assertEquals(100 * threadCount, pool.getStats().getConnectionCount());
      assertEquals(0, pool.getStats().getActiveTimeoutCount());
      assertEquals(0, pool.getStats().getFailedAcquisitionCount());
   }

   @Test
   public void multiThreadAcquisitionFail() throws Exception {

      int threadCount = 15;

      List<TestWorker> workers = new ArrayList<TestWorker>(threadCount);

      for(int i = 0; i < threadCount; i++) {
         TestWorker worker = new TestWorker(pool, 80, 10L, true);
         testService.execute(worker);
         workers.add(worker);
      }

      assertTrue(segment1.getStats().isActive());

      int attempts = 0;
      while(attempts++ < 50 && !pool.isIdle()) {
         Thread.sleep(1000L);
      }
      assertEquals(1L, pool.getStats().getSegmentExpansionCount());

      assertTrue(pool.isIdle());
      assertEquals(0, pool.getStats().getActiveTimeoutCount());

      //Wait for second segment to be deactivated
      attempts = 0;
      while(attempts++ < 50 && segment1.getStats().isActive()) {
         Thread.sleep(1000L);
      }

      assertFalse(segment1.getStats().isActive());
   }
}