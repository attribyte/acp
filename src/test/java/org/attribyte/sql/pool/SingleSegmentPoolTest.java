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

import java.util.concurrent.TimeUnit;

import org.junit.*;

import static org.junit.Assert.*;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.attribyte.api.ConsoleLogger;

/**
 * Pool tests.
 * <p>
 * Some of these tests are performed with methods and variables
 * that have only package visibility.
 * </p>
 */
public class SingleSegmentPoolTest {

   private ConnectionPoolSegment segment0;
   private ConnectionPool pool;
   private final TestDataSource datasource = new TestDataSource(0L, 0L, false);
   private static final int MAX_THREADS = 5;
   private final ExecutorService testService = Executors.newFixedThreadPool(MAX_THREADS * 2);

   @Before
   public void createPool() throws Exception {

      JDBConnection jdbcConnection = new JDBConnection("test", "", "", datasource, 0L, "SELECT 1 FROM test", 400L, false);

      ConnectionPoolSegment.Initializer segmentInit = new ConnectionPoolSegment.Initializer();
      segment0 = segmentInit.setName("segment-0")
              .setConnection(jdbcConnection)
              .setAcquireTimeout(10, TimeUnit.MILLISECONDS)
              .setActiveTimeout(5, TimeUnit.MINUTES)
              .setActiveTimeoutMonitorFrequency(5, TimeUnit.SECONDS)
              .setCloseConcurrency(1)
              .setConnectionLifetime(1, TimeUnit.HOURS)
              .setMaxConcurrentReconnects(1)
              .setMaxReconnectDelay(1, TimeUnit.SECONDS)
              .setSize(MAX_THREADS)
              .setTestOnLogicalClose(false)
              .setTestOnLogicalOpen(false)
              .createSegment();

      ConnectionPool.Initializer poolInit = new ConnectionPool.Initializer();
      pool = poolInit.setName("test-pool")
              .addActiveSegment(segment0)
              .setMinActiveSegments(1)
              .setLogger(new ConsoleLogger()).createPool();
   }

   @After
   public void destroySegment() throws Exception {
      try {
         pool.shutdown();
      } catch(Exception e) {
         e.printStackTrace();
         throw e;
      }

   }

   @Test
   public void multiThreadNoFail() throws Exception {

      int threadCount = 2;

      for(int i = 0; i < threadCount; i++) {
         TestWorker worker = new TestWorker(pool, 100, 10L, true); //Work up to 10ms
         Thread.sleep(1000L);
         testService.execute(worker);
      }

      int attempts = 0;
      while(attempts++ < 10 && !pool.isIdle()) {
         Thread.sleep(1000L);
      }

      assertTrue(pool.isIdle());
      assertEquals(100 * threadCount, pool.getStats().getConnectionCount());
      assertEquals(0, pool.getStats().getActiveTimeoutCount());
      assertEquals(0, pool.getStats().getFailedAcquisitionCount());
   }

   @Test
   public void multiThreadAcquisitionFail() throws Exception {

      int threadCount = 10;

      List<TestWorker> workers = new ArrayList<TestWorker>(threadCount);

      for(int i = 0; i < threadCount; i++) {
         TestWorker worker = new TestWorker(pool, 100, 10L, true);
         testService.execute(worker);
         workers.add(worker);
      }

      int attempts = 0;
      while(attempts++ < 10 && !pool.isIdle()) {
         Thread.sleep(1000L);
      }

      assertTrue(pool.isIdle());
      assertEquals(0, pool.getStats().getActiveTimeoutCount());
      assertTrue(pool.getStats().getFailedAcquisitionCount() > 0);

      Throwable lastException = null;
      for(TestWorker worker : workers) {
         if(worker.lastException != null) {
            lastException = worker.lastException;
            break;
         }
      }

      assertNotNull(lastException);
   }
}