/*
 * Copyright 2010-2026 Attribyte Labs, LLC
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
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * A "connection" that simulates real work.
 */
public class TestWorker implements Runnable {

   public void run() {
      for(int i = 0; i < numLoops; i++) {
         Connection conn = null;
         try {
            if(segment != null) {
               if(maxConnectionWaitMillis > 0) {
                  conn = segment.open(maxConnectionWaitMillis, TimeUnit.MILLISECONDS);
               } else {
                  conn = segment.open();
               }
            } else {
               conn = pool.getConnection();
            }
            workDelay();
         } catch(Throwable t) {
            numExceptions++;
            lastException = t;
         } finally {
            if(conn != null) {
               try {
                  conn.close();
               } catch(Exception e) {
                  e.printStackTrace();
               }
            } else {
               numFailedConnections++;
            }
         }
      }
   }

   static final Random rnd = new Random(System.currentTimeMillis());

   final long getDelayMillis(long val) {
      if(!randomize) {
         return val;
      } else if(val > 0L) {
         double frac = 100.0 / (rnd.nextInt(100) + 1);
         double delay = (double)val * frac;
         return (long)delay;
      } else {
         return val;
      }

   }

   final void workDelay() {
      long sleepTime = getDelayMillis(maxWorkDelayMillis);
      if(sleepTime > 0L) {
         try {
            Thread.sleep(sleepTime);
         } catch(Exception e) {
            Thread.currentThread().interrupt();
         }
      }
   }

   final long maxConnectionWaitMillis;
   final long maxWorkDelayMillis;
   final boolean randomize;
   final int numLoops;
   final ConnectionPoolSegment segment;
   final ConnectionPool pool;

   int numFailedConnections;
   int numExceptions = 0;
   Throwable lastException = null;

   TestWorker(final ConnectionPool pool,
              final long maxConnectionWaitMillis,
              final int numLoops, final long maxWorkDelayMillis, final boolean randomize) {
      this.pool = pool;
      this.segment = null;
      this.maxConnectionWaitMillis = maxConnectionWaitMillis;
      this.numLoops = numLoops;
      this.maxWorkDelayMillis = maxWorkDelayMillis;
      this.randomize = randomize;
   }

   TestWorker(final ConnectionPoolSegment segment, final long maxConnectionWaitMillis,
              final int numLoops, final long maxWorkDelayMillis, final boolean randomize) {
      this.pool = null;
      this.segment = segment;
      this.maxConnectionWaitMillis = maxConnectionWaitMillis;
      this.numLoops = numLoops;
      this.maxWorkDelayMillis = maxWorkDelayMillis;
      this.randomize = randomize;
   }
}