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

import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A clock with 30 second resolution.
 * @author Matt Hamer - Attribyte, LLC
 */
final class Clock {

   /**
    * The clock resolution in milliseconds ({@value}).
    */
   static final long RESOLUTION_MILLIS = 30000L;

   /**
    * The current time, +- 30s.
    */
   volatile static long currTimeMillis = System.currentTimeMillis();

   /**
    * Periodically update the clock time.
    */
   static final ScheduledExecutorService clockService =
           MoreExecutors.getExitingScheduledExecutorService(
           new ScheduledThreadPoolExecutor(1,
                   Util.createThreadFactoryBuilder("LowResClock"))
   );

   static {
      clockService.scheduleAtFixedRate(() -> {
         currTimeMillis = System.currentTimeMillis();
      }, 0, RESOLUTION_MILLIS, TimeUnit.MILLISECONDS);
   }

   /**
    * Shuts down the clock executor immediately.
    */
   static void shutdown() {
      clockService.shutdownNow();
   }
}
