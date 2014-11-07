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

/**
 * A clock with 30 second resolution.
 * @author Matt Hamer - Attribyte, LLC
 */
final class Clock {

   /**
    * The clock resolution in milliseconds (30,000).
    */
   static final long RESOLUTION_MILLIS = 30000L;

   /**
    * Updates the clock every 30 seconds.
    */
   private static final Thread clockThread = new Thread(
           new Runnable() {
              public void run() {
                 while(true) {
                    try {
                       currTimeMillis = System.currentTimeMillis();
                       Thread.sleep(RESOLUTION_MILLIS);
                    } catch(InterruptedException ie) {
                       return;
                    }
                 }
              }
           });

   static {
      clockThread.setDaemon(true);
      clockThread.start();
   }

   /**
    * The current time, +- 30s.
    */
   volatile static long currTimeMillis = System.currentTimeMillis();
}