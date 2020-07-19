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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Various utility methods and constants.
 */
final class Util {

   /**
    * The system's line separator.
    */
   static final String NEW_LINE = System.getProperty("line.separator");

   /**
    * Examine a <tt>Throwable</tt> to preserve <tt>RuntimeException</tt> and <tt>Error</tt>, otherwise throw a <tt>SQLException</tt>.
    * @param t The exception. Ignored if <tt>null</tt>.
    * @throws SQLException The exception.
    */
   static final void throwException(final Throwable t) throws SQLException {
      throwException(t, null);
   }

   /**
    * Examine a <tt>Throwable</tt> to preserve <tt>RuntimeException</tt> and <tt>Error</tt>, otherwise throw a <tt>SQLException</tt>.
    * @param t The exception. Ignored if <tt>null</tt>.
    * @param message A message to be added to the <tt>SQLException</tt>.
    * @throws java.sql.SQLException The exception.
    */
   static final void throwException(final Throwable t, final String message) throws SQLException {
      if(t != null) {
         if(t instanceof SQLException) {
            throw (SQLException)t;
         } else if(t instanceof Error) {
            throw (Error)t;
         } else if(t instanceof RuntimeException) {
            throw (RuntimeException)t;
         } else {
            if(message != null) {
               throw new SQLException(message, t);
            } else {
               throw new SQLException(t);
            }
         }
      }
   }

   /**
    * Determines if a <tt>Throwable</tt> is a runtime error.
    * @param t The <tt>Throwable</tt>.
    * @return Is the <tt>Throwable</tt> a runtime error?
    */
   static final boolean isRuntimeError(final Throwable t) {
      return (t instanceof Error || t instanceof RuntimeException);
   }

   /**
    * Gets the current stack as a string.
    * @return The stack as a string.
    */
   static final String getStack() {
      return getStack(false);
   }

   /**
    * Gets the current stack as a string. Excludes calls from the pool.
    * @return The stack as a string.
    */
   static final String getFilteredStack() {
      return getStack(true);
   }

   /**
    * Gets the current stack as a string.
    * @param filter Should calls from the pool be filtered?
    * @return The stack as a string.
    */
   static final String getStack(final boolean filter) {
      StringBuilder buf = new StringBuilder();
      StackTraceElement[] stack = Thread.currentThread().getStackTrace();
      for(int i = 2; i < stack.length; i++) {
         if(!filter) {
            buf.append(stack[i].toString());
            buf.append(NEW_LINE);
         } else {
            String s = stack[i].toString();
            if(!s.startsWith("org.attribyte.sql.pool")) {
               buf.append(s);
               buf.append(NEW_LINE);
            }
         }
      }
      return buf.toString();
   }


   /**
    * Creates a thread factory builder.
    * @param componentName The (required) component name.
    * @return The thread factory.
    */
   static ThreadFactory createThreadFactoryBuilder(final String componentName) {
      return createThreadFactoryBuilder(null, componentName);
   }

   /**
    * Creates a thread factory builder.
    * @param baseName The (optional) base name.
    * @param componentName The (required) component name.
    * @return The thread factory.
    */
   static ThreadFactory createThreadFactoryBuilder(final String baseName, final String componentName) {
      Preconditions.checkNotNull(Strings.emptyToNull(componentName));
      StringBuilder buf = new StringBuilder("ACP:");
      if(!Strings.isNullOrEmpty(baseName)) {
         buf.append(baseName).append(":").append(componentName);
      } else {
         buf.append(componentName);
      }

      buf.append("-Thread-%d");
      return new ThreadFactoryBuilder().setNameFormat(buf.toString()).build();
   }

   /**
    * The executor for time limiters.
    */
   static final ExecutorService timeLimiterExecutor =
           MoreExecutors.getExitingExecutorService(new ThreadPoolExecutor(1, 256,
                   60L, TimeUnit.SECONDS,
                   new SynchronousQueue<>(), createThreadFactoryBuilder("SimpleTimeLimiter")));

   /**
    * The shared time limiter.
    */
   static final SimpleTimeLimiter timeLimiter = SimpleTimeLimiter.create(timeLimiterExecutor);

   static {
      Runtime.getRuntime().addShutdownHook(new Thread(timeLimiterExecutor::shutdownNow));
   }
}
