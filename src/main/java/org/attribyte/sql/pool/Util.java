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

import java.sql.SQLException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.attribyte.api.InitializationException;
import org.attribyte.util.DOMUtil;
import org.attribyte.util.StringUtil;
import org.w3c.dom.Element;

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
      return t != null && (t instanceof Error || t instanceof RuntimeException);
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
    * Gets milliseconds from a time string.
    * <p>e.g.</p>
    * <ul>
    * <li>100ms</li>
    * <li>5s</li>
    * <li>30m</li>
    * <li>5h</li>
    * <li>3d</li>
    * </ul>
    * @param time The time string.
    * @param defaultTime The default time to return if time string is unspecified or units are invalid.
    * @return The milliseconds, or 0 if unspecified.
    */
   static final long millis(String time, final long defaultTime) {

      if(time == null || time.trim().length() == 0) {
         return defaultTime;
      }

      time = time.trim();

      long mult = 0L;
      if(time.endsWith("ms")) {
         time = time.substring(0, time.length() - 2);
         mult = 1L;
      } else if(time.endsWith("s")) {
         time = time.substring(0, time.length() - 1);
         mult = 1000L;
      } else if(time.endsWith("m")) {
         time = time.substring(0, time.length() - 1);
         mult = 60000L;
      } else if(time.endsWith("h")) {
         time = time.substring(0, time.length() - 1);
         mult = 3600L * 1000L;
      } else if(time.endsWith("d")) {
         time = time.substring(0, time.length() - 1);
         mult = 3600L * 1000L * 24L;
      }

      long millis = Long.parseLong(time) * mult;
      if(millis < 1) {
         return defaultTime;
      } else {
         return millis;
      }
   }

   /**
    * Get milliseconds from an element.
    * <p>
    * Expects time="10ms" or time="10" timeUnit="milliseconds".
    * </p>
    * @param parentElem The parent element.
    * @param elemName The child element name.
    * @param defaultValue The default value returned if the element is not found.
    * @return The time in milliseconds.
    * @throws InitializationException on invalid units or format.
    */
   static final long millisFromElem(final Element parentElem, final String elemName, final long defaultValue)
           throws InitializationException {
      return millisFromElem(parentElem, "time", elemName, defaultValue);
   }

   /**
    * Get milliseconds from an element.
    * <p>
    * Expects time="10ms" or time="10" timeUnit="milliseconds".
    * </p>
    * @param parentElem The parent element.
    * @param attribName The time attribute name.
    * @param elemName The child element name.
    * @param defaultValue The default value returned if the element is not found.
    * @return The time in milliseconds.
    * @throws InitializationException on invalid units or format.
    */
   static final long millisFromElem(final Element parentElem, final String attribName, final String elemName, final long defaultValue)
           throws InitializationException {

      Element elem = DOMUtil.getFirstChild(parentElem, elemName);
      if(elem != null) {
         String time = elem.getAttribute(attribName).toLowerCase();
         if(time.length() == 0) {
            throw new InitializationException("A 'time' must be specified for '" + elemName + "'");
         }

         long mult = 0L;
         if(time.endsWith("ms")) {
            time = time.substring(0, time.length() - 2);
            mult = 1L;
         } else if(time.endsWith("s")) {
            time = time.substring(0, time.length() - 1);
            mult = 1000L;
         } else if(time.endsWith("m")) {
            time = time.substring(0, time.length() - 1);
            mult = 60000L;
         } else if(time.endsWith("h")) {
            time = time.substring(0, time.length() - 1);
            mult = 3600L * 1000L;
         } else if(time.endsWith("d")) {
            time = time.substring(0, time.length() - 1);
            mult = 3600L * 1000L * 24L;
         }

         String unit = elem.getAttribute("timeUnit");
         if(mult == 0L && unit.length() == 0) {
            throw new InitializationException("The 'time' for '" + elemName + "' must have units (ms,s,m,h,d)");
         } else if(unit.length() > 0) {
            TimeUnit timeUnit = TimeUnit.valueOf(unit.trim().toUpperCase());
            if(timeUnit == null) {
               throw new InitializationException("The 'timeUnit' for '" + elemName + "' is invalid");
            } else {
               mult = TimeUnit.MILLISECONDS.convert(1L, timeUnit);
            }
         }

         try {
            long timeVal = Integer.parseInt(time);
            return timeVal * mult;
         } catch(Exception e) {
            throw new InitializationException("The 'time' must be an integer for '" + elemName + "'");
         }
      } else {
         return defaultValue;
      }
   }

   /**
    * Creates a thread factory builder.
    * @param baseName The (optional) base name.
    * @param componentName The (required) component name.
    * @return The thread factory.
    */
   static ThreadFactory createThreadFactoryBuilder(final String baseName, final String componentName) {

      StringBuilder buf = new StringBuilder("ACP:");
      if(StringUtil.hasContent(baseName)) {
         buf.append(baseName).append(":").append(componentName);
      } else {
         buf.append(componentName);
      }

      buf.append("-Thread-%d");
      return new ThreadFactoryBuilder().setNameFormat(buf.toString()).build();
   }
}