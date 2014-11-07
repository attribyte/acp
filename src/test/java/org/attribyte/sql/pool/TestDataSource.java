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
import java.sql.SQLException;
import java.io.PrintWriter;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * A "connection" that simulates real work.
 */
public class TestDataSource implements javax.sql.DataSource {

   TestDataSource(final long maxOpenDelayMillis, final long maxCloseDelayMillis, final boolean randomize) {
      this.maxOpenDelayMillis = maxOpenDelayMillis;
      this.maxCloseDelayMillis = maxCloseDelayMillis;
      this.randomize = randomize;
   }

   final long maxOpenDelayMillis;
   final long maxCloseDelayMillis;
   final boolean randomize;

   volatile boolean failed = false;

   final AtomicLong connectionCount = new AtomicLong(0L);

   public int getLoginTimeout() {
      return 0;
   }

   public PrintWriter getLogWriter() {
      return null;
   }

   public void setLoginTimeout(int timeout) {

   }

   public void setLogWriter(PrintWriter writer) {
   }

   public Connection getConnection() throws SQLException {
      if(!failed) {
         return new TestConnection(maxOpenDelayMillis, maxCloseDelayMillis, randomize);
      } else {
         throw new SQLException("Unable to create conection");
      }
   }

   public Connection getConnection(String u, String p) throws SQLException {
      if(!failed) {
         return new TestConnection(maxOpenDelayMillis, maxCloseDelayMillis, randomize);
      } else {
         throw new SQLException("Unable to create conection");
      }
   }

   public <T> T unwrap(Class<T> iface) throws SQLException {
      throw new SQLException("Not a wrapper");
   }

   public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return false;
   }

   /* JDK 1.7 / JDBC 4 */

   public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
   }
}