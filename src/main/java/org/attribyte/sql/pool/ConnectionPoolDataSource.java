/*
 * Copyright 2014-2026 Attribyte Labs, LLC
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


import org.attribyte.api.InitializationException;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * A {@code DataSource} implementation for pools.
 */
public class ConnectionPoolDataSource implements DataSource {

   private final ConnectionPool pool;
   private PrintWriter sourceLogWriter = null;
   private int sourceTimeoutSeconds;

   /**
    * Creates a data source from a pool initializer.
    * @param initializer The initializer.
    * @throws InitializationException if the pool could not be created.
    * @throws SQLException if pool connections could not be created.
    */
   public ConnectionPoolDataSource(final ConnectionPool.Initializer initializer) throws InitializationException, SQLException {
      this(initializer.createPool());
   }

   /**
    * Creates a datasource from a pool.
    * @param pool The pool.
    */
   public ConnectionPoolDataSource(final ConnectionPool pool) {
      this.pool = pool;
   }

   @Override
   public Connection getConnection() throws SQLException {
      return pool.getConnection();
   }

   @Override
   public Connection getConnection(String username, String password) throws SQLException {
      return pool.getConnection();
   }

   @Override
   public synchronized PrintWriter getLogWriter() throws SQLException {
      return sourceLogWriter;
   }

   @Override
   public synchronized void setLogWriter(PrintWriter out) throws SQLException {
      pool.setDataSourceLogWriter(out);
      this.sourceLogWriter = out;
   }

   @Override
   public synchronized void setLoginTimeout(final int sourceTimeoutSeconds) throws SQLException {
      pool.setDataSourceLoginTimeout(sourceTimeoutSeconds);
      this.sourceTimeoutSeconds = sourceTimeoutSeconds;
   }

   @Override
   public synchronized int getLoginTimeout() throws SQLException {
      return sourceTimeoutSeconds;
   }

   @Override
   public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException("Parent logger ");
   }

   @Override
   public <T> T unwrap(Class<T> iface) throws SQLException {
      return null;
   }

   @Override
   public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return false;
   }

   /**
    * Shutdown the underlying pool.
    */
   public void shutdown() {
      pool.shutdown();
   }
}
