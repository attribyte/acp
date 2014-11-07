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

import java.sql.*;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executor;

/**
 * A "connection" that simulates real work.
 */
public class TestConnection implements Connection {

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

   final void closeDelay() {
      long sleepTime = getDelayMillis(maxCloseDelayMillis);
      if(sleepTime > 0L) {
         try {
            Thread.sleep(sleepTime);
         } catch(Exception e) {
            Thread.currentThread().interrupt();
         }
      }
   }

   final void openDelay() {
      long sleepTime = getDelayMillis(maxOpenDelayMillis);
      if(sleepTime > 0L) {
         try {
            Thread.sleep(sleepTime);
         } catch(Exception e) {
            Thread.currentThread().interrupt();
         }
      }
   }

   final long maxOpenDelayMillis;
   final long maxCloseDelayMillis;
   final boolean randomize;

   TestConnection(final long maxOpenDelayMillis, final long maxCloseDelayMillis, final boolean randomize) {
      this.maxOpenDelayMillis = maxOpenDelayMillis;
      this.maxCloseDelayMillis = maxCloseDelayMillis;
      this.randomize = randomize;
      openDelay();
   }

   public void close() throws SQLException {
      closeDelay();
   }

   public Statement createStatement() throws SQLException {
      return new TestStatement();
   }

   public PreparedStatement prepareStatement(String sql) throws SQLException {
      return null;
   }

   public CallableStatement prepareCall(String sql) throws SQLException {
      return null;
   }

   public String nativeSQL(String sql) throws SQLException {
      return null;
   }

   public void setAutoCommit(boolean autoCommit) throws SQLException {
   }

   public boolean getAutoCommit() throws SQLException {
      return false;
   }

   public void commit() throws SQLException {
   }

   public void rollback() throws SQLException {
   }

   public boolean isClosed() throws SQLException {
      return true;
   }

   public DatabaseMetaData getMetaData() throws SQLException {
      return null;
   }

   public void setReadOnly(boolean readOnly) throws SQLException {
   }

   public boolean isReadOnly() throws SQLException {
      return false;
   }

   public void setCatalog(String catalog) throws SQLException {
   }

   public String getCatalog() throws SQLException {
      return null;
   }

   public void setTransactionIsolation(int level) throws SQLException {
   }

   public int getTransactionIsolation() throws SQLException {
      return 0;
   }

   public SQLWarning getWarnings() throws SQLException {
      return null;
   }

   public void clearWarnings() throws SQLException {
   }

   public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
      return new TestStatement();
   }

   public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      return null;
   }

   public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      return null;
   }

   public java.util.Map<String, Class<?>> getTypeMap() throws SQLException {
      return null;
   }

   public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException {
   }

   public void setHoldability(int holdability) throws SQLException {
   }

   public int getHoldability() throws SQLException {
      return 0;
   }

   public Savepoint setSavepoint() throws SQLException {
      return null;
   }

   public Savepoint setSavepoint(String name) throws SQLException {
      return null;
   }

   public void rollback(Savepoint savepoint) throws SQLException {
   }

   public void releaseSavepoint(Savepoint savepoint) throws SQLException {
   }

   public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return new TestStatement();
   }

   public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return null;
   }

   public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return null;
   }

   public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
      return null;
   }

   public PreparedStatement prepareStatement(String sql, int columnIndexes[]) throws SQLException {
      return null;
   }

   public PreparedStatement prepareStatement(String sql, String columnNames[]) throws SQLException {
      return null;
   }

   public Clob createClob() throws SQLException {
      return null;
   }

   public Blob createBlob() throws SQLException {
      return null;
   }

   public NClob createNClob() throws SQLException {
      return null;
   }

   public SQLXML createSQLXML() throws SQLException {
      return null;
   }

   public boolean isValid(int timeout) throws SQLException {
      return false;
   }

   public void setClientInfo(String name, String value) throws SQLClientInfoException {
   }

   public void setClientInfo(Properties properties) throws SQLClientInfoException {
   }

   public String getClientInfo(String name) throws SQLException {
      return null;
   }

   public Properties getClientInfo() throws SQLException {
      return null;
   }

   public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
      return null;
   }

   public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
      return null;
   }

   public <T> T unwrap(Class<T> iface) throws SQLException {
      return null;
   }

   public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return false;
   }

   /* Java 7 - JDBC 4 */

   public void setSchema(String schema) throws SQLException {
      throw new SQLFeatureNotSupportedException();
   }

   public String getSchema() throws SQLException {
      throw new SQLFeatureNotSupportedException();
   }

   public void abort(Executor executor) throws SQLException {
      throw new SQLFeatureNotSupportedException();
   }

   public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
      throw new SQLFeatureNotSupportedException();
   }

   public int getNetworkTimeout() throws SQLException {
      throw new SQLFeatureNotSupportedException();
   }
}