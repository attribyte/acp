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

import com.google.common.base.Strings;
import org.attribyte.api.InitializationException;

import javax.sql.DataSource;

/**
 * A JDBC connection.
 */
public class JDBConnection {

   /**
    * Initialize an immutable connection.
    */
   public static class Initializer {

      /**
       * Sets the connection name.
       * @param name The connection name.
       * @return A self-reference.
       */
      public Initializer setName(final String name) {
         this.name = name;
         return this;
      }

      /**
       * Sets the user.
       * @param user The user.
       * @return A self-reference.
       */
      public Initializer setUser(final String user) {
         this.user = user;
         return this;
      }

      /**
       * Sets the password.
       * @param password The password.
       * @return A self-reference.
       */
      public Initializer setPassword(final String password) {
         this.password = password;
         return this;
      }

      /**
       * Sets the connection string.
       * @param connectionString The connection string.
       * @return A self-reference.
       */
      public Initializer setConnectionString(final String connectionString) {
         this.connectionString = connectionString;
         return this;
      }

      /**
       * Sets the SQL statement used to test connections.
       * @param testSQL The test SQL.
       * @return A self-reference.
       */
      public Initializer setTestSQL(final String testSQL) {
         this.testSQL = testSQL;
         return this;
      }

      /**
       * Sets the connection test interval in milliseconds.
       * @param testIntervalMillis The test interval in milliseconds.
       * @return A self-reference.
       */
      public Initializer setTestIntervalMillis(final long testIntervalMillis) {
         this.testIntervalMillis = testIntervalMillis;
         return this;
      }

      /**
       * Sets a timeout use when creating new connections. May be <code>0</code>.
       * @param createTimeoutMillis The timeout in milliseconds.
       * @return A self-reference.
       */
      public Initializer setCreateTimeoutMillis(final long createTimeoutMillis) {
         this.createTimeoutMillis = createTimeoutMillis;
         return this;
      }

      /**
       * Sets a <code>DataSource</code> as the source of connections.
       * @param datasource The datasource.
       * @return A self-reference.
       */
      public Initializer setDatasource(final DataSource datasource) {
         this.datasource = datasource;
         return this;
      }

      /**
       * Sets debug mode.
       * <p>
       * When debug mode is <code>true</code> the call site of connection
       * acquisitions is recorded and available for exceptions.
       * </p>
       * @param debug The deebug mode.
       * @return A self-reference.
       */
      public Initializer setDebug(final boolean debug) {
         this.debug = debug;
         return this;
      }

      /**
       * Creates an immutable connection from this initialization.
       * @return The connection.
       */
      public JDBConnection createConnection() {
         if(datasource == null) {
            return new JDBConnection(name, user, password, connectionString,
                    createTimeoutMillis, testSQL, testIntervalMillis, debug);
         } else {
            return new JDBConnection(name, user, password, datasource,
                    createTimeoutMillis, testSQL, testIntervalMillis, debug);
         }
      }

      String name;
      String user;
      String password;
      String connectionString;
      String testSQL;
      long testIntervalMillis = 60L * 1000L;
      long createTimeoutMillis = 60L * 1000L;
      DataSource datasource;
      boolean debug = false;
   }

   /**
    * The SQLSTATE code for connection failure (08006).
    */
   static final String SQLSTATE_CONNECTION_FAILURE = "08006";

   /**
    * The SQLSTATE code for connection exception (08000).
    */
   static final String SQLSTATE_CONNECTION_EXCEPTION = "08000";

   /**
    * The SQLSTATE code for invalid transaction (25000).
    */
   static final String SQLSTATE_INVALID_TRANSACTION_STATE = "25000";

   /**
    * Creates a connection with a connection string.
    * @param name The connection name.
    * @param user The database user.
    * @param password The database password.
    * @param connectionString The database connection string.
    * @param createTimeoutMillis The connection create timeout.
    * @param testSQL The test SQL.
    * @param testIntervalMillis The test frequency.
    * @param debug Should debug information be recorded?
    */
   public JDBConnection(final String name, final String user, final String password, final String connectionString,
                        final long createTimeoutMillis,
                        final String testSQL, final long testIntervalMillis, final boolean debug) {
      this.name = name;
      this.user = user;
      this.password = password;
      this.connectionString = connectionString;
      this.createTimeoutMillis = createTimeoutMillis;
      this.testSQL = testSQL;
      this.testIntervalMillis = testIntervalMillis;
      this.datasource = null;
      this.debug = debug;
   }

   /**
    * Creates a connection with a <tt>DataSource</tt>.
    * @param name The connection name.
    * @param user The database user.
    * @param password The database password.
    * @param datasource The <tt>DataSource</tt>.
    * @param createTimeoutMillis The connection create timeout.
    * @param testSQL The test SQL.
    * @param testIntervalMillis The test frequency.
    * @param debug Should debug information be recorded?
    */
   public JDBConnection(final String name, final String user, final String password, final DataSource datasource,
                        final long createTimeoutMillis,
                        final String testSQL, final long testIntervalMillis, final boolean debug) {
      this.name = name;
      this.user = user;
      this.password = password;
      this.connectionString = null;
      this.createTimeoutMillis = createTimeoutMillis;
      this.testSQL = testSQL;
      this.datasource = datasource;
      this.testIntervalMillis = testIntervalMillis;
      this.debug = debug;
   }

   /**
    * Verify that required parameters are set.
    * @throws InitializationException if required parameters are not set.
    */
   public void validate() throws InitializationException {
      if(Strings.isNullOrEmpty(connectionString) && datasource == null) {
         throw new InitializationException("Either a 'connectionString' or DataSource must be specified");
      }
   }


   /**
    * Gets a description of the database connection.
    * @return A description of the connection.
    */
   final String getConnectionDescription() {

      if(datasource != null) {
         return name;
      }

      String description = connectionString;
      int index = connectionString != null ? connectionString.indexOf('?') : 0;
      if(index > 0) {
         description = connectionString.substring(0, index);
      }

      if(!Strings.isNullOrEmpty(user)) {
         return user + "@" + description;
      } else {
         return description;
      }
   }

   @Override
   public String toString() {
      StringBuilder buf = new StringBuilder();
      if(connectionString != null) {
         buf.append(user).append("@").append(connectionString);
      } else if(datasource != null) {
         buf.append("DataSource: ").append(datasource.toString());
      }

      if(testSQL != null) {
         buf.append(", ").append(testSQL);
      }

      if(testIntervalMillis > 0) {
         buf.append(", test interval = ").append(testIntervalMillis);
      }
      return buf.toString();
   }

   final String name;
   final String user;
   final String password;
   final String connectionString;
   final String testSQL;
   final long testIntervalMillis;
   final long createTimeoutMillis;
   final DataSource datasource;
   final boolean debug;
}   
