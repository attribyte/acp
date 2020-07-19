/*
 * Copyright 2010-2020 Attribyte, LLC
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
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;

import java.sql.*;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wraps a SQL connection.
 */
public class ConnectionPoolConnection implements Connection {

   /**
    * How are incomplete transactions handled on connection close?
    */
   public enum IncompleteTransactionPolicy {

      /**
       * Exception is raised.
       */
      REPORT,

      /**
       * Transaction is committed.
       */
      COMMIT,

      /**
       * Transaction is rolled-back (default).
       */
      ROLLBACK;

      /**
       * Gets the policy from a string.
       * <p>
       * Expect 'report', 'commit' or 'rollback'.
       * </p>
       * @param str The string.
       * @param defaultPolicy The default policy.
       * @return The policy, or default if string is invalid or undefined.
       */
      static IncompleteTransactionPolicy fromString(final String str, final IncompleteTransactionPolicy defaultPolicy) {
         switch(Strings.nullToEmpty(str).toLowerCase().trim()) {
            case "report": return REPORT;
            case "commit": return COMMIT;
            case "rollback": return ROLLBACK;
            default: return defaultPolicy;
         }
      }
   }

   /**
    * How are open statements after close handled?
    */
   public enum ForceRealClosePolicy {

      /**
       * Closes the connection on force real close.
       */
      CONNECTION,

      /**
       * Closes the connection on force real close <em>with a time limit</em>.
       * <p>
       * If the connection close fails to complete, the connection is left
       * in an undefined state.
       * </p>
       */
      CONNECTION_WITH_LIMIT,

      /**
       * Attempts to close all open statements before connection on force real close.
       */
      STATEMENTS_AND_CONNECTION,

      /**
       * Do nothing. Don't even call real close.
       */
      NONE;

      /**
       * Gets the policy from a string.
       * <p>
       * Expect 'connection' or 'statements_and_connection'.
       * </p>
       * @param str The string.
       * @param defaultPolicy The default policy.
       * @return The policy, or default if string is invalid or undefined.
       */
      static ForceRealClosePolicy fromString(final String str, final ForceRealClosePolicy defaultPolicy) {

         switch(Strings.nullToEmpty(str).toLowerCase().trim()) {
            case "connection":
               return CONNECTION;
            case "statements_and_connection":
            case "statements-and-connection":
            case "statementsandconnection":
               return STATEMENTS_AND_CONNECTION;
            case "none":
               return NONE;
            case "connection_with_limit":
            case "connection-with-limit":
            case "connectionwithlimit":
               return CONNECTION_WITH_LIMIT;
            default:
               return defaultPolicy;
         }
      }
   }

   /**
    * How are open statements after close handled?
    */
   public enum OpenStatementPolicy {

      /**
       * Don't track open statements.
       */
      NONE,

      /**
       * Closes statements silently, only reporting exceptions if they happen during statement close.
       */
      SILENT,

      /**
       * Closes statements and raises an application exception.
       */
      REPORT;

      /**
       * Gets the policy from a string.
       * <p>
       * Expect 'silent' or 'report'
       * </p>
       * @param str The string.
       * @param defaultPolicy The default policy.
       * @return The policy, or default if string is invalid or undefined.
       */
      static OpenStatementPolicy fromString(final String str, final OpenStatementPolicy defaultPolicy) {
         switch(Strings.nullToEmpty(str).toLowerCase().trim()) {
            case "none": return NONE;
            case "silent": return SILENT;
            case "report": return REPORT;
            default: return defaultPolicy;
         }
      }
   }

   /**
    * How are connections that have exceeded the activity timeout handled?
    */
   public enum ActivityTimeoutPolicy {

      /**
       * Causes the connection to be closed, even though operations may be pending in another thread.
       * In general, this is probably not thread-safe and deadlock is possible
       * if connection operations are pending in the application thread.
       */
      FORCE_CLOSE,

      /**
       * Timeout is logged, but the connection remains active
       * and unavailable until it is returned to the pool.
       */
      LOG;


      /**
       * Gets the policy from a string.
       * <p>
       * Expect 'force_close' or 'log'.
       * </p>
       * @param str The string.
       * @param defaultPolicy The default policy.
       * @return The policy, or default if string is invalid or undefined.
       */
      static ActivityTimeoutPolicy fromString(final String str, final ActivityTimeoutPolicy defaultPolicy) {

         switch(Strings.nullToEmpty(str).trim().toLowerCase()) {
            case "force_close":
            case "force-close":
            case "forceclose":
               return FORCE_CLOSE;
            case "log":
               return LOG;
            default:
               return defaultPolicy;
         }
      }
   }

   /**
    * Defines transaction states for this connection.
    */
   enum TransactionState {

      /**
       * Connection is (logically) closed.
       */
      CLOSED,

      /**
       * No transaction.
       */
      NONE,

      /**
       * Transaction started.
       */
      STARTED,

      /**
       * Transaction completed with commit or rollback.
       */
      COMPLETED
   }

   /**
    * Connection is available.
    */
   static final int STATE_AVAILABLE = 0;

   /**
    * Connection is open.
    */
   static final int STATE_OPEN = 1;

   /**
    * Connection is being closed.
    */
   static final int STATE_CLOSING = 2;

   /**
    * Connection is being reopened.
    */
   static final int STATE_REOPENING = 3;

   /**
    * Connection is closed and disconnected from the DB.
    */
   static final int STATE_DISCONNECTED = 4;

   /**
    * Creates a connection.
    * @param segment The segment this connection is part of.
    * @param id The connection id.
    * @param testSQL SQL used to test the connection.
    * @param debug Should debug data be recorded?
    * @param incompleteTransactionPolicy The incomplete transaction policy.
    * @param openStatementPolicy The open statements policy.
    * @param forceRealClosePolicy The force-real-close policy.
    * @param closeTimeLimitMillis The close time limit in milliseconds.
    */
   protected ConnectionPoolConnection(final ConnectionPoolSegment segment, final String id, final String testSQL, final boolean debug,
                                      final IncompleteTransactionPolicy incompleteTransactionPolicy,
                                      final OpenStatementPolicy openStatementPolicy,
                                      final ForceRealClosePolicy forceRealClosePolicy,
                                      final long closeTimeLimitMillis) {
      this.segment = segment;
      this.id = id;
      this.testSQL = Strings.emptyToNull(testSQL);
      this.incompleteTransactionPolicy = incompleteTransactionPolicy;
      this.openStatementPolicy = openStatementPolicy;
      this.forceRealClosePolicy = forceRealClosePolicy;
      this.openStatements = forceRealClosePolicy == ForceRealClosePolicy.STATEMENTS_AND_CONNECTION ?
              Sets.newConcurrentHashSet() : Sets.newHashSet();
      this.debug = debug;
      this.closeTimeLimitMillis = closeTimeLimitMillis;
   }

   /**
    * Segment this connection belongs to.
    */
   private final ConnectionPoolSegment segment;

   /**
    * Id assigned to this connection.
    */
   final String id;

   /**
    * SQL used to test the real connection.
    */
   private final String testSQL;

   /**
    * The incomplete transaction policy.
    */
   private final IncompleteTransactionPolicy incompleteTransactionPolicy;

   /**
    * How open statements after connection close are dealt with.
    */
   private final OpenStatementPolicy openStatementPolicy;

   /**
    * The policy on "force real close"
    */
   private final ForceRealClosePolicy forceRealClosePolicy;

   /**
    * Should debug (including trace) be preserved?
    */
   private final boolean debug;

   /**
    * The last stack trace.
    */
   private volatile String lastTrace;

   /**
    * Real connection.
    */
   private Connection conn;

   /**
    * A set containing all created statements to ensure they are
    * closed when connection is returned to pool.
    */
   private final Set<Statement> openStatements;

   /**
    * Random number generator.
    */
   private static final Random rnd = new Random();

   /**
    * Time this connection was opened for use.
    */
   volatile long openTime;

   /**
    * Exception raised during logical close.
    */
   Throwable logicalCloseException;

   /**
    * Time the real connection was created.
    */
   volatile long realOpenTime;

   /**
    * Time the real connection was last tested.
    */
   volatile long lastTestTime;

   /**
    * State of this connection.
    */
   final AtomicInteger state = new AtomicInteger(STATE_AVAILABLE);

   /**
    * The current transaction state.
    */
   private volatile TransactionState transactionState = TransactionState.CLOSED; //Note volatile for forceRealClose() <-> close()

   /**
    * The current number of reopen attempts.
    */
   int reopenAttempts;

   /**
    * The close time limit if policy is configured.
    */
   private final long closeTimeLimitMillis;

   /**
    * Returns this connection to the segment without change to the real connection.
    * <p>
    *   Ignores close after the first.
    * </p>
    */
   @Override
   public void close() throws SQLException {

      switch(transactionState) {
         case CLOSED: //Segment close will ignore multiple close on same connection, but need to skip the other stuff.
            return;
         case NONE: //Autocommit never changed
            transactionState = TransactionState.CLOSED;
            break;
         default:
            try {
               resolveIncompleteTransactions();
               transactionState = TransactionState.CLOSED;
            } catch(Throwable t) {
               setLogicalCloseException(t);
            } finally {
               try {
                  if(!conn.getAutoCommit()) { //Set auto-commit
                     conn.setAutoCommit(true);
                  }
               } catch(Throwable t) {
                  setLogicalCloseException(t);
               }
            }
      }

      boolean openStatements = closeStatements();

      Throwable logicalCloseException = this.logicalCloseException; //Preserve the exception - segment will set to null. 

      segment.close(this);

      if(logicalCloseException != null) { //Now throw to the caller
         Util.throwException(logicalCloseException);
      }

      if(openStatements && openStatementPolicy == OpenStatementPolicy.REPORT) {
         throw new SQLException("Connection has open statements", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }
   }

   /**
    * Sets the connection state to open and records the
    * open time. No tests are performed.
    */
   final void logicalOpen() {
      openTime = Clock.currTimeMillis;
      transactionState = TransactionState.NONE;
      if(debug) {
         lastTrace = Util.getFilteredStack();
      }
   }

   /**
    * Run the connection test query, if configured.
    * @throws SQLException The query fails.
    */
   private final void maybeRunTest() throws SQLException {
      if(testSQL != null) {
         try(Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(testSQL)) {
            Preconditions.checkArgument(rs.next());
         }
      }
   }

   /**
    * Sets the connection state to open and records the
    * open time after performing connection tests.
    * @throws java.sql.SQLException on test error.
    */
   final void logicalOpenWithTest() throws SQLException {

      openTime = Clock.currTimeMillis;
      transactionState = TransactionState.NONE;
      if(debug) {
         lastTrace = Util.getFilteredStack();
      }
      maybeRunTest();
   }

   /**
    * Opens a new (real) database connection <em>if</em>
    * this connection has been {@code realClose}ed.
    * @throws SQLException on open error.
    */
   final void realOpen() throws SQLException {
      if(conn == null) {
         conn = segment.createRealConnection();

         //Don't care about the exact value - randomize with some seconds to make
         //sure connections (if configured) don't expire at the same time even if they
         //are all created at once.
         realOpenTime = Clock.currTimeMillis + rnd.nextInt(120000);
         openStatements.clear(); //If reopen is a result of forceRealClose.
         reopenAttempts = 0;
      }
   }

   /**
    * Prepares the connection for return to the active pool.
    * @param withTest Should the connection be tested?
    * @throws SQLException on test error or exception raised (and thrown) during application close.
    */
   final void logicalClose(final boolean withTest) throws SQLException {

      openTime = 0L;

      if(debug) {
         lastTrace = null;
      }

      if(logicalCloseException != null) { //Set during close() & thrown here to cause segment to reopen.
         Util.throwException(logicalCloseException);
      }

      if(!conn.getAutoCommit()) {
         conn.setAutoCommit(true);
      }

      if(withTest) {
         maybeRunTest();
      }
   }

   /**
    * Check for unclosed statements.
    * @return Were any statements unclosed?
    */
   private boolean closeStatements() {

      // JDBC API says...
      // Calling the method close on a Statement object that is already closed has no effect.
      // Note: When a Statement object is closed, its current ResultSet object, if one exists, is also closed.

      boolean hasUnclosed = false;

      if(openStatements.size() > 0) {
         for(Statement stmt : openStatements) {
            try {
               if(!stmt.isClosed()) {
                  stmt.close();
                  hasUnclosed = true;
               }
            } catch(SQLException e) {
               hasUnclosed = true;
               setLogicalCloseException(e);
            } catch(Throwable t) {
               hasUnclosed = true;
               if(logicalCloseException == null) {
                  if(Util.isRuntimeError(t)) {
                     setLogicalCloseException(t);
                  } else {
                     setLogicalCloseException(new SQLException(t));
                  }
               }
            }
         }

         openStatements.clear();
      }

      return hasUnclosed;
   }

   /**
    * Sets logicalCloseException if null or replaces logicalCloseException
    * if not null && input is a runtime error.
    * @param t The throwable
    */
   private void setLogicalCloseException(final Throwable t) {
      if(logicalCloseException == null) {
         logicalCloseException = t;
      } else if(!Util.isRuntimeError(logicalCloseException) && Util.isRuntimeError(t)) {
         logicalCloseException = t;
      }
   }

   /**
    * Closes the underlying DB connection, ignoring any exception raised.
    * <p>
    * Use the default policy.
    * </p>
    */
   final void forceRealClose() {
      forceRealClose(forceRealClosePolicy);
   }

   /**
    * Terminate this connection.
    */
   final void terminate() {
      conn = null;
      openStatements.clear();
   }

   /**
    * A time-limiter - used for obtaining database connections.
    */
   private static final SimpleTimeLimiter closeTimeLimiter = Util.timeLimiter;

   /**
    * Closes the underlying DB connection, ignoring any exception raised.
    * <p>
    * Use the specified policy.
    * </p>
    * @param policy The policy.
    */
   final void forceRealClose(ForceRealClosePolicy policy) {

      if(conn != null) {

         //Set transaction state to closed so app's close (in finally, hopefully) will do nothing when called.

         transactionState = TransactionState.CLOSED;

         if(policy == ForceRealClosePolicy.STATEMENTS_AND_CONNECTION) {
            for(Statement stmt : openStatements) {
               try {
                  stmt.close();
               } catch(SQLException se) {
                  //Ignore
               }
            }
         }

         if(policy != ForceRealClosePolicy.NONE) {
            if(policy == ForceRealClosePolicy.CONNECTION_WITH_LIMIT) {
               try {
                  closeTimeLimiter.callWithTimeout(() -> {
                     conn.close();
                     return Boolean.TRUE;
                  }, closeTimeLimitMillis, TimeUnit.MILLISECONDS);
               } catch(UncheckedTimeoutException ute) {
                  segment.logError("Unable to close connection after waiting " + closeTimeLimitMillis + " ms");
               } catch(Exception e) {
                  segment.logError("Connection close error", e);
               }
            } else {
               try {
                  conn.close();
               } catch(Exception e) {
                  segment.logError("Connection close error", e);
               }
           }
         }

         conn = null;
         realOpenTime = Long.MAX_VALUE;
         reopenAttempts = 0;
         lastTrace = null;
      }
   }

   /**
    * Attempt to deal with any transaction problems.
    * @throws SQLException on invalid state.
    */
   private void resolveIncompleteTransactions() throws SQLException {

      switch(transactionState) {
         case COMPLETED:
            //All we know for certain is that at least one commit/rollback was called. Do nothing.
            break;
         case STARTED:
            //At least one statement was created with auto-commit false & no commit/rollback.
            //Follow the default policy.
            if(conn != null && !openStatements.isEmpty()) {
               switch(incompleteTransactionPolicy) {
                  case REPORT:
                     throw new SQLException("Statement closed with incomplete transaction", JDBConnection.SQLSTATE_INVALID_TRANSACTION_STATE);
                  case COMMIT:
                     if(!conn.isClosed()) {
                        conn.commit();
                     }
                     break;
                  case ROLLBACK:
                     if(!conn.isClosed()) {
                        conn.rollback();
                     }
               }
            }
            break;
      }
   }

   /**
    * Gets the (on-open) stack trace, if available.
    * @return The stack trace.
    */
   String getTrace() {
      return lastTrace;
   }

   /**
    * Adds statement to open statements set.
    * @param stmt The statement.
    */
   private void openStatement(final Statement stmt) {
      if(openStatementPolicy != OpenStatementPolicy.NONE) {
         openStatements.add(stmt);
      }
   }
   
   /* JDBC */

   public Statement createStatement() throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      if(debug) {
         lastTrace = Util.getFilteredStack();
      }

      Statement stmt = conn.createStatement();
      openStatement(stmt);
      return stmt;
   }

   public PreparedStatement prepareStatement(String sql) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      if(debug) {
         lastTrace = Util.getFilteredStack();
      }

      PreparedStatement stmt = conn.prepareStatement(sql);
      openStatement(stmt);
      return stmt;
   }

   public CallableStatement prepareCall(String sql) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      if(debug) {
         lastTrace = Util.getFilteredStack();
      }

      CallableStatement stmt = conn.prepareCall(sql);
      openStatement(stmt);
      return stmt;
   }

   public String nativeSQL(String sql) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      return conn.nativeSQL(sql);
   }

   public void setAutoCommit(boolean autoCommit) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      conn.setAutoCommit(autoCommit);
      if(!autoCommit) {
         switch(transactionState) {
            case NONE:
               transactionState = TransactionState.STARTED;
               break;
            default:
               transactionState = TransactionState.COMPLETED;
               break;
         }
      } else if(transactionState != TransactionState.NONE) {
         transactionState = TransactionState.COMPLETED;
      }
   }

   public boolean getAutoCommit() throws SQLException {
      return conn.getAutoCommit();
   }

   public void commit() throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      conn.commit();
      transactionState = TransactionState.COMPLETED;
   }

   public void rollback() throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      conn.rollback();
      transactionState = TransactionState.COMPLETED;
   }

   public boolean isClosed() {
      return transactionState == TransactionState.CLOSED;
   }

   public DatabaseMetaData getMetaData() throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      return conn.getMetaData();
   }

   public void setReadOnly(boolean readOnly) throws SQLException {
      //Ignore - this must be set on real connection open. TODO
   }

   public boolean isReadOnly() throws SQLException {
      return conn.isReadOnly();
   }

   public void setCatalog(String catalog) throws SQLException {
      //Ignore - this must be set on real connection open. TODO
   }

   public String getCatalog() throws SQLException {
      return conn.getCatalog();
   }

   public void setTransactionIsolation(int level) throws SQLException {
      //Ignore - this must be set on real connection open. TODO
   }

   public int getTransactionIsolation() throws SQLException {
      return conn.getTransactionIsolation();
   }

   public SQLWarning getWarnings() throws SQLException {
      return conn.getWarnings();
   }

   public void clearWarnings() throws SQLException {
      //Ignore
   }

   public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      if(debug) {
         lastTrace = Util.getFilteredStack();
      }

      Statement stmt = conn.createStatement(resultSetType, resultSetConcurrency);
      openStatement(stmt);
      return stmt;
   }

   public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      if(debug) {
         lastTrace = Util.getFilteredStack();
      }

      PreparedStatement stmt = conn.prepareStatement(sql, resultSetType, resultSetConcurrency);
      openStatement(stmt);
      return stmt;
   }

   public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      if(debug) {
         lastTrace = Util.getFilteredStack();
      }

      CallableStatement stmt = conn.prepareCall(sql, resultSetType, resultSetConcurrency);
      openStatement(stmt);
      return stmt;
   }

   public java.util.Map<String, Class<?>> getTypeMap() throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      return conn.getTypeMap();
   }

   public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException {
      //Ignore - this must be done on real connection construction. TODO.
   }

   public void setHoldability(int holdability) throws SQLException {
      //Ignore - this must be done on real connection construction. TODO.
   }

   public int getHoldability() throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      return conn.getHoldability();
   }

   public Savepoint setSavepoint() throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      return conn.setSavepoint();
   }

   public Savepoint setSavepoint(String name) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      return conn.setSavepoint(name);
   }

   public void rollback(Savepoint savepoint) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      transactionState = TransactionState.COMPLETED;
      conn.rollback(savepoint);
   }

   public void releaseSavepoint(Savepoint savepoint) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      conn.releaseSavepoint(savepoint);
   }

   public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      if(debug) {
         lastTrace = Util.getFilteredStack();
      }

      Statement stmt = conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
      openStatement(stmt);
      return stmt;
   }

   public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      PreparedStatement stmt = conn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
      openStatement(stmt);
      return stmt;
   }

   public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      if(debug) {
         lastTrace = Util.getFilteredStack();
      }

      CallableStatement stmt = conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
      openStatement(stmt);
      return stmt;
   }

   public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      if(debug) {
         lastTrace = Util.getFilteredStack();
      }

      PreparedStatement stmt = conn.prepareStatement(sql, autoGeneratedKeys);
      openStatement(stmt);
      return stmt;
   }

   public PreparedStatement prepareStatement(String sql, int columnIndexes[]) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      if(debug) {
         lastTrace = Util.getFilteredStack();
      }

      PreparedStatement stmt = conn.prepareStatement(sql, columnIndexes);
      openStatement(stmt);
      return stmt;
   }

   public PreparedStatement prepareStatement(String sql, String columnNames[]) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      if(debug) {
         lastTrace = Util.getFilteredStack();
      }

      PreparedStatement stmt = conn.prepareStatement(sql, columnNames);
      openStatement(stmt);
      return stmt;
   }

   public Clob createClob() throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      return conn.createClob();
   }

   public Blob createBlob() throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      return conn.createBlob();
   }

   public NClob createNClob() throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      return conn.createNClob();
   }

   public SQLXML createSQLXML() throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Statements may not be created after connection close", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      return conn.createSQLXML();
   }

   public boolean isValid(int timeout) throws SQLException {
      return transactionState != TransactionState.CLOSED;
   }

   public void setClientInfo(String name, String value) throws SQLClientInfoException {
      //Ignore - this must be done on real connection construction. TODO.
   }

   public void setClientInfo(Properties properties) throws SQLClientInfoException {
      //Ignore - this must be done on real connection construction. TODO.
   }

   public String getClientInfo(String name) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      return conn.getClientInfo(name);
   }

   public Properties getClientInfo() throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      return conn.getClientInfo();
   }

   public Array createArrayOf(String typeName, Object[] elements) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      return conn.createArrayOf(typeName, elements);
   }

   public Struct createStruct(String typeName, Object[] attributes) throws SQLException {

      if(transactionState == TransactionState.CLOSED) {
         throw new SQLException("Operation not permitted on closed connection", JDBConnection.SQLSTATE_CONNECTION_EXCEPTION);
      }

      return conn.createStruct(typeName, attributes);
   }

   public <T> T unwrap(Class<T> iface) throws SQLException {
      return conn.unwrap(iface);
   }

   public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return conn.isWrapperFor(iface);
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
