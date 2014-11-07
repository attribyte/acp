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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.attribyte.api.ConsoleLogger;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Segment config tests.
 */
public class ConnectionPoolConfigTest {

   @Test
   public void propertiesConfig() throws Exception {

      JDBConnection conn1 = new JDBConnection("conn1", "root", "", "jdbc:mysql://127.0.0.1/test", 40000L, "SELECT 1 FROM test", 200L, false);
      JDBConnection conn2 = new JDBConnection("conn2", "root", "", "jdbc:mysql://127.0.0.1/test", 40000L, "SELECT 1 FROM test", 200L, false);

      Map<String, JDBConnection> connMap = new HashMap<String, JDBConnection>();
      connMap.put("conn1", conn1);
      connMap.put("conn2", conn2);

      Properties props = new Properties();

      props.setProperty("minActiveSegments", "1");
      props.setProperty("startActiveSegments", "0"); //Unless connection test is valid, startup will fail.
      props.setProperty("idleCheckInterval", "30s");
      props.setProperty("saturatedAcquireTimeout", "1s");

      props.setProperty("segment0.name", "segment0");
      props.setProperty("segment0.connectionName", "conn1");
      props.setProperty("segment0.size", "5");
      props.setProperty("segment0.closeConcurrency", "2");
      props.setProperty("segment0.testOnLogicalOpen", "true");
      props.setProperty("segment0.testOnLogicalClose", "true");
      props.setProperty("segment0.acquireTimeout", "10ms");
      props.setProperty("segment0.activeTimeout", "60s");
      props.setProperty("segment0.connectionLifetime", "15m");
      props.setProperty("segment0.idleTimeBeforeShutdown", "30s");
      props.setProperty("segment0.minActiveTime", "30s");
      props.setProperty("segment0.reconnectConcurrency", "3");
      props.setProperty("segment0.reconnectMaxWaitTime", "1m");
      props.setProperty("segment0.activeTimeoutMonitorFrequency", "30s");
      props.setProperty("segment0.incompleteTransactionPolicy", "report");
      props.setProperty("segment0.openStatementPolicy", "silent");
      props.setProperty("segment0.forceRealClosePolicy", "connectionWithLimit");
      props.setProperty("segment0.closeTimeLimit", "10 seconds");

      props.setProperty("segment1.clone", "segment0");
      props.setProperty("segment1.acquireTimeout", "20ms");
      props.setProperty("segment1.size", "10");

      props.setProperty("segment2.clone", "segment1");
      props.setProperty("segment2.acquireTimeout", "50ms");

      Config config = ConfigFactory.parseProperties((props));

      ConnectionPool.Initializer initializer = TypesafeConfig.poolFromConfig("localPool", config, connMap, null, new ConsoleLogger());

      assertNotNull(initializer);
      assertEquals(0, initializer.activeSegments.size());
      assertEquals(3, initializer.reserveSegments.size());

      ConnectionPool pool = initializer.createPool();
      ConnectionPoolSegment[] segments = pool.getSegmentsForTest();
      assertEquals(3, segments.length);

      ConnectionPoolSegment segment = segments[0];

      assertNotNull(segment);
      assertNotNull(segment.dbConnection);
      assertEquals("root", segment.dbConnection.user);
      assertEquals("", segment.dbConnection.password);
      assertEquals("SELECT 1 FROM test", segment.dbConnection.testSQL);
      assertEquals("jdbc:mysql://127.0.0.1/test", segment.dbConnection.connectionString);

      assertEquals(2, segment.getCloserThreadCount());
      assertEquals(3, segment.getMaxConcurrentReconnects());
      assertEquals(60000, segment.maxReconnectDelayMillis);
      assertEquals(5, segment.getMaxConnections());
      assertTrue(segment.testOnLogicalOpen);
      assertTrue(segment.testOnLogicalClose);
      assertEquals(10, segment.acquireTimeoutMillis);
      assertEquals(60000, segment.activeTimeoutMillis);
      assertEquals(15L * 60L * 1000, segment.connectionLifetimeMillis);
      assertEquals(30000L, segment.idleTimeBeforeShutdownMillis);
      assertEquals(30L, segment.getActiveTimeoutMonitorFrequencySeconds());
      assertEquals("localPool.segment0", segment.name);


      segment = segments[1];

      assertNotNull(segment);
      assertNotNull(segment.dbConnection);
      assertEquals("root", segment.dbConnection.user);
      assertEquals("", segment.dbConnection.password);
      assertEquals("SELECT 1 FROM test", segment.dbConnection.testSQL);
      assertEquals("jdbc:mysql://127.0.0.1/test", segment.dbConnection.connectionString);

      assertEquals(2, segment.getCloserThreadCount());
      assertEquals(3, segment.getMaxConcurrentReconnects());
      assertEquals(60000, segment.maxReconnectDelayMillis);
      assertEquals(10, segment.getMaxConnections());
      assertTrue(segment.testOnLogicalOpen);
      assertTrue(segment.testOnLogicalClose);
      assertEquals(20, segment.acquireTimeoutMillis);
      assertEquals(60000, segment.activeTimeoutMillis);
      assertEquals(15L * 60L * 1000, segment.connectionLifetimeMillis);
      assertEquals(30000L, segment.idleTimeBeforeShutdownMillis);
      assertEquals(30L, segment.getActiveTimeoutMonitorFrequencySeconds());
      assertEquals("localPool.segment1", segment.name);

      segment = segments[2];

      assertNotNull(segment);
      assertNotNull(segment.dbConnection);
      assertEquals("root", segment.dbConnection.user);
      assertEquals("", segment.dbConnection.password);
      assertEquals("SELECT 1 FROM test", segment.dbConnection.testSQL);
      assertEquals("jdbc:mysql://127.0.0.1/test", segment.dbConnection.connectionString);

      assertEquals(2, segment.getCloserThreadCount());
      assertEquals(3, segment.getMaxConcurrentReconnects());
      assertEquals(60000, segment.maxReconnectDelayMillis);
      assertEquals(10, segment.getMaxConnections());
      assertTrue(segment.testOnLogicalOpen);
      assertTrue(segment.testOnLogicalClose);
      assertEquals(50, segment.acquireTimeoutMillis);
      assertEquals(60000, segment.activeTimeoutMillis);
      assertEquals(15L * 60L * 1000, segment.connectionLifetimeMillis);
      assertEquals(30000L, segment.idleTimeBeforeShutdownMillis);
      assertEquals(30L, segment.getActiveTimeoutMonitorFrequencySeconds());
      assertEquals("localPool.segment2", segment.name);
   }

   @Test
   public void multiPoolPropertiesConfig() throws Exception {

      Properties props = new Properties();

      props.setProperty("logger.class", "org.attribyte.api.ConsoleLogger");

      props.setProperty("property.std.useUnicode", "true");
      props.setProperty("property.std.characterEncoding", "utf8");

      props.setProperty("connection.local.user", "root");
      props.setProperty("connection.local.password", "");
      props.setProperty("connection.local.connectionString", "jdbc:mysql://127.0.0.1/test");
      props.setProperty("connection.local.testSQL", "SELECT 1 FROM test");
      props.setProperty("connection.local.testInterval", "30s");
      props.setProperty("connection.local.createTimeout", "60s");
      props.setProperty("connection.local.debug", "true");
      props.setProperty("connection.local.properties", "std");

      props.setProperty("connection.remote.user", "root");
      props.setProperty("connection.remote.password", "");
      props.setProperty("connection.remote.connectionString", "jdbc:mysql://127.0.0.1/test");
      props.setProperty("connection.remote.testSQL", "SELECT 1 FROM test");
      props.setProperty("connection.remote.testInterval", "30s");
      props.setProperty("connection.remote.createTimeout", "60s");
      props.setProperty("connection.remote.debug", "true");
      props.setProperty("connection.remote.properties", "std");

      props.setProperty("pool.localPool.minActiveSegments", "1");
      props.setProperty("pool.localPool.startActiveSegments", "0");
      props.setProperty("pool.localPool.idleCheckInterval", "30s");
      props.setProperty("pool.localPool.saturatedAcquireTimeout", "1s");

      props.setProperty("pool.localPool.segment0.name", "segment0");
      props.setProperty("pool.localPool.segment0.connectionName", "local");
      props.setProperty("pool.localPool.segment0.size", "5");
      props.setProperty("pool.localPool.segment0.closeConcurrency", "2");
      props.setProperty("pool.localPool.segment0.testOnLogicalOpen", "true");
      props.setProperty("pool.localPool.segment0.testOnLogicalClose", "true");
      props.setProperty("pool.localPool.segment0.acquireTimeout", "10ms");
      props.setProperty("pool.localPool.segment0.activeTimeout", "60s");
      props.setProperty("pool.localPool.segment0.connectionLifetime", "15m");
      props.setProperty("pool.localPool.segment0.idleTimeBeforeShutdown", "30s");
      props.setProperty("pool.localPool.segment0.minActiveTime", "30s");
      props.setProperty("pool.localPool.segment0.reconnectConcurrency", "3");
      props.setProperty("pool.localPool.segment0.reconnectMaxWaitTime", "1m");
      props.setProperty("pool.localPool.segment0.activeTimeoutMonitorFrequency", "30s");
      props.setProperty("pool.localPool.segment0.incompleteTransactionPolicy", "report");
      props.setProperty("pool.localPool.segment0.openStatementPolicy", "silent");
      props.setProperty("pool.localPool.segment0.forceRealClosePolicy", "connectionWithLimit");
      props.setProperty("pool.localPool.segment0.closeTimeLimit", "10 seconds");

      props.setProperty("pool.localPool.segment1.clone", "segment0");
      props.setProperty("pool.localPool.segment1.acquireTimeout", "20ms");
      props.setProperty("pool.localPool.segment1.size", "10");

      props.setProperty("pool.localPool.segment2.clone", "segment1");
      props.setProperty("pool.localPool.segment2.acquireTimeout", "50ms");

      props.setProperty("pool.remotePool.minActiveSegments", "1");
      props.setProperty("pool.remotePool.startActiveSegments", "0");
      props.setProperty("pool.remotePool.idleCheckInterval", "30s");
      props.setProperty("pool.remotePool.saturatedAcquireTimeout", "1s");

      props.setProperty("pool.remotePool.segment0.name", "segment0");
      props.setProperty("pool.remotePool.segment0.connectionName", "remote");
      props.setProperty("pool.remotePool.segment0.size", "4");
      props.setProperty("pool.remotePool.segment0.closeConcurrency", "1");
      props.setProperty("pool.remotePool.segment0.testOnLogicalOpen", "false");
      props.setProperty("pool.remotePool.segment0.testOnLogicalClose", "false");
      props.setProperty("pool.remotePool.segment0.acquireTimeout", "9ms");
      props.setProperty("pool.remotePool.segment0.activeTimeout", "59s");
      props.setProperty("pool.remotePool.segment0.connectionLifetime", "14m");
      props.setProperty("pool.remotePool.segment0.idleTimeBeforeShutdown", "29s");
      props.setProperty("pool.remotePool.segment0.minActiveTime", "29s");
      props.setProperty("pool.remotePool.segment0.reconnectConcurrency", "2");
      props.setProperty("pool.remotePool.segment0.reconnectMaxWaitTime", "59s");
      props.setProperty("pool.remotePool.segment0.activeTimeoutMonitorFrequency", "29s");
      props.setProperty("pool.remotePool.segment0.incompleteTransactionPolicy", "report");
      props.setProperty("pool.remotePool.segment0.openStatementPolicy", "silent");
      props.setProperty("pool.remotePool.segment0.forceRealClosePolicy", "connectionWithLimit");
      props.setProperty("pool.remotePool.segment0.closeTimeLimit", "10 seconds");

      props.setProperty("pool.remotePool.segment1.clone", "segment0");
      props.setProperty("pool.remotePool.segment1.acquireTimeout", "19ms");
      props.setProperty("pool.remotePool.segment1.size", "9");

      Config config = ConfigFactory.parseProperties((props));

      List<ConnectionPool.Initializer> initializers = TypesafeConfig.poolsFromConfig(config, null, new ConsoleLogger());
      Map<String, ConnectionPool> pools = TypesafeConfig.buildPools(initializers);

      assertNotNull(initializers);
      assertEquals(2, initializers.size());

      ConnectionPool pool = pools.get("localPool");
      assertNotNull(pool);

      ConnectionPoolSegment[] segments = pool.getSegmentsForTest();
      assertEquals(3, segments.length);

      ConnectionPoolSegment segment = segments[0];

      assertNotNull(segment);
      assertNotNull(segment.dbConnection);
      assertEquals("root", segment.dbConnection.user);
      assertEquals("", segment.dbConnection.password);
      assertEquals("SELECT 1 FROM test", segment.dbConnection.testSQL);
      assertEquals("jdbc:mysql://127.0.0.1/test?useUnicode=true&characterEncoding=utf8", segment.dbConnection.connectionString);

      assertEquals(2, segment.getCloserThreadCount());
      assertEquals(3, segment.getMaxConcurrentReconnects());
      assertEquals(60000, segment.maxReconnectDelayMillis);
      assertEquals(5, segment.getMaxConnections());
      assertTrue(segment.testOnLogicalOpen);
      assertTrue(segment.testOnLogicalClose);
      assertEquals(10, segment.acquireTimeoutMillis);
      assertEquals(60000, segment.activeTimeoutMillis);
      assertEquals(15L * 60L * 1000, segment.connectionLifetimeMillis);
      assertEquals(30000L, segment.idleTimeBeforeShutdownMillis);
      assertEquals(30L, segment.getActiveTimeoutMonitorFrequencySeconds());
      assertEquals("localPool.segment0", segment.name);


      segment = segments[1];
      assertNotNull(segment);
      assertNotNull(segment.dbConnection);
      assertEquals("root", segment.dbConnection.user);
      assertEquals("", segment.dbConnection.password);
      assertEquals("SELECT 1 FROM test", segment.dbConnection.testSQL);
      assertEquals("jdbc:mysql://127.0.0.1/test?useUnicode=true&characterEncoding=utf8", segment.dbConnection.connectionString);

      assertEquals(2, segment.getCloserThreadCount());
      assertEquals(3, segment.getMaxConcurrentReconnects());
      assertEquals(60000, segment.maxReconnectDelayMillis);
      assertEquals(10, segment.getMaxConnections());
      assertTrue(segment.testOnLogicalOpen);
      assertTrue(segment.testOnLogicalClose);
      assertEquals(20, segment.acquireTimeoutMillis);
      assertEquals(60000, segment.activeTimeoutMillis);
      assertEquals(15L * 60L * 1000, segment.connectionLifetimeMillis);
      assertEquals(30000L, segment.idleTimeBeforeShutdownMillis);
      assertEquals(30L, segment.getActiveTimeoutMonitorFrequencySeconds());
      assertEquals("localPool.segment1", segment.name);

      segment = segments[2];
      assertNotNull(segment);
      assertNotNull(segment.dbConnection);
      assertEquals("root", segment.dbConnection.user);
      assertEquals("", segment.dbConnection.password);
      assertEquals("SELECT 1 FROM test", segment.dbConnection.testSQL);
      assertEquals("jdbc:mysql://127.0.0.1/test?useUnicode=true&characterEncoding=utf8", segment.dbConnection.connectionString);

      assertEquals(2, segment.getCloserThreadCount());
      assertEquals(3, segment.getMaxConcurrentReconnects());
      assertEquals(60000, segment.maxReconnectDelayMillis);
      assertEquals(10, segment.getMaxConnections());
      assertTrue(segment.testOnLogicalOpen);
      assertTrue(segment.testOnLogicalClose);
      assertEquals(50, segment.acquireTimeoutMillis);
      assertEquals(60000, segment.activeTimeoutMillis);
      assertEquals(15L * 60L * 1000, segment.connectionLifetimeMillis);
      assertEquals(30000L, segment.idleTimeBeforeShutdownMillis);
      assertEquals(30L, segment.getActiveTimeoutMonitorFrequencySeconds());
      assertEquals("localPool.segment2", segment.name);

      //Rempote pool

      pool = pools.get("remotePool");
      assertNotNull(pool);
      segments = pool.getSegmentsForTest();
      assertEquals(2, segments.length);

      segment = segments[0];

      assertNotNull(segment);
      assertNotNull(segment.dbConnection);
      assertEquals("root", segment.dbConnection.user);
      assertEquals("", segment.dbConnection.password);
      assertEquals("SELECT 1 FROM test", segment.dbConnection.testSQL);
      assertEquals("jdbc:mysql://127.0.0.1/test?useUnicode=true&characterEncoding=utf8", segment.dbConnection.connectionString);

      assertEquals(1, segment.getCloserThreadCount());
      assertEquals(2, segment.getMaxConcurrentReconnects());
      assertEquals(59000, segment.maxReconnectDelayMillis);
      assertEquals(4, segment.getMaxConnections());
      assertFalse(segment.testOnLogicalOpen);
      assertFalse(segment.testOnLogicalClose);
      assertEquals(9, segment.acquireTimeoutMillis);
      assertEquals(59000, segment.activeTimeoutMillis);
      assertEquals(14L * 60L * 1000, segment.connectionLifetimeMillis);
      assertEquals(29000L, segment.idleTimeBeforeShutdownMillis);
      assertEquals(29L, segment.getActiveTimeoutMonitorFrequencySeconds());
      assertEquals("remotePool.segment0", segment.name);


      segment = segments[1];
      assertNotNull(segment);
      assertNotNull(segment.dbConnection);
      assertEquals("root", segment.dbConnection.user);
      assertEquals("", segment.dbConnection.password);
      assertEquals("SELECT 1 FROM test", segment.dbConnection.testSQL);
      assertEquals("jdbc:mysql://127.0.0.1/test?useUnicode=true&characterEncoding=utf8", segment.dbConnection.connectionString);

      assertEquals(1, segment.getCloserThreadCount());
      assertEquals(2, segment.getMaxConcurrentReconnects());
      assertEquals(59000, segment.maxReconnectDelayMillis);
      assertEquals(9, segment.getMaxConnections());
      assertFalse(segment.testOnLogicalOpen);
      assertFalse(segment.testOnLogicalClose);
      assertEquals(19, segment.acquireTimeoutMillis);
      assertEquals(59000, segment.activeTimeoutMillis);
      assertEquals(14L * 60L * 1000, segment.connectionLifetimeMillis);
      assertEquals(29000L, segment.idleTimeBeforeShutdownMillis);
      assertEquals(29L, segment.getActiveTimeoutMonitorFrequencySeconds());
      assertEquals("remotePool.segment1", segment.name);
   }

}