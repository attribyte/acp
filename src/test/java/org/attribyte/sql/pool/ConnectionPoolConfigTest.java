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

import org.attribyte.sql.pool.contrib.PropertiesPasswordSource;
import org.junit.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

/**
 * Segment config tests.
 */
public class ConnectionPoolConfigTest {

   static final String seg0XML =
           "<segment name=\"segment0\" size=\"5\" concurrency=\"2\" testOnLogicalOpen=\"true\" testOnLogicalClose=\"false\">" +
                   "<connection user=\"test\" password=\"pass\" testSQL=\"testsql\" connectionString=\"cs\" createTimeout=\"30s\">" +
                   "<acquireTimeout time=\"50ms\"/>" +
                   "<activeTimeout time=\"10s\"/>" +
                   "<lifetime time=\"1h\"/>" +
                   "</connection>" +
                   "<reconnect concurrency=\"3\" maxWaitTime=\"4s\"/>" +
                   "<activeTimeoutMonitor frequency=\"45s\"/>" +
                   "</segment>";

   static final String seg1XML =
           "<segment name=\"segment1\" size=\"5\" concurrency=\"2\" testOnLogicalOpen=\"true\" testOnLogicalClose=\"false\">" +
                   "<connection name=\"conn0\">" +
                   "<acquireTimeout time=\"15ms\"/>" +
                   "<activeTimeout time=\"25s\"/>" +
                   "<lifetime time=\"2h\"/>" +
                   "</connection>" +
                   "<reconnect concurrency=\"4\" maxWaitTime=\"5s\"/>" +
                   "<activeTimeoutMonitor frequency=\"55s\"/>" +
                   "</segment>";

   static final String seg2XML =
           "<segment name=\"segment2\" size=\"5\" concurrency=\"2\" testOnLogicalOpen=\"true\" testOnLogicalClose=\"false\">" +
                   "<connection name=\"conn1\">" +
                   "<acquireTimeout time=\"15ms\"/>" +
                   "<activeTimeout time=\"25s\"/>" +
                   "<lifetime time=\"2h\"/>" +
                   "</connection>" +
                   "<reconnect concurrency=\"4\" maxWaitTime=\"5s\"/>" +
                   "<activeTimeoutMonitor frequency=\"55s\"/>" +
                   "</segment>";

   @Test
   public void xmlConfig() throws Exception {

      String xml =
              "<acp>" +
                      "<connections>" +
                      "<properties name=\"std\">" +
                      "<property name=\"test1\" value=\"val1\"/>" +
                      "<property name=\"test2\" value=\"val2\"/>" +
                      "</properties>" +
                      "<connection name=\"conn0\" user=\"test2\" password=\"pass2\" testSQL=\"testsql2\" connectionString=\"cs2\" createTimeout=\"40s\">" +
                      "<properties clone=\"std\"/>" +
                      "</connection>" +
                      "<connection name=\"conn1\" user=\"test3\" password=\"secret\" testSQL=\"testsql2\" connectionString=\"cs2\" createTimeout=\"40s\">" +
                      "<properties clone=\"std\"/>" +
                      "</connection>" +
                      "</connections>" +
                      "<pool name=\"testpool\">" +
                      "<active min=\"-1\">" +
                      seg0XML +
                      "<segment name=\"segment0c\" clone=\"segment0\" size=\"10\"/>" +
                      "</active>" +
                      "<reserve>" +
                      seg1XML +
                      seg2XML +
                      "</reserve>" +
                      "<saturatedAcquireTimeout time=\"5s\"/>" +
                      "</pool>" +
                      "</acp>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();

      Properties props = new Properties();
      props.setProperty("conn1", "pass3");
      PasswordSource passwordSource = new PropertiesPasswordSource(props);

      ConnectionPool[] pools = ConnectionPool.createPools(elem, null, passwordSource);
      assertNotNull(pools);
      assertEquals(1, pools.length);
      ConnectionPool pool = pools[0];
      assertEquals(5000L, pool.saturatedAcquireTimeoutMillis);
      ConnectionPoolSegment[] segments = pool.getSegmentsForTest();
      assertEquals(4, segments.length);
      ConnectionPoolSegment seg0 = segments[0];
      assertEquals("testpool:segment0", seg0.name);
      assertEquals(5, seg0.getMaxConnections());
      assertTrue(seg0.testOnLogicalOpen);
      assertFalse(seg0.testOnLogicalClose);
      assertEquals(50, seg0.acquireTimeoutMillis);
      assertEquals(10000, seg0.activeTimeoutMillis);
      assertEquals(3600 * 1000, seg0.connectionLifetimeMillis);
      assertEquals("cs", seg0.dbConnection.connectionString);
      assertEquals("test", seg0.dbConnection.user);
      assertEquals("pass", seg0.dbConnection.password);
      assertEquals("testsql", seg0.dbConnection.testSQL);
      assertEquals(4000, seg0.maxReconnectDelayMillis);
      assertEquals(3, seg0.getMaxConcurrentReconnects());
      assertEquals(30000L, seg0.dbConnection.createTimeoutMillis);

      ConnectionPoolSegment seg1 = segments[1];
      assertEquals("testpool:segment0c", seg1.name);
      assertEquals(10, seg1.getMaxConnections());
      assertTrue(seg1.testOnLogicalOpen);
      assertFalse(seg1.testOnLogicalClose);
      assertEquals(50, seg1.acquireTimeoutMillis);
      assertEquals(10000, seg1.activeTimeoutMillis);
      assertEquals(3600 * 1000, seg1.connectionLifetimeMillis);
      assertEquals("cs", seg1.dbConnection.connectionString);
      assertEquals("test", seg1.dbConnection.user);
      assertEquals("pass", seg1.dbConnection.password);
      assertEquals("testsql", seg1.dbConnection.testSQL);
      assertEquals(4000, seg1.maxReconnectDelayMillis);
      assertEquals(3, seg1.getMaxConcurrentReconnects());
      assertEquals(30000L, seg1.dbConnection.createTimeoutMillis);

      ConnectionPoolSegment seg2 = segments[2];
      assertEquals("testpool:segment1", seg2.name);
      assertEquals(5, seg2.getMaxConnections());
      assertTrue(seg2.testOnLogicalOpen);
      assertFalse(seg2.testOnLogicalClose);
      assertEquals(15, seg2.acquireTimeoutMillis);
      assertEquals(25000, seg2.activeTimeoutMillis);
      assertEquals(2 * 3600 * 1000, seg2.connectionLifetimeMillis);
      assertEquals("cs2?test1=val1&test2=val2", seg2.dbConnection.connectionString);
      assertEquals("test2", seg2.dbConnection.user);
      assertEquals("pass2", seg2.dbConnection.password);
      assertEquals("testsql2", seg2.dbConnection.testSQL);
      assertEquals(5000, seg2.maxReconnectDelayMillis);
      assertEquals(4, seg2.getMaxConcurrentReconnects());
      assertEquals(40000L, seg2.dbConnection.createTimeoutMillis);

      ConnectionPoolSegment seg3 = segments[3];
      assertNotNull(seg3);
      String checkPassword = seg3.getPassword();
      assertNotNull(checkPassword);
      assertEquals("pass3", checkPassword);
   }

   @Test
   public void propertiesConfig() throws Exception {

      JDBConnection conn1 = new JDBConnection("conn1", "root", "", "jdbc:mysql://127.0.0.1/test", 40000L, "SELECT 1 FROM test", 200L, false);
      JDBConnection conn2 = new JDBConnection("conn2", "root", "", "jdbc:mysql://127.0.0.1/test", 40000L, "SELECT 1 FROM test", 200L, false);

      Map<String, JDBConnection> connMap = new HashMap<String, JDBConnection>();
      connMap.put("conn1", conn1);
      connMap.put("conn2", conn2);

      Properties props = new Properties();

      props.setProperty("pool.localPool.minActiveSegments", "1");
      props.setProperty("pool.localPool.startActiveSegments", "1");
      props.setProperty("pool.localPool.idleCheckInterval", "30s");
      props.setProperty("pool.localPool.saturatedAcquireTimeout", "1s");

      props.setProperty("pool.localPool.segment0.name", "segment0");
      props.setProperty("pool.localPool.segment0.connectionName", "conn1");
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

      props.setProperty("pool.localPool.segment1.clone", "segment0");
      props.setProperty("pool.localPool.segment1.acquireTimeout", "20ms");
      props.setProperty("pool.localPool.segment1.size", "10");

      props.setProperty("pool.localPool.segment2.clone", "segment1");
      props.setProperty("pool.localPool.segment2.acquireTimeout", "50ms");

      ConnectionPool.Initializer initializer =
              ConnectionPool.Initializer.fromProperties("localPool", "pool.localPool.", props, connMap, null);

      assertNotNull(initializer);
      assertEquals(1, initializer.activeSegments.size());
      assertEquals(2, initializer.reserveSegments.size());

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
      assertEquals("localPool:segment0", segment.name);


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
      assertEquals("localPool:segment1", segment.name);

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
      assertEquals("localPool:segment2", segment.name);
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
      props.setProperty("pool.localPool.startActiveSegments", "1");
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

      props.setProperty("pool.localPool.segment1.clone", "segment0");
      props.setProperty("pool.localPool.segment1.acquireTimeout", "20ms");
      props.setProperty("pool.localPool.segment1.size", "10");

      props.setProperty("pool.localPool.segment2.clone", "segment1");
      props.setProperty("pool.localPool.segment2.acquireTimeout", "50ms");

      props.setProperty("pool.remotePool.minActiveSegments", "1");
      props.setProperty("pool.remotePool.startActiveSegments", "1");
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

      props.setProperty("pool.remotePool.segment1.clone", "segment0");
      props.setProperty("pool.remotePool.segment1.acquireTimeout", "19ms");
      props.setProperty("pool.remotePool.segment1.size", "9");

      ConnectionPool.Initializer[] initializers = ConnectionPool.Initializer.fromProperties(props, null); //No password source
      assertNotNull(initializers);
      assertEquals(2, initializers.length);

      ConnectionPool pool = initializers[0].createPool();

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
      assertEquals("localPool:segment0", segment.name);


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
      assertEquals("localPool:segment1", segment.name);

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
      assertEquals("localPool:segment2", segment.name);

      //Rempote pool

      pool = initializers[1].createPool();
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
      assertEquals("remotePool:segment0", segment.name);


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
      assertEquals("remotePool:segment1", segment.name);
   }
}