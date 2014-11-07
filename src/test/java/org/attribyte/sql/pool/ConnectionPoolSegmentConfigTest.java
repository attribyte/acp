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

import org.junit.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Properties;
import java.util.HashMap;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.attribyte.sql.pool.contrib.PropertiesPasswordSource;

/**
 * Segment config tests.
 */
public class ConnectionPoolSegmentConfigTest {

   @Test
   public void xmlConfig() throws Exception {

      String xml =
              "<segment name=\"segment0\" size=\"5\" concurrency=\"2\" testOnLogicalOpen=\"true\" testOnLogicalClose=\"false\">" +
                      "<connection user=\"test\" password=\"pass\" testSQL=\"testsql\" connectionString=\"cs\">" +
                      "<acquireTimeout time=\"5\" timeUnit=\"seconds\"/>" +
                      "<activeTimeout time=\"5\" timeUnit=\"milliseconds\"/>" +
                      "<lifetime time=\"1\" timeUnit=\"hours\"/>" +
                      "</connection>" +
                      "<reconnect concurrency=\"3\" maxWaitTime=\"4\" timeUnit=\"seconds\"/>" +
                      "<activeTimeoutMonitor frequency=\"45\" timeUnit=\"seconds\"/>" +
                      "</segment>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      ConnectionPoolSegment segment = ConnectionPoolSegment.Initializer.fromXML("pool0", elem, null).createSegment();
      assertNotNull(segment);
      assertNotNull(segment.dbConnection);
      assertEquals("test", segment.dbConnection.user);
      assertEquals("pass", segment.dbConnection.password);
      assertEquals("testsql", segment.dbConnection.testSQL);
      assertEquals("cs", segment.dbConnection.connectionString);
      assertEquals(2, segment.getCloserThreadCount());
      assertEquals(3, segment.getMaxConcurrentReconnects());
      assertEquals(4000, segment.maxReconnectDelayMillis);
      assertEquals(5, segment.getMaxConnections());
      assertTrue(segment.testOnLogicalOpen);
      assertFalse(segment.testOnLogicalClose);
      assertEquals(5000, segment.acquireTimeoutMillis);
      assertEquals(5, segment.activeTimeoutMillis);
      assertEquals(3600 * 1000, segment.connectionLifetimeMillis);
      assertEquals("pool0:segment0", segment.name);
   }

   @Test
   public void xmlConfigWithPasswordSource() throws Exception {

      String xml =
              "<segment name=\"segment0\" size=\"5\" concurrency=\"2\" testOnLogicalOpen=\"true\" testOnLogicalClose=\"false\">" +
                      "<connection user=\"test\" password=\"secret\" testSQL=\"testsql\" connectionString=\"cs\">" +
                      "<acquireTimeout time=\"5\" timeUnit=\"seconds\"/>" +
                      "<activeTimeout time=\"5\" timeUnit=\"milliseconds\"/>" +
                      "<lifetime time=\"1\" timeUnit=\"hours\"/>" +
                      "</connection>" +
                      "<reconnect concurrency=\"3\" maxWaitTime=\"4\" timeUnit=\"seconds\"/>" +
                      "<activeTimeoutMonitor frequency=\"45\" timeUnit=\"seconds\"/>" +
                      "</segment>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      ConnectionPoolSegment.Initializer segmentInit = ConnectionPoolSegment.Initializer.fromXML("pool0", elem, null);
      Properties props = new Properties();
      props.setProperty("test@cs", "pass");
      segmentInit.setPasswordSource(new PropertiesPasswordSource(props));
      ConnectionPoolSegment segment = segmentInit.createSegment();
      assertNotNull(segment);
      String password = segment.getPassword();
      assertNotNull(password);
      assertEquals("pass", password);
   }

   @Test
   public void xmlConfigTimeUnits() throws Exception {

      String xml =
              "<segment name=\"segment0\" size=\"5\" concurrency=\"2\" testOnLogicalOpen=\"true\" testOnLogicalClose=\"false\">" +
                      "<connection user=\"test\" password=\"pass\" testSQL=\"testsql\" connectionString=\"cs\">" +
                      "<acquireTimeout time=\"5s\"/>" +
                      "<activeTimeout time=\"5ms\"/>" +
                      "<lifetime time=\"1h\"/>" +
                      "</connection>" +
                      "<reconnect concurrency=\"3\" maxWaitTime=\"4s\"/>" +
                      "<activeTimeoutMonitor frequency=\"45s\"/>" +
                      "</segment>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      ConnectionPoolSegment segment = ConnectionPoolSegment.Initializer.fromXML("pool0", elem, null).createSegment();
      assertNotNull(segment);
      assertNotNull(segment.dbConnection);
      assertEquals("test", segment.dbConnection.user);
      assertEquals("pass", segment.dbConnection.password);
      assertEquals("testsql", segment.dbConnection.testSQL);
      assertEquals("cs", segment.dbConnection.connectionString);
      assertEquals(2, segment.getCloserThreadCount());
      assertEquals(3, segment.getMaxConcurrentReconnects());
      assertEquals(4000, segment.maxReconnectDelayMillis);
      assertEquals(5, segment.getMaxConnections());
      assertTrue(segment.testOnLogicalOpen);
      assertFalse(segment.testOnLogicalClose);
      assertEquals(5000, segment.acquireTimeoutMillis);
      assertEquals(5, segment.activeTimeoutMillis);
      assertEquals(3600 * 1000, segment.connectionLifetimeMillis);
      assertEquals("pool0:segment0", segment.name);
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void xmlConfigMissingUnits() throws Exception {

      String xml =
              "<segment name=\"segment0\" size=\"5\" concurrency=\"2\" testOnLogicalOpen=\"true\" testOnLogicalClose=\"false\">" +
                      "<connection user=\"test\" password=\"pass\" testSQL=\"testsql\" connectionString=\"cs\">" +
                      "<acquireTimeout time=\"5s\"/>" +
                      "<activeTimeout time=\"5\"/>" +
                      "<lifetime time=\"1h\"/>" +
                      "</connection>" +
                      "<reconnect concurrency=\"3\" maxWaitTime=\"4s\"/>" +
                      "<activeTimeoutMonitor frequency=\"45s\"/>" +
                      "</segment>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      ConnectionPoolSegment.Initializer.fromXML("pool0", elem, null).createSegment();
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void xmlInvalidUnits() throws Exception {

      String xml =
              "<segment name=\"segment0\" size=\"5\" concurrency=\"2\" testOnLogicalOpen=\"true\" testOnLogicalClose=\"false\">" +
                      "<connection user=\"test\" password=\"pass\" testSQL=\"testsql\" connectionString=\"cs\">" +
                      "<acquireTimeout time=\"5s\"/>" +
                      "<activeTimeout time=\"5x\"/>" +
                      "<lifetime time=\"1h\"/>" +
                      "</connection>" +
                      "<reconnect concurrency=\"3\" maxWaitTime=\"4s\"/>" +
                      "<activeTimeoutMonitor frequency=\"45s\"/>" +
                      "</segment>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      ConnectionPoolSegment.Initializer.fromXML("pool0", elem, null).createSegment();
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void xmlInvalidActiveTimeout() throws Exception {

      String xml =
              "<segment name=\"segment0\" size=\"5\" concurrency=\"2\" testOnLogicalOpen=\"true\" testOnLogicalClose=\"false\">" +
                      "<connection user=\"test\" password=\"pass\" testSQL=\"testsql\" connectionString=\"cs\">" +
                      "<acquireTimeout time=\"5s\"/>" +
                      "<activeTimeout time=\"-5s\"/>" +
                      "<lifetime time=\"1h\"/>" +
                      "</connection>" +
                      "<reconnect concurrency=\"3\" maxWaitTime=\"4s\"/>" +
                      "<activeTimeoutMonitor frequency=\"45s\"/>" +
                      "</segment>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      ConnectionPoolSegment.Initializer.fromXML("pool0", elem, null).createSegment();
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void xmlInvalidLifetime() throws Exception {

      String xml =
              "<segment name=\"segment0\" size=\"5\" concurrency=\"2\" testOnLogicalOpen=\"true\" testOnLogicalClose=\"false\">" +
                      "<connection user=\"test\" password=\"pass\" testSQL=\"testsql\" connectionString=\"cs\">" +
                      "<acquireTimeout time=\"5s\"/>" +
                      "<activeTimeout time=\"5s\"/>" +
                      "<lifetime time=\"-1h\"/>" +
                      "</connection>" +
                      "<reconnect concurrency=\"3\" maxWaitTime=\"4s\"/>" +
                      "<activeTimeoutMonitor frequency=\"45s\"/>" +
                      "</segment>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      ConnectionPoolSegment.Initializer.fromXML("pool0", elem, null).createSegment();
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void xmlInvalidReconnectMaxWait() throws Exception {

      String xml =
              "<segment name=\"segment0\" size=\"5\" concurrency=\"2\" testOnLogicalOpen=\"true\" testOnLogicalClose=\"false\">" +
                      "<connection user=\"test\" password=\"pass\" testSQL=\"testsql\" connectionString=\"cs\">" +
                      "<acquireTimeout time=\"5s\"/>" +
                      "<activeTimeout time=\"5s\"/>" +
                      "<lifetime time=\"1h\"/>" +
                      "</connection>" +
                      "<reconnect concurrency=\"3\" maxWaitTime=\"-4s\"/>" +
                      "<activeTimeoutMonitor frequency=\"45s\"/>" +
                      "</segment>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      ConnectionPoolSegment.Initializer.fromXML("pool0", elem, null).createSegment();
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void xmlInvalidActiveTimeoutMonitorFrequency() throws Exception {

      String xml =
              "<segment name=\"segment0\" size=\"5\" concurrency=\"2\" testOnLogicalOpen=\"true\" testOnLogicalClose=\"false\">" +
                      "<connection user=\"test\" password=\"pass\" testSQL=\"testsql\" connectionString=\"cs\">" +
                      "<acquireTimeout time=\"5s\"/>" +
                      "<activeTimeout time=\"5s\"/>" +
                      "<lifetime time=\"1h\"/>" +
                      "</connection>" +
                      "<reconnect concurrency=\"3\" maxWaitTime=\"-4s\"/>" +
                      "<activeTimeoutMonitor frequency=\"-45s\"/>" +
                      "</segment>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      ConnectionPoolSegment.Initializer.fromXML("pool0", elem, null).createSegment();
   }

   @Test
   public void xmlConfigWithMap() throws Exception {

      JDBConnection conn = new JDBConnection("test123", "test", "pass", "cs", 4L, "testsql", 200L, false);
      Map<String, JDBConnection> connMap = new HashMap<String, JDBConnection>();
      connMap.put("test123", conn);

      String xml =
              "<segment name=\"segment0\" size=\"5\" concurrency=\"2\" testOnLogicalOpen=\"true\" testOnLogicalClose=\"false\">" +
                      "<connection name=\"test123\">" +
                      "<acquireTimeout time=\"5\" timeUnit=\"seconds\"/>" +
                      "<activeTimeout time=\"5\" timeUnit=\"milliseconds\"/>" +
                      "<lifetime time=\"1\" timeUnit=\"hours\"/>" +
                      "</connection>" +
                      "<reconnect concurrency=\"3\" maxWaitTime=\"4\" timeUnit=\"seconds\"/>" +
                      "<activeTimeoutMonitor frequency=\"45\" timeUnit=\"seconds\"/>" +
                      "</segment>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      ConnectionPoolSegment segment = ConnectionPoolSegment.Initializer.fromXML("pool0", elem, connMap).createSegment();
      assertNotNull(segment);
      assertNotNull(segment.dbConnection);
      assertEquals("test", segment.dbConnection.user);
      assertEquals("pass", segment.dbConnection.password);
      assertEquals("testsql", segment.dbConnection.testSQL);
      assertEquals("cs", segment.dbConnection.connectionString);
      assertEquals(2, segment.getCloserThreadCount());
      assertEquals(3, segment.getMaxConcurrentReconnects());
      assertEquals(4000, segment.maxReconnectDelayMillis);
      assertEquals(5, segment.getMaxConnections());
      assertTrue(segment.testOnLogicalOpen);
      assertFalse(segment.testOnLogicalClose);
      assertEquals(5000, segment.acquireTimeoutMillis);
      assertEquals(5, segment.activeTimeoutMillis);
      assertEquals(3600 * 1000, segment.connectionLifetimeMillis);
      assertEquals("pool0:segment0", segment.name);
   }

   @Test
   public void xmlConfigPasswordSourceWithMap() throws Exception {

      JDBConnection conn = new JDBConnection("test123", "test", "secret", "cs", 4L, "testsql", 200L, false);
      Map<String, JDBConnection> connMap = new HashMap<String, JDBConnection>();
      connMap.put("test123", conn);

      String xml =
              "<segment name=\"segment0\" size=\"5\" concurrency=\"2\" testOnLogicalOpen=\"true\" testOnLogicalClose=\"false\">" +
                      "<connection name=\"test123\">" +
                      "<acquireTimeout time=\"5\" timeUnit=\"seconds\"/>" +
                      "<activeTimeout time=\"5\" timeUnit=\"milliseconds\"/>" +
                      "<lifetime time=\"1\" timeUnit=\"hours\"/>" +
                      "</connection>" +
                      "<reconnect concurrency=\"3\" maxWaitTime=\"4\" timeUnit=\"seconds\"/>" +
                      "<activeTimeoutMonitor frequency=\"45\" timeUnit=\"seconds\"/>" +
                      "</segment>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();

      ConnectionPoolSegment.Initializer segmentInit = ConnectionPoolSegment.Initializer.fromXML("pool0", elem, connMap);
      Properties props = new Properties();
      props.setProperty("test123", "pass");
      segmentInit.setPasswordSource(new PropertiesPasswordSource(props));
      ConnectionPoolSegment segment = segmentInit.createSegment();
      assertNotNull(segment);
      String password = segment.getPassword();
      assertNotNull(password);
      assertEquals("pass", password);
   }

   @Test
   public void propertiesConfig() throws Exception {

      JDBConnection conn1 = new JDBConnection("conn1", "test1", "secret1", "cs1", 4L, "testsql1", 200L, false);
      JDBConnection conn2 = new JDBConnection("conn2", "test", "secret", "cs", 4L, "testsql", 200L, false);

      Map<String, JDBConnection> connMap = new HashMap<String, JDBConnection>();
      connMap.put("conn1", conn1);
      connMap.put("conn2", conn2);

      Properties props = new Properties();
      ConnectionPoolSegment.Initializer initializer = new ConnectionPoolSegment.Initializer();
      props.setProperty("name", "segment0");
      props.setProperty("connectionName", "conn1");
      props.setProperty("size", "5");
      props.setProperty("closeConcurrency", "2");
      props.setProperty("testOnLogicalOpen", "true");
      props.setProperty("testOnLogicalClose", "true");
      props.setProperty("acquireTimeout", "10ms");
      props.setProperty("activeTimeout", "60s");
      props.setProperty("connectionLifetime", "15m");
      props.setProperty("idleTimeBeforeShutdown", "30s");
      props.setProperty("minActiveTime", "30s");
      props.setProperty("reconnectConcurrency", "3");
      props.setProperty("reconnectMaxWaitTime", "1m");
      props.setProperty("activeTimeoutMonitorFrequency", "30s");
      ConnectionPoolSegment.Initializer.fromProperties("localPool", initializer, props, connMap);
      ConnectionPoolSegment segment = initializer.createSegment();

      assertNotNull(segment);
      assertNotNull(segment.dbConnection);
      assertEquals("test1", segment.dbConnection.user);
      assertEquals("secret1", segment.dbConnection.password);
      assertEquals("testsql1", segment.dbConnection.testSQL);
      assertEquals("cs1", segment.dbConnection.connectionString);

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
   }
}