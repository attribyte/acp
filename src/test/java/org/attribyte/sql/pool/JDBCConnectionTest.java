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
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

/**
 * Connection tests.
 */
public class JDBCConnectionTest {

   @Test
   public void xmlConfig() throws Exception {

      String xml =
              "<connection name=\"conn0\" user=\"test\" password=\"pass\" testSQL=\"testsql\" connectionString=\"cs\" testInterval=\"5s\" createTimeout=\"30s\">" +
                      "<properties>" +
                      "<property name=\"test1\" value=\"val1\"/>" +
                      "<property name=\"test2\" value=\"val2\"/>" +
                      "</properties>" +
                      "</connection>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      JDBConnection conn = new JDBConnection(elem);
      assertEquals("conn0", conn.name);
      assertEquals("test", conn.user);
      assertEquals("pass", conn.password);
      assertEquals("testsql", conn.testSQL);
      assertEquals("cs?test1=val1&test2=val2", conn.connectionString);
      assertEquals(5000L, conn.testIntervalMillis);
      assertEquals(30000L, conn.createTimeoutMillis);
   }

   @Test
   public void singlePropXMLConfig() throws Exception {

      String xml =
              "<connection name=\"conn0\" user=\"test\" password=\"pass\" testSQL=\"testsql\" connectionString=\"cs\">" +
                      "<properties>" +
                      "<property name=\"test1\" value=\"val1\"/>" +
                      "</properties>" +
                      "</connection>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      JDBConnection conn = new JDBConnection(elem);
      assertEquals("conn0", conn.name);
      assertEquals("test", conn.user);
      assertEquals("pass", conn.password);
      assertEquals("testsql", conn.testSQL);
      assertEquals("cs?test1=val1", conn.connectionString);
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void noConnectionStringXMLConfig() throws Exception {

      String xml =
              "<connection name=\"conn0\" user=\"test\" password=\"pass\" testSQL=\"testsql\" connectionStringx=\"cs\">" +
                      "<properties>" +
                      "<property name=\"test1\" value=\"val1\"/>" +
                      "</properties>" +
                      "</connection>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      JDBConnection conn = new JDBConnection(elem);
   }

   @Test
   public void noPropXMLConfig() throws Exception {
      String xml =
              "<connection name=\"conn0\" user=\"test\" password=\"pass\" testSQL=\"testsql\" connectionString=\"cs\"/>";
      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      JDBConnection conn = new JDBConnection(elem);
      assertEquals("conn0", conn.name);
      assertEquals("test", conn.user);
      assertEquals("pass", conn.password);
      assertEquals("testsql", conn.testSQL);
      assertEquals("cs", conn.connectionString);
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void initDrivers() throws Exception {
      String xml =
              "<drivers>" +
                      "<driver class=\"java.lang.Object\"/>" +
                      "<driver class=\"xxx123\"/>" +
                      "</drivers>";
      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      JDBConnection.initDrivers(elem);
   }

   @Test
   public void mapFromXML() throws Exception {

      String xml =
              "<connections>" +
                      "<connection name=\"conn0\" user=\"test\" password=\"pass\" testSQL=\"testsql\" connectionString=\"cs\">" +
                      "<properties>" +
                      "<property name=\"test1\" value=\"val1\"/>" +
                      "<property name=\"test2\" value=\"val2\"/>" +
                      "</properties>" +
                      "</connection>" +
                      "<connection name=\"conn1\" user=\"test1\" password=\"pass1\" testSQL=\"testsql1\" connectionString=\"cs2\" createTimeout=\"30s\"/>" +
                      "</connections>";

      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

      Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
      Element elem = doc.getDocumentElement();
      Map<String, JDBConnection> connMap = JDBConnection.mapFromXML(elem);
      assertNotNull(connMap);
      assertEquals(2, connMap.size());
      JDBConnection conn0 = connMap.get("conn0");
      assertNotNull(conn0);
      assertEquals("conn0", conn0.name);
      assertEquals("test", conn0.user);
      assertEquals("pass", conn0.password);
      assertEquals("testsql", conn0.testSQL);
      assertEquals("cs?test1=val1&test2=val2", conn0.connectionString);

      JDBConnection conn1 = connMap.get("conn1");
      assertNotNull(conn1);
      assertEquals("conn1", conn1.name);
      assertEquals("test1", conn1.user);
      assertEquals("pass1", conn1.password);
      assertEquals("testsql1", conn1.testSQL);
      assertEquals("cs2", conn1.connectionString);
      assertEquals(30000L, conn1.createTimeoutMillis);
   }

   @Test
   public void initDriversProps() throws Exception {
      Properties props = new Properties();
      props.setProperty("driver.test.class", "java.lang.Object");
      JDBConnection.initDrivers(props);
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void initDriversPropsFailBadClass() throws Exception {
      Properties props = new Properties();
      props.setProperty("driver.test.class", "notaclass");
      JDBConnection.initDrivers(props);
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void initDriversPropsFailNoClass() throws Exception {
      Properties props = new Properties();
      props.setProperty("driver.test.classs", "java.lang.Object");
      JDBConnection.initDrivers(props);
   }

   @Test
   public void fromProps() throws Exception {
      Properties props = new Properties();
      props.setProperty("property.std.prop1", "prop1val");
      props.setProperty("property.std.prop2", "prop2val");
      props.setProperty("connection.local.user", "apprw");
      props.setProperty("connection.local.password", "secret");
      props.setProperty("connection.local.connectionString", "jdbc:mysql://127.0.0.1/attribyte");
      props.setProperty("connection.local.testSQL", "SELECT 1 FROM test");
      props.setProperty("connection.local.testInterval", "30s");
      props.setProperty("connection.local.createTimeout", "1m");
      props.setProperty("connection.local.debug", "true");
      props.setProperty("connection.local.properties", "std");
      Map<String, JDBConnection> connMap = JDBConnection.mapFromProperties(props);
      assertNotNull(connMap);
      assertEquals(1, connMap.size());
      JDBConnection conn = connMap.get("local");
      assertNotNull(conn);
      assertEquals("apprw", conn.user);
      assertEquals("secret", conn.password);
      assertEquals("SELECT 1 FROM test", conn.testSQL);
      assertEquals(30000L, conn.testIntervalMillis);
      assertEquals(60000L, conn.createTimeoutMillis);
      assertTrue(conn.debug);
      assertTrue(conn.connectionString.indexOf("prop1=prop1val") > 0);
      assertTrue(conn.connectionString.indexOf("prop2=prop2val") > 0);
   }

   @Test
   public void mapFromProps() throws Exception {
      Properties props = new Properties();
      props.setProperty("property.std.prop1", "prop1val");
      props.setProperty("property.std.prop2", "prop2val");

      props.setProperty("property.std2.prop1", "prop1val2");
      props.setProperty("property.std2.prop2", "prop2val2");

      props.setProperty("connection.local.user", "apprw");
      props.setProperty("connection.local.password", "secret");
      props.setProperty("connection.local.connectionString", "jdbc:mysql://127.0.0.1/attribyte");
      props.setProperty("connection.local.testSQL", "SELECT 1 FROM test");
      props.setProperty("connection.local.testInterval", "30s");
      props.setProperty("connection.local.createTimeout", "1m");
      props.setProperty("connection.local.debug", "true");
      props.setProperty("connection.local.properties", "std");

      props.setProperty("connection.local2.user", "apprw2");
      props.setProperty("connection.local2.password", "secret2");
      props.setProperty("connection.local2.connectionString", "jdbc:mysql://127.0.0.1/attribyte2");
      props.setProperty("connection.local2.testSQL", "SELECT 1 FROM test2");
      props.setProperty("connection.local2.testInterval", "60s");
      props.setProperty("connection.local2.createTimeout", "2m");
      props.setProperty("connection.local2.debug", "false");
      props.setProperty("connection.local2.properties", "std2");

      Map<String, JDBConnection> connMap = JDBConnection.mapFromProperties(props);
      assertNotNull(connMap);
      assertEquals(2, connMap.size());

      JDBConnection conn = connMap.get("local");
      assertNotNull(conn);
      assertEquals("apprw", conn.user);
      assertEquals("secret", conn.password);
      assertEquals("SELECT 1 FROM test", conn.testSQL);
      assertEquals(30000L, conn.testIntervalMillis);
      assertEquals(60000L, conn.createTimeoutMillis);
      assertTrue(conn.debug);
      assertTrue(conn.connectionString.indexOf("prop1=prop1val") > 0);
      assertTrue(conn.connectionString.indexOf("prop2=prop2val") > 0);

      JDBConnection conn2 = connMap.get("local2");
      assertNotNull(conn2);
      assertEquals("apprw2", conn2.user);
      assertEquals("secret2", conn2.password);
      assertEquals("SELECT 1 FROM test2", conn2.testSQL);
      assertEquals(60000L, conn2.testIntervalMillis);
      assertEquals(120000L, conn2.createTimeoutMillis);
      assertFalse(conn2.debug);
      assertTrue(conn2.connectionString.indexOf("prop1=prop1val2") > 0);
      assertTrue(conn2.connectionString.indexOf("prop2=prop2val2") > 0);
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void mapFromPropsMissingProps() throws Exception {
      Properties props = new Properties();
      props.setProperty("property.std.prop1", "prop1val");
      props.setProperty("property.std.prop2", "prop2val");
      props.setProperty("connection.local.user", "apprw");
      props.setProperty("connection.local.password", "secret");
      props.setProperty("connection.local.connectionString", "jdbc:mysql://127.0.0.1/attribyte");
      props.setProperty("connection.local.testSQL", "SELECT 1 FROM test");
      props.setProperty("connection.local.testInterval", "30s");
      props.setProperty("connection.local.createTimeout", "1m");
      props.setProperty("connection.local.debug", "true");
      props.setProperty("connection.local.properties", "std2");
      Map<String, JDBConnection> connMap = JDBConnection.mapFromProperties(props);
   }

}