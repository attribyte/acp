/*
 * Copyright 2010, 2014 Attribyte, LLC
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
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Connection tests.
 */
public class JDBCConnectionTest {

   @Test
   public void initDrivers() throws Exception {
      Properties props = new Properties();
      props.setProperty("driver.test.class", "java.lang.Object");
      TypesafeConfig.initDrivers(ConfigFactory.parseProperties((props)).getObject("driver").toConfig());
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void initDriversFailBadClass() throws Exception {
      Properties props = new Properties();
      props.setProperty("driver.test.class", "notaclass");
      TypesafeConfig.initDrivers(ConfigFactory.parseProperties((props)));
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void initDriversFailNoClass() throws Exception {
      Properties props = new Properties();
      props.setProperty("driver.test.classs", "java.lang.Object");
      TypesafeConfig.initDrivers(ConfigFactory.parseProperties((props)));
   }

   @Test
   public void connectionMap() throws Exception {
      Properties props = new Properties();
      props.setProperty("property.std.prop1", "prop1val");
      props.setProperty("property.std.prop2", "prop2val");

      props.setProperty("property.std2.prop1", "prop1val2");

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
      Config config = ConfigFactory.parseProperties((props));

      Map<String, JDBConnection> connMap = TypesafeConfig.connectionMapFromConfig(config);
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
      assertTrue(conn.connectionString.indexOf('?') > 0);
      assertTrue(conn.connectionString.indexOf('&') > 0);

      JDBConnection conn2 = connMap.get("local2");
      assertNotNull(conn2);
      assertEquals("apprw2", conn2.user);
      assertEquals("secret2", conn2.password);
      assertEquals("SELECT 1 FROM test2", conn2.testSQL);
      assertEquals(60000L, conn2.testIntervalMillis);
      assertEquals(120000L, conn2.createTimeoutMillis);
      assertFalse(conn2.debug);
      assertTrue(conn2.connectionString.indexOf("prop1=prop1val2") > 0);
      assertTrue(conn2.connectionString.indexOf('?') > 0);
      assertTrue(conn2.connectionString.indexOf('&') < 0);
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void missingPropsFail() throws Exception {
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
      props.setProperty("connection.local.properties", "std2"); //Undefined
      Config config = ConfigFactory.parseProperties((props));
      TypesafeConfig.connectionMapFromConfig(config);
   }

   @Test(expected = org.attribyte.api.InitializationException.class)
   public void missingConnectionStringFail() throws Exception {
      Properties props = new Properties();
      props.setProperty("property.std.prop1", "prop1val");
      props.setProperty("property.std.prop2", "prop2val");
      props.setProperty("connection.local.user", "apprw");
      props.setProperty("connection.local.password", "secret");
      props.setProperty("connection.local.testSQL", "SELECT 1 FROM test");
      props.setProperty("connection.local.testInterval", "30s");
      props.setProperty("connection.local.createTimeout", "1m");
      props.setProperty("connection.local.debug", "true");
      props.setProperty("connection.local.properties", "std2");
      Config config = ConfigFactory.parseProperties((props));
      TypesafeConfig.connectionMapFromConfig(config);
   }

}