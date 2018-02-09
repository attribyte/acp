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
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Segment config tests.
 */
public class ConnectionPoolSegmentConfigTest {

   @Test
   public void propertiesConfig() throws Exception {

      JDBConnection conn1 = new JDBConnection("conn1", "test1", "secret1", "cs1", 4L, "testsql1", 200L, false);
      JDBConnection conn2 = new JDBConnection("conn2", "test", "secret", "cs", 4L, "testsql", 200L, false);

      Map<String, JDBConnection> connMap = new HashMap<String, JDBConnection>();
      connMap.put("conn1", conn1);
      connMap.put("conn2", conn2);

      Properties props = new Properties();
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
      props.setProperty("incompleteTransactionPolicy", "report");
      props.setProperty("openStatementPolicy", "silent");
      props.setProperty("forceRealClosePolicy", "connectionWithLimit");
      props.setProperty("closeTimeLimit", "10 seconds");
      props.setProperty("activityTimeoutPolicy", "log");


      Config config = ConfigFactory.parseProperties((props));
      ConnectionPoolSegment.Initializer initializer = new ConnectionPoolSegment.Initializer();
      TypesafeConfig.segmentFromConfig("localPool", "segment0", config, initializer, connMap);

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
      assertEquals("localPool.segment0", segment.name);
   }
}