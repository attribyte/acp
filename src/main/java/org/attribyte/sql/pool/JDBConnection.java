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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.attribyte.api.InitializationException;
import org.attribyte.util.DOMUtil;
import org.attribyte.util.InitUtil;
import org.attribyte.util.Pair;
import org.attribyte.util.StringUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Config for creating and maintaining JDBC connections.
 */
class JDBConnection {

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
    * Initialize JDBC drivers from properties.
    * <p>
    * e.g. <tt>driver.mysql.class=com.mysql.jdbc.Driver</tt>
    * </p>
    * @param props The properties.
    * @throws InitializationException if driver could not be initialized
    */
   static void initDrivers(final Properties props) throws InitializationException {

      Map<String, Properties> driverProperties = new InitUtil("driver.", props, true).split();
      for(Map.Entry<String, Properties> driverProps : driverProperties.entrySet()) {
         String className = driverProps.getValue().getProperty("class");
         if(StringUtil.hasContent(className)) {
            try {
               Class.forName(className.trim());
            } catch(Throwable t) {
               throw new InitializationException("Unable to load JDBC driver, '" + className.trim() + "'", t);
            }
         } else {
            throw new InitializationException("A 'class' must be specified for driver, '" + driverProps.getKey() + "'");
         }
      }
   }

   /**
    * Initialize JDBC drivers from an XML element.
    * @param parentElem The parent element.
    * @throws InitializationException if driver could not be initialized
    */
   static void initDrivers(final Element parentElem) throws InitializationException {

      Element driversElem = DOMUtil.getFirstChild(parentElem, "drivers");
      if(driversElem == null) {
         driversElem = parentElem;
      }

      NodeList driverList = driversElem.getElementsByTagName("driver");
      for(int i = 0; i < driverList.getLength(); i++) {
         Element driverElem = (Element)driverList.item(i);
         String className = driverElem.getAttribute("class");
         try {
            Class.forName(className);
         } catch(Throwable t) {
            throw new InitializationException("Unable to load JDBC driver, '" + className + "'", t);
         }
      }
   }

   /**
    * Creates a map of connection vs. name from properties.
    * <p>
    * <tt>property.std.useUnicode=true</tt><br/>
    * <tt>property.std.characterEncoding=utf8</tt><br/><br/>
    * <tt>connection.local.user=apprw</tt><br/>
    * <tt>connection.local.password=secret</tt><br/>
    * <tt>connection.local.connectionString=jdbc:mysql://127.0.0.1/testdb</tt><br/>
    * <tt>connection.local.testSQL=SELECT 1 FROM test</tt><br/>
    * <tt>connection.local.testInterval=30s</tt><br/>
    * <tt>connection.local.createTimeout=60s</tt><br/>
    * <tt>connection.local.debug=true</tt><br/>
    * <tt>connection.local.properties=std</tt>
    * </p>
    * @param props The properties.
    * @return The connection map.
    * @throws InitializationException if connection could not be initialized.
    */
   static Map<String, JDBConnection> mapFromProperties(final Properties props) throws InitializationException {

      Map<String, Properties> globalPropertyMap = new InitUtil("property.", props, false).split();
      Map<String, Properties> connectionPropertyMap = new InitUtil("connection.", props, false).split();
      Map<String, JDBConnection> connectionMap = Maps.newLinkedHashMap();

      for(Map.Entry<String, Properties> connectionNameProperties : connectionPropertyMap.entrySet()) {
         String connectionName = connectionNameProperties.getKey();
         Properties connectionProperties = connectionNameProperties.getValue();
         connectionProperties.setProperty("name", connectionName);
         connectionMap.put(connectionName, new JDBConnection(connectionProperties, globalPropertyMap));
      }

      return connectionMap;
   }

   /**
    * Creates a map of connection vs. name from XML.
    * @param connectionsElem The parent element.
    * @return The connection map.
    * @throws InitializationException if connection could not be initialized.
    */
   static Map<String, JDBConnection> mapFromXML(final Element connectionsElem) throws InitializationException {
      if(connectionsElem == null) {
         return null;
      } else {

         List<Element> propertiesElements = DOMUtil.getChildElementsByTagName(connectionsElem, "properties");
         Map<String, Collection<Pair<String, String>>> globalPropertiesMap = Maps.newHashMap();
         for(Element propertiesElem : propertiesElements) {
            if(propertiesElem.getAttribute("name").length() > 0) {
               List<Pair<String, String>> globalProperties = Lists.newArrayListWithExpectedSize(4);
               NodeList propertyList = propertiesElem.getElementsByTagName("property");
               for(int j = 0; j < propertyList.getLength(); j++) {
                  Element propertyElem = (Element)propertyList.item(j);
                  String name = propertyElem.getAttribute("name").trim();
                  String val = propertyElem.getAttribute("value").trim();
                  globalProperties.add(new Pair<String, String>(name, val));
               }
               globalPropertiesMap.put(propertiesElem.getAttribute("name").trim(), globalProperties);
            } else {
               throw new InitializationException("All global 'properties' elements must have a 'name'");
            }
         }

         Map<String, JDBConnection> connectionMap = Maps.newHashMap();
         NodeList connectionList = connectionsElem.getElementsByTagName("connection");
         for(int i = 0; i < connectionList.getLength(); i++) {
            Element connectionElem = (Element)connectionList.item(i);
            JDBConnection connection = new JDBConnection(connectionElem, globalPropertiesMap);
            connectionMap.put(connection.name, connection);
         }

         return connectionMap.size() == 0 ? null : connectionMap;
      }
   }


   /**
    * Creates a connection from properties.
    * @param props The properties.
    * @param globalPropertiesMap A map containing global connection properties.
    * @throws InitializationException if required parameters are unspecified.
    */
   JDBConnection(final Properties props,
                 final Map<String, Properties> globalPropertiesMap) throws InitializationException {

      this.name = props.getProperty("name");
      this.user = props.getProperty("user") != null ? props.getProperty("user").trim() : null;
      this.password = props.getProperty("password") != null ? props.getProperty("password").trim() : null;
      this.testSQL = props.getProperty("testSQL") != null ? props.getProperty("testSQL").trim() : null;
      String _connectionString = props.getProperty("connectionString", "").trim();
      this.debug = props.getProperty("debug").equalsIgnoreCase("true");
      this.datasource = null;
      if(_connectionString.length() == 0) {
         throw new InitializationException("A 'connectionString' must be specified");
      }

      String createTimeout = props.getProperty("createTimeout");
      this.createTimeoutMillis = Util.millis(createTimeout, 0L);

      String testInterval = props.getProperty("testInterval", "").trim();
      if(testInterval.length() > 0) {
         this.testIntervalMillis = Util.millis(testInterval, Long.MAX_VALUE);
      } else {
         this.testIntervalMillis = Long.MAX_VALUE;
      }

      String propsRef = props.getProperty("properties", "").trim();
      if(propsRef.length() > 0) {
         Properties globalProperties = globalPropertiesMap.get(propsRef);
         if(globalProperties == null) {
            throw new InitializationException("The referenced global properties, '" + propsRef + "' is not defined");
         }
         this.connectionString = buildConnectionString(_connectionString, InitUtil.toPairCollection(globalProperties));
      } else {
         this.connectionString = _connectionString;
      }
   }

   /**
    * Builds a connection string with properties as parameters.
    * @param connectionString The connection string.
    * @param globalProperties The properties.
    * @return The connection string with properties as parameters.
    */
   private static String buildConnectionString(final String connectionString,
                                               final Collection<Pair<String, String>> globalProperties) {
      if(globalProperties == null || globalProperties.size() == 0) {
         return connectionString;
      } else {
         Map<String, String> propertyMap = Maps.newLinkedHashMap();
         for(Pair<String, String> prop : globalProperties) {
            String name = prop.getKey().trim();
            String val = prop.getValue().trim();
            if(name.length() > 0 && val.length() > 0) {
               propertyMap.put(name, val);
            }
         }

         StringBuilder buf = new StringBuilder();
         Iterator<Map.Entry<String, String>> iter = propertyMap.entrySet().iterator();
         if(iter.hasNext()) {
            Map.Entry<String, String> curr = iter.next();
            buf.append(curr.getKey()).append("=").append(curr.getValue());
            while(iter.hasNext()) {
               curr = iter.next();
               buf.append("&").append(curr.getKey()).append("=").append(curr.getValue());
            }
         }

         String propertyString = buf.toString();
         if(propertyString.length() > 0) {
            if(connectionString.indexOf('?') > 0) {
               return connectionString + "&" + propertyString;
            } else {
               return connectionString + "?" + propertyString;
            }
         } else {
            return connectionString;
         }
      }
   }

   /**
    * Creates a connection from XML.
    * @param connectionElem The connection element.
    * @throws InitializationException if required parameters are unspecified.
    */
   JDBConnection(final Element connectionElem) throws InitializationException {
      this(connectionElem, null);
   }

   /**
    * Creates a connection from XML with a map of global connection properties..
    * @param connectionElem The connection element.
    * @param globalPropertiesMap A map containing global connection properties.
    * @throws InitializationException if required parameters are unspecified.
    */
   JDBConnection(final Element connectionElem, final Map<String, Collection<Pair<String, String>>> globalPropertiesMap) throws InitializationException {
      name = connectionElem.getAttribute("name");
      user = connectionElem.getAttribute("user");
      password = connectionElem.getAttribute("password");
      testSQL = connectionElem.getAttribute("testSQL");
      String _connectionString = connectionElem.getAttribute("connectionString");
      debug = connectionElem.getAttribute("debug").equalsIgnoreCase("true");
      datasource = null;
      if(_connectionString.length() == 0) {
         throw new InitializationException("A 'connectionString' must be specified");
      }

      String createTimeout = connectionElem.getAttribute("createTimeout");
      this.createTimeoutMillis = Util.millis(createTimeout, 0L);

      String testInterval = connectionElem.getAttribute("testInterval");
      if(testInterval.length() > 0) {
         testIntervalMillis = Util.millis(testInterval, Long.MAX_VALUE);
      } else {
         testIntervalMillis = Long.MAX_VALUE;
      }


      Element propertiesElem = DOMUtil.getFirstChild(connectionElem, "properties");
      if(propertiesElem != null) {

         Map<String, String> propertyMap = Maps.newHashMap();

         String ref = propertiesElem.getAttribute("clone").trim();
         if(ref.length() > 0) {
            Collection<Pair<String, String>> globalProperties = globalPropertiesMap.get(ref);
            if(globalProperties != null) {
               for(Pair<String, String> prop : globalProperties) {
                  String name = prop.getKey().trim();
                  String val = prop.getValue().trim();
                  if(name.length() > 0) {
                     propertyMap.put(name, val);
                  }
               }
            } else {
               throw new InitializationException("The referenced global properties, '" + ref + "' is not defined");
            }
         }

         NodeList propertyList = propertiesElem.getElementsByTagName("property");
         if(propertyList.getLength() > 0) {
            for(int i = 0; i < propertyList.getLength(); i++) {
               Element propertyElem = (Element)propertyList.item(i);
               String name = propertyElem.getAttribute("name").trim();
               String val = propertyElem.getAttribute("value").trim();
               if(name.length() > 0) {
                  propertyMap.put(name, val);
               }
            }
         }

         StringBuilder buf = new StringBuilder();
         Iterator<Map.Entry<String, String>> iter = propertyMap.entrySet().iterator();
         if(iter.hasNext()) {
            Map.Entry<String, String> curr = iter.next();
            buf.append(curr.getKey()).append("=").append(curr.getValue());
            while(iter.hasNext()) {
               curr = iter.next();
               buf.append("&").append(curr.getKey()).append("=").append(curr.getValue());
            }
         }

         String propertyString = buf.toString();
         if(propertyString.length() > 0) {
            if(_connectionString.indexOf('?') > 0) {
               _connectionString = _connectionString + "&" + propertyString;
            } else {
               _connectionString = _connectionString + "?" + propertyString;
            }
         }
      }

      this.connectionString = _connectionString;
   }

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
      if(!StringUtil.hasContent(connectionString) && datasource == null) {
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
      int index = connectionString.indexOf('?');
      if(index > 0) {
         description = connectionString.substring(0, index);
      }

      if(StringUtil.hasContent(user)) {
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
