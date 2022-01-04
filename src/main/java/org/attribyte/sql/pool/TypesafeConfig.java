package org.attribyte.sql.pool;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.*;
import org.attribyte.api.InitializationException;
import org.attribyte.api.Logger;

import java.io.File;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TypesafeConfig {

   public static void main(String[] args) throws Exception {

      File configFile = new File("/home/matt/hg/acp/doc/config/sample.config");
      Config rootConfig = ConfigFactory.parseFile(configFile);
      List<ConnectionPool.Initializer> pools = poolsFromConfig(rootConfig, null, null);
      for(ConnectionPool.Initializer poolInit : pools) {
         ConnectionPool pool = poolInit.createPool();
         System.out.println(pool.getName());
         System.out.println("Total Segments: " + pool.getTotalSegments());

      }
   }

   static final int MISSING_INT = Integer.MIN_VALUE;
   static final long MISSING_LONG = Long.MIN_VALUE;

   static final String getString(final Config config, final String name) throws InitializationException {
      return getString(config, name, null);
   }

   static final String getString(final Config config, final String name,
                                 final String defaultValue) throws InitializationException {

      final String val;
      try {
         val = config.hasPath(name) ? config.getString(name) : defaultValue;
      } catch(ConfigException ce) {
         throw new InitializationException(ce.getMessage());
      }

      if(val == null) {
         throw new InitializationException("The property, '" + name + "', must be supplied");
      } else {
         return val.trim();
      }
   }

   static final int getInt(final Config config, final String name,
                           final int minValue) throws InitializationException {

      final int val;
      try {
         val = config.hasPath(name) ? config.getInt(name) : MISSING_INT;
      } catch(ConfigException ce) {
         throw new InitializationException(ce.getMessage());
      }

      if(val == MISSING_INT) {
         throw new InitializationException("The property, '" + name + "', must be supplied");
      } else if(val < minValue) {
         throw new InitializationException("The property, '" + name + "', must be > " + minValue);
      } else {
         return val;
      }
   }

   static final long getMilliseconds(final Config config, final String name) throws InitializationException {
      final long val;
      try {
         val = config.hasPath(name) ? config.getDuration(name, TimeUnit.MILLISECONDS) : MISSING_LONG;
      } catch(ConfigException ce) {
         throw new InitializationException(ce.getMessage());
      }

      if(val == MISSING_LONG) {
         throw new InitializationException("The property, '" + name + "', must be supplied");
      } else if(val < 0) {
         throw new InitializationException("The property, '" + name + "', must not be negative");
      } else {
         return val;
      }
   }

   /**
    * Gets all available keys in the config.
    * @param config The config.
    * @return The keys.
    */
   static final Set<String> uniquePrefixSet(final Config config) {

      Set<String> prefixSet = Sets.newHashSet();
      for(Map.Entry<String, ConfigValue> entry : config.entrySet()) {
         String key = entry.getKey();
         int index = key.indexOf('.');
         if(index > 0) {
            String prefix = key.substring(0, index);
            prefixSet.add(prefix);
         }
      }

      return prefixSet;
   }

   public static final Map<String, ConnectionPool> buildPools(final Collection<ConnectionPool.Initializer> initializers)
           throws InitializationException, SQLException {
      Map<String, ConnectionPool> poolMap = Maps.newHashMap();
      for(ConnectionPool.Initializer initializer : initializers) {
         ConnectionPool pool = initializer.createPool();
         poolMap.put(pool.getName(), pool);
      }

      return poolMap;
   }

   public static final List<ConnectionPool.Initializer> poolsFromConfig(final Config config,
                                                                        final PasswordSource passwordSource,
                                                                        final Logger logger) throws InitializationException {

      Map<String, JDBConnection> connectionMap = connectionMapFromConfig(config);
      final Config poolsConfig;
      if(config.hasPath("acp.pool")) {
         poolsConfig = config.getObject("acp.pool").toConfig();
      } else if(config.hasPath("pool")) {
         poolsConfig = config.getObject("pool").toConfig();
      } else {
         throw new InitializationException("A 'pool' object must be specified");
      }

      Set<String> poolNames = uniquePrefixSet(poolsConfig);
      if(poolNames.size() == 0) {
         throw new InitializationException("At least one pool must be specified");
      }

      List<ConnectionPool.Initializer> pools = Lists.newArrayListWithCapacity(poolNames.size());

      for(String poolName : poolNames) {
         pools.add(poolFromConfig(poolName, poolsConfig.getObject(poolName).toConfig(),
                 connectionMap, passwordSource, logger));
      }

      return pools;
   }

   /**
    * Initializes a pool from properties.
    * @param poolName The pool name.
    * @param baseConfig The base configuration. Defaults from {@code reference.conf} will supply defaults if they make sense.
    * @param connectionMap A map of configured connections.
    * @param passwordSource A password source. May be {@code null}.
    * @return The initializer
    * @throws InitializationException if configuration is invalid.
    */
   static final ConnectionPool.Initializer poolFromConfig(final String poolName,
                                                          final Config baseConfig,
                                                          final Map<String, JDBConnection> connectionMap,
                                                          final PasswordSource passwordSource,
                                                          final Logger logger) throws InitializationException {

      ConnectionPool.Initializer poolInit = new ConnectionPool.Initializer();
      poolInit.setName(poolName);
      poolInit.setLogger(logger);

      Config poolRef = ConfigFactory.load().getObject("acp.defaults.pool").toConfig();
      Config config = baseConfig.withFallback(poolRef);

      int minActiveSegments = getInt(config, "minActiveSegments", 1);
      poolInit.setMinActiveSegments(minActiveSegments);
      poolInit.setSaturatedAcquireTimeout(getMilliseconds(config, "saturatedAcquireTimeout"), TimeUnit.MILLISECONDS);
      poolInit.setIdleCheckInterval(getMilliseconds(config, "idleCheckInterval"), TimeUnit.MILLISECONDS);

      Map<String, ConnectionPoolSegment.Initializer> initializerMap = Maps.newHashMap();
      Map<String, Config> segmentConfigMap = Maps.newHashMap();
      List<String> segmentNames = Lists.newArrayListWithExpectedSize(8);

      if(!config.hasPath("segments")) {
         segmentNames = Lists.newArrayList(uniquePrefixSet(config));
         Collections.sort(segmentNames);
         for(String segmentName : segmentNames) {
            segmentConfigMap.put(segmentName, config.getObject(segmentName).toConfig());
         }
      } else {
         for(ConfigObject obj : config.getObjectList("segments")) {
            Config segmentConfig = obj.toConfig();
            String segmentName = getString(segmentConfig, "name");
            segmentNames.add(segmentName);
            segmentConfigMap.put(segmentName, segmentConfig);
         }
      }

      Config segmentRef = ConfigFactory.load().getObject("acp.defaults.segment").toConfig();

      for(String segmentName : segmentNames) {
         Config segmentConfig = segmentConfigMap.get(segmentName);
         String cloneSegmentName = getString(segmentConfig, "clone", "");
         ConnectionPoolSegment.Initializer segmentInitializer;
         ConnectionPoolSegment.Initializer baseInitializer = initializerMap.get(cloneSegmentName);
         if(cloneSegmentName.length() > 0 && baseInitializer == null) {
            throw new InitializationException("Forward/Invalid 'clone' reference to " + cloneSegmentName);
         } else if(baseInitializer == null) {
            segmentConfig = segmentConfig.withFallback(segmentRef);
            segmentInitializer = new ConnectionPoolSegment.Initializer();
         } else {
            Config cloneSegmentConfig = segmentConfigMap.get(cloneSegmentName);
            segmentConfig = segmentConfig.withFallback(cloneSegmentConfig).withFallback(segmentRef);
            segmentInitializer = new ConnectionPoolSegment.Initializer(baseInitializer);
         }

         segmentInitializer.setPasswordSource(passwordSource);
         segmentInitializer.setLogger(logger);

         segmentFromConfig(poolName, segmentName, segmentConfig, segmentInitializer, connectionMap);
         segmentConfigMap.put(segmentName, segmentConfig);
         initializerMap.put(segmentName, segmentInitializer);
      }

      int startActiveSegments = getInt(config, "startActiveSegments", 0);
      for(int i = 0; i < segmentNames.size(); i++) {
         if(i < startActiveSegments) {
            poolInit.addActiveSegment(initializerMap.get(segmentNames.get(i)).createSegment());
         } else {
            poolInit.addReserveSegment(initializerMap.get(segmentNames.get(i)).createSegment());
         }
      }

      return poolInit;
   }

   /**
    * Sets initializer properties from properties.
    * @param poolName The pool name.
    * @param segmentName The segment name.
    * @param config The config.
    * @param initializer An initializer.
    * @param connectionMap A map of connection vs. name.
    * @return The segment initializer.
    * @throws InitializationException if configuration is invalid.
    */
   static final ConnectionPoolSegment.Initializer segmentFromConfig(String poolName,
                                                                    String segmentName,
                                                                    final Config config,
                                                                    ConnectionPoolSegment.Initializer initializer,
                                                                    Map<String, JDBConnection> connectionMap) throws InitializationException {

      initializer.setName(poolName + "." + segmentName);
      initializer.setSize(getInt(config, "size", 1));
      initializer.setCloseConcurrency(getInt(config, "closeConcurrency", 0));
      initializer.setTestOnLogicalOpen(getString(config, "testOnLogicalOpen", "false").equalsIgnoreCase("true"));
      initializer.setTestOnLogicalClose(getString(config, "testOnLogicalClose", "false").equalsIgnoreCase("true"));

      String incompleteTransactionPolicy = getString(config, "incompleteTransactionPolicy");
      initializer.setIncompleteTransactionOnClosePolicy(
              ConnectionPoolConnection.IncompleteTransactionPolicy.fromString(incompleteTransactionPolicy, ConnectionPoolConnection.IncompleteTransactionPolicy.REPORT)
      );

      String openStatementPolicy = getString(config, "openStatementPolicy");
      initializer.setOpenStatementOnClosePolicy(
              ConnectionPoolConnection.OpenStatementPolicy.fromString(openStatementPolicy, ConnectionPoolConnection.OpenStatementPolicy.SILENT)
      );

      String forceRealClosePolicy = getString(config, "forceRealClosePolicy");
      initializer.setForceRealClosePolicy(
              ConnectionPoolConnection.ForceRealClosePolicy.fromString(forceRealClosePolicy, ConnectionPoolConnection.ForceRealClosePolicy.CONNECTION)
      );

      String activityTimeoutPolicy = getString(config, "activityTimeoutPolicy");
      initializer.setActivityTimeoutPolicy(
              ConnectionPoolConnection.ActivityTimeoutPolicy.fromString(activityTimeoutPolicy, ConnectionPoolConnection.ActivityTimeoutPolicy.LOG)
      );

      initializer.setCloseTimeLimitMillis(getMilliseconds(config, "closeTimeLimit"));

      String connectionName = getString(config, "connectionName", "");
      JDBConnection conn = null;
      if(connectionName.length() > 0) {
         conn = connectionMap.get(connectionName);
         if(conn == null) {
            throw new InitializationException("No connection defined with name '" + connectionName + "'");
         }
      }

      if(conn == null && !initializer.hasConnection()) {
         throw new InitializationException("A 'connectionName' must be specified for segment, '" + segmentName + "'");
      } else if(conn != null) {
         initializer.setConnection(conn);
      }

      initializer.setAcquireTimeout(getMilliseconds(config, "acquireTimeout"), TimeUnit.MILLISECONDS);
      initializer.setActiveTimeout(getMilliseconds(config, "activeTimeout"), TimeUnit.MILLISECONDS);
      initializer.setConnectionLifetime(getMilliseconds(config, "connectionLifetime"), TimeUnit.MILLISECONDS);
      initializer.setIdleTimeBeforeShutdown(getMilliseconds(config, "idleTimeBeforeShutdown"), TimeUnit.MILLISECONDS);
      initializer.setMinActiveTime(getMilliseconds(config, "minActiveTime"), TimeUnit.MILLISECONDS);
      initializer.setMaxConcurrentReconnects(getInt(config, "reconnectConcurrency", 0));
      initializer.setMaxReconnectDelay(getMilliseconds(config, "reconnectMaxWaitTime"), TimeUnit.MILLISECONDS);
      initializer.setActiveTimeoutMonitorFrequency(getMilliseconds(config, "activeTimeoutMonitorFrequency"), TimeUnit.MILLISECONDS);
      return initializer;
   }

   /**
    * Creates a map of connection vs. name from properties.
    * @return The connection map.
    * @throws org.attribyte.api.InitializationException if connection could not be initialized.
    */
   static Map<String, JDBConnection> connectionMapFromConfig(Config config) throws InitializationException {

      final Config globalPropertyConfig;
      if(config.hasPath("acp.property")) {
         globalPropertyConfig = config.getObject("acp.property").toConfig();
      } else if(config.hasPath("property")) {
         globalPropertyConfig = config.getObject("property").toConfig();
      } else {
         globalPropertyConfig = null;
      }


      final Config connectionsConfig;
      if(config.hasPath("acp.connection")) {
         connectionsConfig = config.getObject("acp.connection").toConfig();
      } else if(config.hasPath("connection")) {
         connectionsConfig = config.getObject("connection").toConfig();
      } else {
         return Collections.emptyMap();
      }

      Map<String, JDBConnection> connectionMap = Maps.newLinkedHashMap();

      for(String connectionName : uniquePrefixSet(connectionsConfig)) {
         Config connectionConfig = connectionsConfig.getObject(connectionName).toConfig();
         connectionMap.put(connectionName, createConnection(connectionName, connectionConfig, globalPropertyConfig));
      }

      return connectionMap;
   }

   /**
    * Creates a connection from properties.
    * @throws InitializationException if required parameters are unspecified.
    */
   static JDBConnection createConnection(final String connectionName,
                                         final Config baseConfig,
                                         final Config globalPropertyConfig) throws InitializationException {

      Config connectionRef = ConfigFactory.load().getObject("acp.defaults.connection").toConfig();
      Config config = baseConfig.withFallback(connectionRef);

      final String user = getString(config, "user");

      final String password = getString(config, "password");
      final String testSQL = getString(config, "testSQL", "");
      String connectionString = getString(config, "connectionString");
      final boolean debug = getString(config, "debug", "false").equalsIgnoreCase("true");

      final long createTimeoutMillis = getMilliseconds(config, "createTimeout");
      final long testIntervalMillis = getMilliseconds(config, "testInterval");


      String propsRef = getString(config, "properties", "");
      Config propsConfig;
      if(propsRef.length() > 0) {
         if(globalPropertyConfig.hasPath(propsRef)) {
            propsConfig = globalPropertyConfig.getObject(propsRef).toConfig();
         } else {
            throw new InitializationException("The referenced global properties, '" + propsRef + "' is not defined");
         }

         connectionString = buildConnectionString(connectionString, propsConfig);
      }

      return new JDBConnection(connectionName, user, password, connectionString,
              createTimeoutMillis,
              testSQL.length() > 0 ? testSQL : null,
              testIntervalMillis, debug);
   }

   /**
    * Initialize JDBC drivers from config.
    * @throws InitializationException if driver could not be initialized
    */
   static void initDrivers(final Config config) throws InitializationException {
      for(String driverName : uniquePrefixSet(config)) {
         Config driverConfig = config.getObject(driverName).toConfig();
         if(driverConfig.hasPath("class")) {
            String className = driverConfig.getString("class");
            try {
               Class.forName(className);
            } catch(Throwable t) {
               throw new InitializationException("Unable to load JDBC driver, '" + className + "'", t);
            }
         } else {
            throw new InitializationException("A 'class' must be specified for JDBC driver, '" + driverName + "'");
         }
      }
   }

   /**
    * Builds a connection string with properties as parameters.
    * @param connectionString The connection string.
    * @return The connection string with properties as parameters.
    */
   private static String buildConnectionString(final String connectionString,
                                               final Config config) {

      if(config == null) {
         return connectionString;
      }

      StringBuilder buf = new StringBuilder();
      Iterator<Map.Entry<String, ConfigValue>> iter = config.entrySet().iterator();
      if(iter.hasNext()) {
         Map.Entry<String, ConfigValue> curr = iter.next();
         buf.append(curr.getKey()).append("=").append(curr.getValue().unwrapped().toString());
      } else {
         return connectionString;
      }

      while(iter.hasNext()) {
         Map.Entry<String, ConfigValue> curr = iter.next();
         buf.append("&").append(curr.getKey()).append("=").append(curr.getValue().unwrapped().toString());
      }

      if(connectionString.indexOf('?') > 0) {
         return connectionString + "&" + buf;
      } else {
         return connectionString + "?" + buf;
      }
   }
}
