/*
 * Copyright 2011 Attribyte, LLC
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


package org.attribyte.sql.pool.contrib;

import org.attribyte.sql.pool.PasswordSource;

import java.util.Properties;

/**
 * A password source that uses <tt>Properties</tt> to
 * resolve passwords.
 * <p>
 * <ul>
 * <li>[connection name]=[password]</li>
 * <li>[username]@[connection string]=[password]</li>
 * </ul>
 * </p>
 */
public class PropertiesPasswordSource implements PasswordSource {

   /**
    * Creates a properties password source.
    * @param properties The properties.
    */
   public PropertiesPasswordSource(final Properties properties) {
      this.properties = properties;
   }

   @Override
   public String getPassword(String connectionName) {
      return properties.getProperty(connectionName);
   }

   @Override
   public String getPassword(String connectionString, String username) {
      StringBuilder buf = new StringBuilder(username);
      buf.append("@");
      buf.append(connectionString);
      return properties.getProperty(buf.toString());
   }

   private final Properties properties;
}