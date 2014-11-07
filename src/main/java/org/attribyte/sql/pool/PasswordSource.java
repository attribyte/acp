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

package org.attribyte.sql.pool;

/**
 * Allows connection passwords to be specified at runtime.
 */
public interface PasswordSource {

   /**
    * Gets the password for a named connection.
    * @param connectionName The connection name.
    * @return The password or <tt>null</tt> if none.
    */
   public String getPassword(String connectionName);

   /**
    * Gets the password for a connection string and username.
    * @param connectionString The connection string.
    * @param username The username.
    * @return The password or <tt>null</tt> if none.
    */
   public String getPassword(String connectionString, String username);
}