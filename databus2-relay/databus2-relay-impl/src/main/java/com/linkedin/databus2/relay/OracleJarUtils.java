package com.linkedin.databus2.relay;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.linkedin.databus2.core.DatabusException;

public class OracleJarUtils {

   private static final Logger LOG = Logger.getLogger(OracleJarUtils.class);

    /**
     * 
     * @param set URI after loading the class OracleDataSource, and instantiating an object 
     * @return
     * @throws DatabusException
     */
    public static DataSource createOracleDataSource(String uri)
    throws Exception
	{
		// Create the OracleDataSource used to get DB connection(s)
		DataSource ds = null;
		try
		{
			Class oracleDataSourceClass = loadClass("oracle.jdbc.pool.OracleDataSource");
			Object ods = oracleDataSourceClass.newInstance(); 	  
			ds = (DataSource) ods;

			Method setURLMethod = oracleDataSourceClass.getMethod("setURL", String.class);
			Method getConnectionPropertiesMethod = oracleDataSourceClass.getMethod("getConnectionProperties");
			Method setConnectionPropertiesMethod = oracleDataSourceClass.getMethod("setConnectionProperties", Properties.class);
			setURLMethod.invoke(ods, uri);
			// DDS-425. Set oracle.jdbc.V8Compatible so DATE column will be mapped to java.sql.TimeStamp
			//          oracle jdbc 11g fixed this. So we can skip this after will upgrade jdbc to 11g.

			Properties prop = (Properties) getConnectionPropertiesMethod.invoke(ods);
			if (prop == null)
			{
				prop = new Properties();
			}
			//prop.put("oracle.jdbc.V8Compatible","true");
			setConnectionPropertiesMethod.invoke(ods, prop);
		} catch (Exception e)
		{
			String errMsg = "Error trying to create an Oracle DataSource"; 
			LOG.error(errMsg, e);
			throw e;
		}
	    return ds;
    }
    
    /**
     * 
     * @param The class which needs to be loaded dynamically
     * @return
     * @throws DatabusException
     */
    public static Class loadClass(String className)
    throws Exception
    {
    	try
    	{
    	  Class cName = OracleJarUtils.class.getClassLoader().loadClass(className);
  		  return cName;    		
    	} catch (Exception e)
    	{
    		LOG.error("Error loading a class " + className + " from ojdbc jar", e);
    		throw e;
    	}
    }
    
}
