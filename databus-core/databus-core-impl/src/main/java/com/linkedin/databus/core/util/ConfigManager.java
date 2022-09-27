package com.linkedin.databus.core.util;
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


import java.io.Reader;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

/**
 * Manages the dynamic configuration the container
 * @author cbotev
 *
 * @param <D>   Dynamic config class
 */
public class ConfigManager<D extends ConfigApplier<D>> extends ConfigLoader<D>
{
  public final static String MODULE = ConfigManager.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);

  private D _readonlyConfig;

  public ConfigManager(String propPrefix, ConfigBuilder<D> dynConfigBuilder)
	                      throws InvalidConfigException
  {
	super(propPrefix, dynConfigBuilder);
    setNewConfig(_configBuilder.build());
  }

  @Override
  public D loadConfig(Map<?, ?> props) throws InvalidConfigException
  {
    D newConfig = super.loadConfig(props);
	return setNewConfig(newConfig);
  }

  @Override
  public D setSetting(String settingName, Object value) throws InvalidConfigException
  {
    D newConfig = super.setSetting(settingName, value);
    return setNewConfig(newConfig);
  }

  @Override
  public D loadConfigFromJson(Reader jsonReader) throws InvalidConfigException
  {
    D newConfig = super.loadConfigFromJson(jsonReader);
    return setNewConfig(newConfig);
  }

	public D getReadOnlyConfig()
	{
	  Lock readLock = acquireReadLock();
	  try
	  {
	    return _readonlyConfig;
	  }
	  finally
	  {
	    releaseLock(readLock);
	   }
	}

	public D setNewConfig(D newConfig)
	{
      D oldConfig = null;
	  Lock writeLock = acquireWriteLock();
	  try
	  {
	    oldConfig = _readonlyConfig;
	    _readonlyConfig = newConfig;

	      try
	      {
	        _readonlyConfig.applyNewConfig(oldConfig);
	      }
	      catch (Exception e)
	      {
	        LOG.error("Error updating config", e);
	      }
	  }
	  finally
	  {
	    releaseLock(writeLock);
	  }

	  return newConfig;
	}

}
