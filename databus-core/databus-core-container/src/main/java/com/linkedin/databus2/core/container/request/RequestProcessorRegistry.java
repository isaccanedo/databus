package com.linkedin.databus2.core.container.request;
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


import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.ProcessorRegistrationConflictException;
import com.linkedin.databus2.core.container.request.RequestProcessor;
import com.linkedin.databus2.core.container.request.UnknownCommandException;

/**
 * Maintains a mapping of command names to processors
 * @author cbotev
 *
 */
public class RequestProcessorRegistry 
{
	
	private static final RequestProcessor UNKOWN_COMMAND_PROCESSOR = new UnknownCommandProcessor();
	
	private final HashMap<String, RequestProcessor> _processors = new HashMap<String, RequestProcessor>();
	
	public void register(String commandName, RequestProcessor processor) throws ProcessorRegistrationConflictException
	{
		if (_processors.containsKey(commandName))
		{
			throw new ProcessorRegistrationConflictException(commandName);
		}
		
		_processors.put(commandName, processor);
	}
	
	public void reregister(String commandName, RequestProcessor processor) throws ProcessorRegistrationConflictException
    {
        if (_processors.containsKey(commandName))
        {
            unregister(commandName);
        }
        
        _processors.put(commandName, processor);
    }
	
	public void unregister(String commandName)
	{
		_processors.remove(commandName);
	}
	
	public Future<DatabusRequest> run(DatabusRequest request)
	{
		RequestProcessor processor = _processors.get(request.getName());
		if (null == processor)
		{
			processor = UNKOWN_COMMAND_PROCESSOR;
		}
		request.setProcessor(processor);
		
		ExecutorService procExecutor = processor.getExecutorService();
		if (null != procExecutor)
		{
		  return procExecutor.submit(request); 
		}
		else
		{
          request.call();
          return request;
		}
	}
}

class UnknownCommandProcessor implements RequestProcessor
{
	
	private static final ExecutorService UNKNOWN_COMMAND_PROCESSOR_EXECUTOR = 
			new ThreadPoolExecutor(1, 1, Integer.MAX_VALUE, TimeUnit.DAYS, 
			                       new LinkedBlockingDeque<Runnable>());

	@Override
	public ExecutorService getExecutorService() {
		return UNKNOWN_COMMAND_PROCESSOR_EXECUTOR;
	}

	@Override
	public DatabusRequest process(DatabusRequest request) throws IOException {
		request.setError(new UnknownCommandException(request.getName()));
		return request;
	}

}
