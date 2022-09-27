package com.linkedin.databus2.core.container.netty;
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


import static org.jboss.netty.channel.Channels.pipeline;

import java.nio.ByteOrder;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLogLevel;

import com.linkedin.databus2.core.container.ExtendedReadTimeoutHandler;
import com.linkedin.databus2.core.container.ExtendedWriteTimeoutHandler;
import com.linkedin.databus2.core.container.request.SimpleBinaryDatabusRequestDecoder;
import com.linkedin.databus2.core.container.request.SimpleBinaryDatabusResponseEncoder;

/** A Netty channel pipeline factory for processing of TCP commands */
public class TcpServerPipelineFactory implements ChannelPipelineFactory
{

  private final ServerContainer _serverContainer;
  private final ByteOrder _byteOrder;

  public TcpServerPipelineFactory(ServerContainer serverContainer, ByteOrder byteOrder)
  {
    super();
    _serverContainer = serverContainer;
    _byteOrder = byteOrder;
  }

  @Override
  public ChannelPipeline getPipeline() throws Exception
  {

    // Create a default pipeline implementation.
    ChannelPipeline pipeline = pipeline();


    pipeline.addLast("auto group register ",
                     new ConnectionChannelRegistrationHandler(_serverContainer.getTcpChannelGroup()));

    if (Logger.getRootLogger().isTraceEnabled())
    {
      pipeline.addLast("netty server traffic",
                       new LoggingHandler("netty server traffic", InternalLogLevel.DEBUG, true));
    }

    pipeline.addLast("outbound statistics collector",
                     new OutboundContainerStatisticsCollectingHandler(
                         _serverContainer.getContainerStatsCollector()));

    ExtendedWriteTimeoutHandler writeTimeoutHandler =
        new ExtendedWriteTimeoutHandler("server container " + _serverContainer.getContainerStaticConfig().getId(),
                                        _serverContainer.getNetworkTimeoutTimer(),
                                        _serverContainer.getContainerStaticConfig().getWriteTimeoutMs(),
                                        true);

    ExtendedReadTimeoutHandler readTimeoutHandler =
        new ExtendedReadTimeoutHandler("server container " + _serverContainer.getContainerStaticConfig().getId(),
                                       _serverContainer.getNetworkTimeoutTimer(),
                                       _serverContainer.getContainerStaticConfig().getReadTimeoutMs(),
                                       true);
    pipeline.addLast("read timeout", readTimeoutHandler);

    //add a handler to deal with write timeouts
    pipeline.addLast("server container write timeout handler", writeTimeoutHandler);

    pipeline.addLast("decoder",
                     new SimpleBinaryDatabusRequestDecoder(_serverContainer.getCommandsRegistry(),
                                                           readTimeoutHandler,
                                                           _byteOrder));
    pipeline.addLast("encoder", new SimpleBinaryDatabusResponseEncoder());

    // Fix for DDSDBUS-1000
//    pipeline.addLast("executionHandler", _serverContainer.getNettyExecHandler());

    //Dummy handler that will be automatically replaced depending on the current command being
    //executed
    pipeline.addLast(SimpleBinaryDatabusRequestDecoder.REQUEST_EXEC_HANDLER_NAME,
                     new LoggingHandler("netty server traffic", InternalLogLevel.DEBUG, true));


    return pipeline;
  }

}
