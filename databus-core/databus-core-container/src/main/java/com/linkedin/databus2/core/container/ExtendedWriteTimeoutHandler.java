package com.linkedin.databus2.core.container;
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


import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.LifeCycleAwareChannelHandler;
import org.jboss.netty.handler.timeout.WriteTimeoutException;
import org.jboss.netty.handler.timeout.WriteTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

/**
 * Extended implementation for write timeout handlers based on {@link WriteTimeoutHandler}.
 *
 * Main differences are:
 *
 * * Ability to automatically close the channel on timeout
 * * Ability to start with its own timer
 * * Automatic deregistration of owned timers
 * * Slightly more informative timeout exceptions.
 *
 * @author cbotev
 *
 */
public class ExtendedWriteTimeoutHandler extends WriteTimeoutHandler
                                             implements LifeCycleAwareChannelHandler
{
  public static final String MODULE = ExtendedWriteTimeoutHandler.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final String _name;
  private final boolean _closeOnTimeout;
  private final boolean _ownTimer;

  public ExtendedWriteTimeoutHandler(String name, Timer timer, long timeoutMs,
                                         boolean closeOnTimeout)
  {
    super((null != timer) ? timer : new HashedWheelTimer(timeoutMs, TimeUnit.MILLISECONDS, 10),
          timeoutMs, TimeUnit.MILLISECONDS);
    _name = name;
    _closeOnTimeout = closeOnTimeout;
    _ownTimer = (null == timer);
  }

  public void destroy()
  {
    if (_ownTimer)
    {
      LOG.info("releasing external resources");
      super.releaseExternalResources();
    }
  }

  @Override
  public void beforeAdd(ChannelHandlerContext arg0) throws Exception
  {
    //Nothing to do
  }

  @Override
  public void afterAdd(ChannelHandlerContext arg0) throws Exception
  {
    //Nothing to do
  }

  @Override
  public void beforeRemove(ChannelHandlerContext arg0) throws Exception
  {
    destroy();
  }

  @Override
  public void afterRemove(ChannelHandlerContext arg0) throws Exception
  {
    //Nothing to do
  }

  @Override
  public void releaseExternalResources()
  {
    destroy();
  }

  @Override
  protected void writeTimedOut(ChannelHandlerContext ctx)
  {
    Channels.fireExceptionCaught(ctx, new WriteTimeoutException(_name));
    if (_closeOnTimeout) ctx.getChannel().close(); //close the channel asynchronously
  }

}
