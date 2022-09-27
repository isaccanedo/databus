package com.linkedin.databus.client.netty;
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


import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.DefaultHttpChunkTrailer;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.ChunkedBodyReadableByteChannel;
import com.linkedin.databus.client.DatabusBootstrapConnection;
import com.linkedin.databus.client.DatabusBootstrapConnectionStateMessage;
import com.linkedin.databus.client.DatabusRelayConnectionStateMessage;
import com.linkedin.databus.client.netty.TestResponseProcessors.TestRemoteExceptionHandler.ExceptionType;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusEventV1Factory;
import com.linkedin.databus.core.async.ActorMessageQueue;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.container.DatabusHttpHeaders;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.container.request.RegisterResponseMetadataEntry;
import com.linkedin.databus2.test.TestUtil;

public class TestResponseProcessors
{
  @BeforeClass
  public void setUp()
  {
    TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestResponseProcessors_", ".log",
                                             Level.OFF);
  }

  @Test
  public void testStreamHappyPathChunked() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    StreamHttpResponseProcessor  processor =
        new StreamHttpResponseProcessor(null, queue, stateMsg, null);
    ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(100);
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", false, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, stateMsg._channel.hasNoMoreChunks());
  }

  @Test
  public void testStreamHappyPathNonChunked() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    StreamHttpResponseProcessor  processor =
        new StreamHttpResponseProcessor(null, queue, stateMsg, null);

    processor.startResponse(httpResponse);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", false, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, stateMsg._channel.hasNoMoreChunks());
  }

  @Test
  public void testStreamExceptionAtStart() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    StreamHttpResponseProcessor  processor =
        new StreamHttpResponseProcessor(null, queue, stateMsg, null);
    ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(100);
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.channelException(new Exception("dummy exception"));
    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, (null == stateMsg._channel));
  }

  @Test
  public void testStreamExceptionAfterStartCase1() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    StreamHttpResponseProcessor  processor =
        new StreamHttpResponseProcessor(null, queue, stateMsg, null);
    ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(100);
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.channelException(new Exception("dummy exception"));
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, stateMsg._channel.hasNoMoreChunks());
  }

  @Test
  public void testStreamExceptionAfterStartCase2() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    StreamHttpResponseProcessor  processor =
        new StreamHttpResponseProcessor(null, queue, stateMsg, null);
    ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(100);
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.channelException(new Exception("dummy exception"));
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, stateMsg._channel.hasNoMoreChunks());
  }

  @Test
  public void testStreamExceptionAfterStartCase3() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    StreamHttpResponseProcessor  processor =
        new StreamHttpResponseProcessor(null, queue, stateMsg, null);
    ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(100);
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.channelException(new Exception("dummy exception"));
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, stateMsg._channel.hasNoMoreChunks());
  }

  @Test
  public void testStreamExceptionAfterFinish() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    StreamHttpResponseProcessor  processor =
        new StreamHttpResponseProcessor(null, queue, stateMsg, null);
    ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(100);
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();
    processor.channelException(new Exception("dummy exception"));

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, stateMsg._channel.hasNoMoreChunks());
  }

  @Test
  public void testStreamException() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    StreamHttpResponseProcessor  processor =
        new StreamHttpResponseProcessor(null, queue, stateMsg, null);

    processor.channelException(new Exception("dummy exception"));

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, null == stateMsg._channel);
  }

  @Test
  public void testRegisterHappyPathChunked() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    RegisterHttpResponseProcessor  processor =
        new RegisterHttpResponseProcessor(null, queue, stateMsg, null);
    ChannelBuffer buf = getRegisterResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", false, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_SUCCESS, gotMsg._state);
    Assert.assertEquals("Register Response Id Check",true, stateMsg._registerResponse.containsKey(new Long(202)));
    Assert.assertEquals("Register Response Id Check",1, stateMsg._registerResponse.size());
  }

  @Test
  public void testRegisterHappyPathNonChunked() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    RegisterHttpResponseProcessor  processor =
        new RegisterHttpResponseProcessor(null, queue, stateMsg, null);
    ChannelBuffer buf = getRegisterResponse();
    httpResponse.setContent(buf);
    httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH,"1000");
    processor.startResponse(httpResponse);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", false, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    System.out.println("Long Max is :" + Long.MAX_VALUE);
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_SUCCESS, gotMsg._state);
    Assert.assertEquals("Register Response Id Check",1, stateMsg._registerResponse.size());
  }

  @Test
  public void testRegisterExceptionAtStart() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    RegisterHttpResponseProcessor  processor =
        new RegisterHttpResponseProcessor(null, queue, stateMsg, null);
    ChannelBuffer buf = getRegisterResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.channelException(new Exception("dummy exception"));
    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, (null == stateMsg._registerResponse));
  }

  @Test
  public void testRegisterExceptionAfterStartCase1() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    RegisterHttpResponseProcessor  processor =
        new RegisterHttpResponseProcessor(null, queue, stateMsg, null);
    ChannelBuffer buf = getRegisterResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.channelException(new Exception("dummy exception"));
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, (null == stateMsg._registerResponse));
  }

  @Test
  public void testRegisterExceptionAfterStartCase2() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    RegisterHttpResponseProcessor  processor =
        new RegisterHttpResponseProcessor(null, queue, stateMsg, null);
    ChannelBuffer buf = getRegisterResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.channelException(new Exception("dummy exception"));
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, (null == stateMsg._registerResponse));
  }

  @Test
  public void testRegisterExceptionAfterStartCase3() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    RegisterHttpResponseProcessor  processor =
        new RegisterHttpResponseProcessor(null, queue, stateMsg, null);
    ChannelBuffer buf = getRegisterResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.channelException(new Exception("dummy exception"));
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, (null == stateMsg._registerResponse));
  }

  @Test
  public void testRegisterExceptionAfterFinish() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    RegisterHttpResponseProcessor  processor =
        new RegisterHttpResponseProcessor(null, queue, stateMsg, null);
    ChannelBuffer buf = getRegisterResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();
    processor.channelException(new Exception("dummy exception"));

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_SUCCESS, gotMsg._state);
    Assert.assertEquals("Register Response Id Check",true, stateMsg._registerResponse.containsKey(new Long(202)));
    Assert.assertEquals("Register Response Id Check",1, stateMsg._registerResponse.size());
  }

  @Test
  public void testRegisterExceptionChannelClose() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    RegisterHttpResponseProcessor  processor =
        new RegisterHttpResponseProcessor(null, queue, stateMsg, null);

    processor.channelException(new Exception("dummy exception"));

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, null == stateMsg._registerResponse);
  }

  @Test
  public void testSourceHappyPathChunked() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor =
        new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(null, queue,
                                                                             stateMsg, null);
    ChannelBuffer buf = getSourcesResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", false, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_SUCCESS, gotMsg._state);
    Assert.assertEquals("Register Response Id Check",1, stateMsg._sourcesResponse.size());
    Assert.assertEquals("Register Response Id Check",new Long(301), stateMsg._sourcesResponse.get(0).getId());
  }

  @Test
  public void testSourceHappyPathNonChunked() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor =
        new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(null, queue,
                                                                             stateMsg, null);
    ChannelBuffer buf = getSourcesResponse();
    httpResponse.setContent(buf);
    httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH,"1000");
    processor.startResponse(httpResponse);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", false, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    System.out.println("Long Max is :" + Long.MAX_VALUE);
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_SUCCESS, gotMsg._state);
    Assert.assertEquals("Register Response Id Check",1, stateMsg._sourcesResponse.size());
    Assert.assertEquals("Register Response Id Check",new Long(301), stateMsg._sourcesResponse.get(0).getId());
  }

  @Test
  public void testSourceExceptionAtStart() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor =
        new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(null, queue,
                                                                             stateMsg, null);
    ChannelBuffer buf = getSourcesResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.channelException(new Exception("dummy exception"));
    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, (null == stateMsg._sourcesResponse));
  }

  @Test
  public void testSourceExceptionAfterStartCase1() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor =
        new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(null, queue,
                                                                             stateMsg, null);
    ChannelBuffer buf = getSourcesResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.channelException(new Exception("dummy exception"));
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, (null == stateMsg._sourcesResponse));
  }

  @Test
  public void testSourceExceptionAfterStartCase2() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor =
        new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(null, queue,
                                                                             stateMsg, null);
    ChannelBuffer buf = getSourcesResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.channelException(new Exception("dummy exception"));
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, (null == stateMsg._sourcesResponse));
  }

  @Test
  public void testSourceExceptionAfterStartCase3() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor =
        new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(null, queue,
                                                                             stateMsg, null);
    ChannelBuffer buf = getSourcesResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.channelException(new Exception("dummy exception"));
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, (null == stateMsg._sourcesResponse));
  }

  @Test
  public void testSourceExceptionAfterFinishResponse() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor =
        new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(null, queue,
                                                                             stateMsg, null);
    ChannelBuffer buf = getSourcesResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();
    processor.channelException(new Exception("dummy exception"));

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_SUCCESS, gotMsg._state);
    Assert.assertEquals("Register Response Id Check",1, stateMsg._sourcesResponse.size());
    Assert.assertEquals("Register Response Id Check",new Long(301), stateMsg._sourcesResponse.get(0).getId());
  }

  @Test
  public void testSourceExceptionChannelClose() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor =
        new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(null, queue,
		                                                                           stateMsg, null);

    processor.channelException(new Exception("dummy exception"));

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No More CHunks",true, null == stateMsg._sourcesResponse);
  }


  @Test
  public void testStartSCNHappyPathChunked() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    BootstrapStartScnHttpResponseProcessor  processor =
        new BootstrapStartScnHttpResponseProcessor(null, queue, stateMsg, cp, remoteExHandler,
                                                   null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", false, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_SUCCESS, gotMsg._state);
    Assert.assertEquals("StartSCN Response Id Check",new Long(5678912),cp.getBootstrapStartScn());
  }

  @Test
  public void testStartSCNHappyPathNonChunked() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    BootstrapStartScnHttpResponseProcessor  processor =
        new BootstrapStartScnHttpResponseProcessor(null, queue, stateMsg, cp, remoteExHandler,
                                                   null);
    ChannelBuffer buf = getScnResponse();
    httpResponse.setContent(buf);
    httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH,"1000");
    processor.startResponse(httpResponse);
    processor.finishResponse();


    Assert.assertEquals("Error Handled", false, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    System.out.println("Long Max is :" + Long.MAX_VALUE);
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_SUCCESS, gotMsg._state);
    Assert.assertEquals("StartSCN Response Id Check",new Long(5678912),cp.getBootstrapStartScn());
  }

  @Test
  public void testStartSCNBootstrapTooOldException() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    remoteExHandler._exType= ExceptionType.BOOTSTRAP_TOO_OLD_EXCEPTION;
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    BootstrapStartScnHttpResponseProcessor  processor =
        new BootstrapStartScnHttpResponseProcessor(null, queue, stateMsg, cp, remoteExHandler,
                                                   null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", false, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No response",true, (null == stateMsg._cp));
  }

  @Test
  public void testStartSCNOtherException() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    httpResponse.setHeader(DatabusHttpHeaders.DATABUS_ERROR_CAUSE_CLASS_HEADER, "Other Dummy Error");
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    remoteExHandler._exType= ExceptionType.OTHER_EXCEPTION;
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    BootstrapStartScnHttpResponseProcessor  processor =
        new BootstrapStartScnHttpResponseProcessor(null, queue, stateMsg, cp, remoteExHandler,
                                                   null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", false, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No response",true, (null == stateMsg._cp));
  }

  @Test
  public void testStartSCNExceptionAtStart() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    BootstrapStartScnHttpResponseProcessor  processor =
        new BootstrapStartScnHttpResponseProcessor(null, queue, stateMsg, cp, remoteExHandler,
                                                   null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.channelException(new Exception("dummy exception"));
    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No response",true, (null == stateMsg._cp));
  }

  @Test
  public void testStartSCNExceptionAfterStartCase1() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    BootstrapStartScnHttpResponseProcessor  processor =
        new BootstrapStartScnHttpResponseProcessor(null, queue, stateMsg, cp, remoteExHandler,
                                                   null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.channelException(new Exception("dummy exception"));
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No response",true, (null == stateMsg._cp));
  }

  @Test
  public void testStartSCNExceptionAfterStartCase2() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    BootstrapStartScnHttpResponseProcessor  processor =
        new BootstrapStartScnHttpResponseProcessor(null, queue, stateMsg, cp, remoteExHandler,
                                                   null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.channelException(new Exception("dummy exception"));
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No response",true, (null == stateMsg._cp));
  }

  @Test
  public void testStartSCNExceptionAfterStartCase3() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    BootstrapStartScnHttpResponseProcessor  processor =
        new BootstrapStartScnHttpResponseProcessor(null, queue, stateMsg, cp, remoteExHandler,
                                                   null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.channelException(new Exception("dummy exception"));
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No response",true, (null == stateMsg._cp));
  }

  @Test
  public void testStartSCNExceptionAfterFinish() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    BootstrapStartScnHttpResponseProcessor  processor =
        new BootstrapStartScnHttpResponseProcessor(null, queue, stateMsg, cp, remoteExHandler,
                                                   null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();
    processor.channelException(new Exception("dummy exception"));

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_SUCCESS, gotMsg._state);
    Assert.assertEquals("StartSCN Response Id Check",new Long(5678912),cp.getBootstrapStartScn());
  }

  @Test
  public void testStartSCNException() throws Exception
  {
    // Both Exception and ChannelClose getting triggered
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    BootstrapStartScnHttpResponseProcessor  processor =
        new BootstrapStartScnHttpResponseProcessor(null, queue, stateMsg, cp, remoteExHandler,
                                                   null);
    processor.channelException(new Exception("dummy exception"));

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No response",true, (null == stateMsg._cp));
  }

  @Test
  public void testTargetSCNHappyPathChunked() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    BootstrapTargetScnHttpResponseProcessor  processor =
        new BootstrapTargetScnHttpResponseProcessor(null, queue, stateMsg, cp,
                                                    remoteExHandler, null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", false, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_SUCCESS, gotMsg._state);
    Assert.assertEquals("StartSCN Response Id Check",new Long(5678912),cp.getBootstrapTargetScn());
  }

  @Test
  public void testTargetSCNHappyPathNonChunked() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    BootstrapTargetScnHttpResponseProcessor  processor =
        new BootstrapTargetScnHttpResponseProcessor(null, queue, stateMsg, cp,
                                                    remoteExHandler, null);
    ChannelBuffer buf = getScnResponse();
    httpResponse.setContent(buf);
    httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH,"1000");
    processor.startResponse(httpResponse);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", false, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    System.out.println("Long Max is :" + Long.MAX_VALUE);
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_SUCCESS, gotMsg._state);
    Assert.assertEquals("StartSCN Response Id Check",new Long(5678912),cp.getBootstrapTargetScn());
  }

  @Test
  public void testTargetSCNBootstrapTooOldException() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    remoteExHandler._exType= ExceptionType.BOOTSTRAP_TOO_OLD_EXCEPTION;
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
    BootstrapTargetScnHttpResponseProcessor  processor =
        new BootstrapTargetScnHttpResponseProcessor(null, queue, stateMsg, cp, remoteExHandler, null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", false, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No response",true, (null == stateMsg._cp));
  }

  @Test
  public void testTargetSCNOtherException() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    httpResponse.setHeader(DatabusHttpHeaders.DATABUS_ERROR_CAUSE_CLASS_HEADER, "Other Dummy Error");
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    remoteExHandler._exType= ExceptionType.OTHER_EXCEPTION;
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
    BootstrapTargetScnHttpResponseProcessor  processor =
        new BootstrapTargetScnHttpResponseProcessor(null, queue, stateMsg, cp,
                                                    remoteExHandler, null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", false, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No response",true, (null == stateMsg._cp));
  }

  @Test
  public void testTargetSCNExceptionAtStart() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
    BootstrapTargetScnHttpResponseProcessor  processor =
        new BootstrapTargetScnHttpResponseProcessor(null, queue, stateMsg, cp,
                                                    remoteExHandler, null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.channelException(new Exception("dummy exception"));
    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No response",true, (null == stateMsg._cp));
  }

  @Test
  public void testTargetSCNExceptionAfterStartCase1() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
    BootstrapTargetScnHttpResponseProcessor  processor =
        new BootstrapTargetScnHttpResponseProcessor(null, queue, stateMsg, cp,
                                                    remoteExHandler, null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.channelException(new Exception("dummy exception"));
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No response",true, (null == stateMsg._cp));
  }

  @Test
  public void testTargetSCNExceptionAfterStartCase2() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
    BootstrapTargetScnHttpResponseProcessor  processor =
        new BootstrapTargetScnHttpResponseProcessor(null, queue, stateMsg, cp,
                                                    remoteExHandler, null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.channelException(new Exception("dummy exception"));
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No response",true, (null == stateMsg._cp));
  }

  @Test
  public void testTargetSCNExceptionAfterStartCase3() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
    BootstrapTargetScnHttpResponseProcessor  processor =
        new BootstrapTargetScnHttpResponseProcessor(null, queue, stateMsg, cp,
                                                    remoteExHandler, null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.channelException(new Exception("dummy exception"));
    processor.finishResponse();

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No response",true, (null == stateMsg._cp));
  }

  @Test
  public void testTargetSCNExceptionAfterFinish() throws Exception
  {
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    BootstrapTargetScnHttpResponseProcessor  processor =
        new BootstrapTargetScnHttpResponseProcessor(null, queue, stateMsg, cp,
                                                    remoteExHandler, null);
    ChannelBuffer buf = getScnResponse();
    HttpChunk httpChunk = new DefaultHttpChunk(buf);
    HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

    processor.startResponse(httpResponse);
    processor.addChunk(httpChunk);
    processor.addTrailer(httpChunkTrailer);
    processor.finishResponse();
    processor.channelException(new Exception("dummy exception"));

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_SUCCESS, gotMsg._state);
    Assert.assertEquals("StartSCN Response Id Check",new Long(5678912),cp.getBootstrapTargetScn());
  }

  @Test
  public void testTargetSCNExceptionChannelClose() throws Exception
  {
    // Both Exception and ChannelClose getting triggered
    TestAbstractQueue queue = new TestAbstractQueue();
    TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
    TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
    BootstrapTargetScnHttpResponseProcessor  processor =
        new BootstrapTargetScnHttpResponseProcessor(null, queue, stateMsg, cp,
                                                    remoteExHandler, null);
    processor.channelException(new Exception("dummy exception"));

    Assert.assertEquals("Error Handled", true, processor._errorHandled);
    Assert.assertEquals("Processor Response State", AbstractHttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
    Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
    Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
    TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
    Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_ERROR, gotMsg._state);
    Assert.assertEquals("No response",true, (null == stateMsg._cp));
  }

  public static class TestAbstractQueue
     implements ActorMessageQueue
  {
	private List<Object> _msgList = new ArrayList<Object>();

	@Override
	public void enqueueMessage(Object message) {
		_msgList.add(message);
	}

	public void clear()
	{
		_msgList.clear();
	}

	public List<Object> getMessages()
	{
		return _msgList;
	}
  }

  public static class TestConnectionStateMessage
  	implements DatabusRelayConnectionStateMessage, DatabusBootstrapConnectionStateMessage
  {
	  public enum State
	  {
		  STREAM_REQUEST_SENT,
		  STREAM_REQUEST_ERROR,
		  STREAM_RESPONSE_ERROR,
		  STREAM_RESPONSE_SUCCESS,
		  SOURCES_REQUEST_ERROR,
		  SOURCES_RESPONSE_ERROR,
		  SOURCES_REQUEST_SENT,
		  SOURCES_RESPONSE_SUCCESS,
		  SOURCES_SUCCESS,
		  REGISTER_REQUEST_ERROR,
		  REGISTER_RESPONSE_ERROR,
		  REGISTER_REQUEST_SENT,
		  REGISTER_RESPONSE_SUCCESS,
		  REGISTER_SUCCESS,
		  BOOTSTRAP_REQUESTED,
		  STARTSCN_REQUEST_ERROR,
		  STARTSCN_RESPONSE_ERROR,
		  STARTSCN_REQUEST_SENT,
		  STARTSCN_RESPONSE_SUCCESS,
		  TARGETSCN_REQUEST_ERROR,
		  TARGETSCN_RESPONSE_ERROR,
		  TARGETSCN_REQUEST_SENT,
		  TARGETSCN_RESPONSE_SUCCESS,
	  };

	  protected State _state;
	  protected ChunkedBodyReadableByteChannel _channel;
	  protected Map<Long, List<RegisterResponseEntry>> _registerResponse;
	  protected List<IdNamePair> _sourcesResponse;
	  protected Checkpoint _cp;
	  protected ServerInfo _currServer;
	  protected String _hostName, _svcName;

	@Override
	public void switchToStreamRequestError() {
		_state = State.STREAM_REQUEST_ERROR;
	}

	@Override
	public void switchToStreamResponseError() {
		_state = State.STREAM_RESPONSE_ERROR;
	}

	@Override
	public void switchToStreamSuccess(ChunkedBodyReadableByteChannel result)
	{
		_state = State.STREAM_RESPONSE_SUCCESS;
		_channel = result;
	}

	@Override
	public void switchToSourcesRequestError() {
		_state = State.SOURCES_REQUEST_ERROR;
	}

	@Override
	public void switchToSourcesRequestSent() {
		_state = State.SOURCES_REQUEST_SENT;
	}

	@Override
	public void switchToSourcesResponseError() {
		_state = State.SOURCES_RESPONSE_ERROR;
	}

	@Override
	public void switchToSourcesSuccess(List<IdNamePair> result, String hostName, String svcName) {
		_state = State.SOURCES_SUCCESS;
		_sourcesResponse = result;
		_hostName = hostName;
		_svcName = svcName;
	}

	@Override
	public void switchToRegisterRequestError() {
		_state = State.REGISTER_REQUEST_ERROR;
	}

	@Override
	public void swichToRegisterRequestSent() {
		_state = State.REGISTER_REQUEST_SENT;
	}

	@Override
	public void switchToRegisterResponseError() {
		_state = State.REGISTER_RESPONSE_ERROR;
	}

	@Override
	public void switchToRegisterSuccess(Map<Long, List<RegisterResponseEntry>> result,
	                                    Map<Long, List<RegisterResponseEntry>> keySchemasIgnoredForNow,
	                                    List<RegisterResponseMetadataEntry> metadataSchemasIgnoredForNow)
	{
		_state = State.REGISTER_SUCCESS;
		_registerResponse = result;
	}

	@Override
	public void switchToStreamRequestSent() {
		_state = State.STREAM_REQUEST_SENT;
	}

	@Override
	public void switchToBootstrapRequested() {
		_state = State.BOOTSTRAP_REQUESTED;
	}

	@Override
	public void switchToStartScnRequestError() {
		_state = State.STARTSCN_REQUEST_ERROR;
	}

	@Override
	public void switchToStartScnResponseError() {
		_state = State.STARTSCN_RESPONSE_ERROR;
	}

	@Override
	public void switchToStartScnSuccess(Checkpoint cp,
			DatabusBootstrapConnection bootstrapConnection,
			ServerInfo serverInfo) {
		_state = State.STARTSCN_RESPONSE_SUCCESS;
		_cp = cp;
		_currServer = serverInfo;
	}

	@Override
	public void switchToStartScnRequestSent() {
		_state = State.STARTSCN_REQUEST_SENT;
	}

	@Override
	public void switchToTargetScnRequestError() {
		_state = State.TARGETSCN_REQUEST_ERROR;
	}

	@Override
	public void switchToTargetScnResponseError() {
		_state = State.TARGETSCN_RESPONSE_ERROR;
	}

	@Override
	public void switchToTargetScnSuccess() {
		_state = State.TARGETSCN_RESPONSE_SUCCESS;
	}

	@Override
	public void switchToTargetScnRequestSent() {
		_state = State.TARGETSCN_REQUEST_SENT;
	}

	@Override
	public void switchToBootstrapDone() {
		// TODO Auto-generated method stub

	}

  public State getState()
  {
    return _state;
  }

  @Override
  public String toString()
  {
    return "TestConnectionStateMessage:" + getState();
  }
  }


  public static class TestRemoteExceptionHandler
     extends RemoteExceptionHandler
  {
	  public static enum ExceptionType
	  {
		  NO_EXCEPTION,
		  BOOTSTRAP_TOO_OLD_EXCEPTION,
		  OTHER_EXCEPTION
	  };

	  protected ExceptionType _exType = ExceptionType.NO_EXCEPTION;

	  public TestRemoteExceptionHandler()
	  {
		  super(null,null, new DbusEventV1Factory());
	  }

	  @Override
	  public Throwable getException(ChunkedBodyReadableByteChannel readChannel)
	  {
		  switch(_exType)
		  {
		  case NO_EXCEPTION: return null;
		  case BOOTSTRAP_TOO_OLD_EXCEPTION: return new BootstrapDatabaseTooOldException();
		  default: return new Exception();
		  }
	  }
  }

  private ChannelBuffer getRegisterResponse()
  {
	  String result =
		  "[{\"id\":202,\"version\":1}]";
	  byte[] b = result.getBytes(Charset.defaultCharset());
	  ByteBuffer buf = ByteBuffer.wrap(b);
	  ChannelBuffer c = HeapChannelBufferFactory.getInstance().getBuffer(buf);
	  return c;
  }

  private ChannelBuffer getSourcesResponse()
  {
	  String result =
		  "[{\"name\":\"com.linkedin.events.company.Companies\",\"id\":301}]";
	  byte[] b = result.getBytes(Charset.defaultCharset());
	  ByteBuffer buf = ByteBuffer.wrap(b);
	  ChannelBuffer c = HeapChannelBufferFactory.getInstance().getBuffer(buf);
	  return c;
  }

  private ChannelBuffer getScnResponse()
  {
	  String result =
		  "5678912";
	  byte[] b = result.getBytes(Charset.defaultCharset());
	  ByteBuffer buf = ByteBuffer.wrap(b);
	  ChannelBuffer c = HeapChannelBufferFactory.getInstance().getBuffer(buf);
	  return c;
  }
}
