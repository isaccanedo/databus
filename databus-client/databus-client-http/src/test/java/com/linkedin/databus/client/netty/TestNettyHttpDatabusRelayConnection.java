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


import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.local.LocalAddress;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpServerCodec;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLogLevel;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.CheckpointMult;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusEventV2Factory;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.OffsetNotFoundException;
import com.linkedin.databus.core.ScnNotFoundException;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.container.DatabusHttpHeaders;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.container.request.RegisterResponseMetadataEntry;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.test.container.SimpleObjectCaptureHandler;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;

public class TestNettyHttpDatabusRelayConnection
{
  static final String SOURCE1_SCHEMA_STR =
      "{\"name\":\"source1\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}";
  //  "{\"name\":\"source2_v1\",\"namespace\":\"test3\",\"type\":\"record\",\"fields\":[{\"type\":\"string\",\"name\":\"strField\"}]}";
  static final String KEY1_SCHEMA_STR =
      "{\"name\":\"key1\",\"type\":\"record\",\"fields\":[{\"name\":\"strField\",\"type\":\"string\"}]}";
  static final String METADATA1_SCHEMA_STR =
      "{\"name\":\"metadata\",\"namespace\":\"test_namespace\",\"type\":\"record\",\"fields\":[{\"name\":\"randomStrField\",\"type\":\"string\"}]}";

  static final ExecutorService BOSS_POOL = Executors.newCachedThreadPool();
  static final ExecutorService IO_POOL = Executors.newCachedThreadPool();
  static final int SERVER_ADDRESS_ID = 14466;
  static final LocalAddress SERVER_ADDRESS = new LocalAddress(SERVER_ADDRESS_ID);
  static final Timer NETWORK_TIMER = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);
  static final ChannelGroup TEST_CHANNELS_GROUP = new DefaultChannelGroup();
  static final long DEFAULT_READ_TIMEOUT_MS = 10000;
  static final long DEFAULT_WRITE_TIMEOUT_MS = 10000;
  static final String SOURCE1_NAME = "test.source1";
  static final ServerInfo RELAY_SERVER_INFO =
      new ServerInfo("testRelay", "master", new InetSocketAddress(SERVER_ADDRESS_ID), SOURCE1_NAME);
  static NettyHttpConnectionFactory CONN_FACTORY;
  static SimpleTestServerConnection _dummyServer;
  static DbusEventBuffer.StaticConfig _bufCfg;
  static List<Object> _sourceObjectsList;
  static List<Object> _keyObjectsList;
  static List<Object> _metadataObjectsList;
  static HashMap<String, List<Object>> _fullV4ResponseMap;
  static int MAX_EVENT_VERSION = DbusEventFactory.DBUS_EVENT_V2;
  private Level _logLevel = Level.INFO;

  @BeforeClass
  public void setUpClass() throws InvalidConfigException
  {
    TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestNettyHttpDatabusRelayConnection_" ,
                                             ".log" , Level.INFO);
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

    _dummyServer = new SimpleTestServerConnection(new DbusEventV2Factory().getByteOrder(),
                                                  SimpleTestServerConnection.ServerType.NIO);
    _dummyServer.setPipelineFactory(new ChannelPipelineFactory() {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(new LoggingHandler(InternalLogLevel.DEBUG),
                                     new HttpServerCodec(),
                                     new LoggingHandler(InternalLogLevel.DEBUG),
                                     new SimpleObjectCaptureHandler());
        }
    });
    _dummyServer.start(SERVER_ADDRESS_ID);

    DatabusHttpClientImpl.Config clientCfgBuilder = new DatabusHttpClientImpl.Config();
    clientCfgBuilder.getContainer().setReadTimeoutMs(DEFAULT_READ_TIMEOUT_MS);
    clientCfgBuilder.getContainer().setWriteTimeoutMs(DEFAULT_WRITE_TIMEOUT_MS);

    CONN_FACTORY =
        new NettyHttpConnectionFactory(BOSS_POOL, IO_POOL, null, NETWORK_TIMER,
                                       clientCfgBuilder.getContainer().getWriteTimeoutMs(),
                                       clientCfgBuilder.getContainer().getReadTimeoutMs(),
                                       clientCfgBuilder.getContainer().getBstReadTimeoutMs(),
                                       4, // protocolVersion
                                       MAX_EVENT_VERSION,
                                       TEST_CHANNELS_GROUP);
    DbusEventBuffer.Config bufCfgBuilder = new DbusEventBuffer.Config();
    bufCfgBuilder.setAllocationPolicy(AllocationPolicy.HEAP_MEMORY.toString());
    bufCfgBuilder.setMaxSize(100000);
    bufCfgBuilder.setScnIndexSize(128);
    bufCfgBuilder.setAverageEventSize(1);

    _bufCfg = bufCfgBuilder.build();

    initSchemaObjectsLists();

    _fullV4ResponseMap = new HashMap<String, List<Object>>();
    _fullV4ResponseMap.put(RegisterResponseEntry.SOURCE_SCHEMAS_KEY, _sourceObjectsList);
    _fullV4ResponseMap.put(RegisterResponseEntry.KEY_SCHEMAS_KEY, _keyObjectsList);
    _fullV4ResponseMap.put(RegisterResponseMetadataEntry.METADATA_SCHEMAS_KEY, _metadataObjectsList);
  }

  @SuppressWarnings("unchecked")
  private static void initSchemaObjectsLists()
  {
    List<RegisterResponseEntry> sourceSchemasList = new ArrayList<RegisterResponseEntry>();
    sourceSchemasList.add(new RegisterResponseEntry(1L, (short)1, SOURCE1_SCHEMA_STR));
    _sourceObjectsList = (List<Object>)(List<?>)sourceSchemasList;

    List<RegisterResponseEntry> keySchemasList = new ArrayList<RegisterResponseEntry>();
    keySchemasList.add(new RegisterResponseEntry(1L, (short)7, KEY1_SCHEMA_STR));
    _keyObjectsList = (List<Object>)(List<?>)keySchemasList;

    List<RegisterResponseMetadataEntry> metadataSchemasList = new ArrayList<RegisterResponseMetadataEntry>();
    byte[] tbdCrc32 = new byte[DbusEvent.CRC32_DIGEST_LEN];
    final short schemaVersion = 5;
    metadataSchemasList.add(new RegisterResponseMetadataEntry(schemaVersion, METADATA1_SCHEMA_STR, tbdCrc32));
    _metadataObjectsList = (List<Object>)(List<?>)metadataSchemasList;
  }

  @AfterClass
  public void tearDownClass()
  {
    _dummyServer.stop();
    BOSS_POOL.shutdownNow();
    IO_POOL.shutdownNow();
  }

  @Test
  public void testHappyPath() throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testHappyPath");
    //NettyHttpDatabusRelayConnection.LOG.setLevel(Level.DEBUG);

    DbusEventBuffer buf = createSimpleBuffer();

    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    conn.getHandler().getLog().setLevel(Level.DEBUG);

    Assert.assertEquals(MAX_EVENT_VERSION, conn.getMaxEventVersion()); // verify the version - current DBUS_EVENT_V2
    try
    {
      runHappyPathIteration(log, buf, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerSourcesDisconnect() throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerSourcesDisconnect");
    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerSourcesDisconnectIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerRegisterDisconnect()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerRegisterDisconnect");
    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerRegisterDisconnectIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerRegisterReqDisconnect()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerRegisterReqDisconnect");
    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerRegisterReqDisconnectIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerStreamDisconnect()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerStreamDisconnect");

    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerStreamDisconnectIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerStreamReqDisconnect()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerStreamReqDisconnect");

    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerStreamReqDisconnectIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerPartialDisconnect()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerPartialDisconnect");

    DbusEventBuffer buf = createSimpleBuffer();
    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerPartialStreamIteration(log, buf, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }


  @Test
  public void testServerSourcesTimeout()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerSourcesTimeout");
    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerSourcesReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerRegisterTimeout()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerRegisterTimeout");
    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerRegisterReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerStreamTimeout()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerStreamTimeout");
    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerStreamReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerPartialTimeout()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerPartialResponse");

    DbusEventBuffer buf = createSimpleBuffer();
    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerPartialStreamTimeoutIteration(log, buf, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  /**
   * <p>Test scenario
   * <p>
   *
   * <ul>
   *   <li> relay disconnected on /sources
   *   <li> relay timed out on /sources
   *   <li> relay disconnected on /sources again
   *   <li> relay timed out on /register
   *   <li> relay disconnected on /register
   *   <li> relay timed out on /register again
   *   <li> relay disconnected on /stream
   *   <li> relay disconnected on /stream again
   *   <li> relay succeeded on /stream
   *   <li> relay succeeded on /stream
   *   <li> relay timed out on partial /stream
   *   <li> relay succeeded on /stream
   *   <li> relay timed out on /stream
   *   <li> relay succeeded on /stream
   * </ul>
   */
  @Test
  public void testServerFixedScenario()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerFixedScenario");

    DbusEventBuffer buf = createSimpleBuffer();
    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    conn.getHandler().getLog().setLevel(_logLevel);
    try
    {
      log.info("********* 1. relay disconnected on /sources ********");
      runServerSourcesDisconnectIteration(log, callback, remoteExceptionHandler, conn);
      conn.getHandler().reset();
      log.info("********* 2. relay timed out on /sources ********");
      runServerSourcesReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
      conn.getHandler().reset();
      log.info("********* 3. relay disconnected on /sources ********");
      runServerSourcesDisconnectIteration(log, callback, remoteExceptionHandler, conn);
      conn.getHandler().reset();
      log.info("********* 4. relay timed out on /register ********");
      runServerRegisterReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
      conn.getHandler().reset();
      log.info("********* 5. relay disconnected on /register ********");
      runServerRegisterDisconnectIteration(log, callback, remoteExceptionHandler, conn);
      conn.getHandler().reset();
      log.info("********* 6. relay timed out on /register ********");
      runServerRegisterReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
      conn.getHandler().reset();
      log.info("********* 7. relay disconnected on /stream ********");
      runServerStreamDisconnectIteration(log, callback, remoteExceptionHandler, conn);
      conn.getHandler().reset();
      log.info("********* 8. relay disconnected on /stream ********");
      runServerStreamDisconnectIteration(log, callback, remoteExceptionHandler, conn);
      conn.getHandler().reset();
      log.info("********* 9. relay success on /stream ********");
      runHappyPathIteration(log, buf, callback, remoteExceptionHandler, conn);
      log.info("********* 10. relay success on /stream ********");
      runHappyPathIteration(log, buf, callback, remoteExceptionHandler, conn);
      log.info("********* 11. relay timed out on partial /stream ********");
      runServerPartialStreamTimeoutIteration(log, buf, callback, remoteExceptionHandler, conn);
      conn.getHandler().reset();
      log.info("********* 12. relay success on /stream ********");
      runHappyPathIteration(log, buf, callback, remoteExceptionHandler, conn);
      log.info("********* 13. relay timed out on /stream ********");
      runServerStreamReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
      conn.getHandler().reset();
      log.info("********* 14. relay success on /stream ********");
      runHappyPathIteration(log, buf, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  /**
   * <p>Random sequence of test iterations
   */
  @Test
  public void testServerRandomScenario()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerRandomScenario");
    log.info("in");

    DbusEventBuffer buf = createSimpleBuffer();
    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testServerRandomScenario");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    conn.getHandler().getLog().setLevel(_logLevel);
    try
    {
      final int iterNum = 15;
      Random rng = new Random();
      for (int i = 1; i<= iterNum; ++i )
      {
        int testId = rng.nextInt(10);
        switch (testId)
        {
        case 0:
        {
          log.info("======> step " + i + ": runHappyPathIteration");
          runHappyPathIteration(log, buf, callback, remoteExceptionHandler, conn);
          break;
        }
        case 1:
        {
          log.info("======> step " + i + ": runServerSourcesDisconnectIteration");
          runServerSourcesDisconnectIteration(log, callback, remoteExceptionHandler, conn);
          conn.getHandler().reset();
          break;
        }
        case 2:
        {
          log.info("======> step " + i + ": runServerSourcesReadTimeoutIteration");
          runServerSourcesReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
          conn.getHandler().reset();
          break;
        }
        case 3:
        {
          log.info("======> step " + i + ": runServerRegisterDisconnectIteration");
          runServerRegisterDisconnectIteration(log, callback, remoteExceptionHandler, conn);
          conn.getHandler().reset();
          break;
        }
        case 4:
        {
          log.info("======> step " + i + ": runServerRegisterReqDisconnectIteration");
          runServerRegisterReqDisconnectIteration(log, callback, remoteExceptionHandler, conn);
          conn.getHandler().reset();
          break;
        }
        case 5:
        {
          log.info("======> step " + i + ": runServerRegisterReadTimeoutIteration");
          runServerRegisterReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
          conn.getHandler().reset();
          break;
        }
        case 6:
        {
          log.info("======> step " + i + ": runServerStreamDisconnectIteration");
          runServerStreamDisconnectIteration(log, callback, remoteExceptionHandler, conn);
          conn.getHandler().reset();
          break;
        }
        case 7:
        {
          log.info("======> step " + i + ": runServerStreamReqDisconnectIteration");
          runServerStreamReqDisconnectIteration(log, callback, remoteExceptionHandler, conn);
          conn.getHandler().reset();
          break;
        }
        case 8:
        {
          log.info("======> step " + i + ": runServerStreamReadTimeoutIteration");
          runServerStreamReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
          conn.getHandler().reset();
          break;
        }
        case 9:
        {
          log.info("======> step " + i + ": runServerPartialStreamTimeoutIteration");
          runServerPartialStreamTimeoutIteration(log, buf, callback, remoteExceptionHandler, conn);
          conn.getHandler().reset();
          break;
        }
        default:
        {
          Assert.fail("step " + i + ": unknown test id: " + testId);
        }
        log.info("======> step " + i + ": complete.");
        if (0 != testId)
        {
          //if it was an iteration with an error, sleep a bit until we make sure the client
          //channel has been disconnected
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return !conn._channel.isConnected();
            }
          }, "wait for child channel closure ", 1024, log);
        }
        }
      }
    }
    finally
    {
      conn.close();
      callback.shutdown();
      log.info("out");
    }
  }

  @Test
  public void testHttpRequestLoggingHandlerRequest() throws Exception
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testHttpRequestLoggingHandlerRequest");

    DbusEventBuffer buf = createSimpleBuffer();

    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testHttpRequestLoggingHandlerRequest");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runHappyPathIteration(log, buf, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  /**
   * Tests "normal" client-relay protocol v4 happy path:  v4 request (hardcoded in
   * NettyHttpDatabusRelayConnection) leads to v4 response with all three schema types.
   */
  @Test
  public void testRegisterV4HappyPath_FullV4Response()
  throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    String responseStr = NettyTestUtils.generateRegisterResponseV4(_fullV4ResponseMap);
    runRegisterV4("testRegisterV4HappyPath_FullV4Response",
                  "4",
                  responseStr,
                  TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_SUCCESS);
  }

  /**
   * Tests client-relay protocol v4 happy path in which the relay omits the (optional)
   * key schemas.
   */
  @Test
  public void testRegisterV4HappyPath_V4ResponseNoKeySchemas()
  throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    HashMap<String, List<Object>> entries = new HashMap<String, List<Object>>();
    entries.put(RegisterResponseEntry.SOURCE_SCHEMAS_KEY, _sourceObjectsList);
    entries.put(RegisterResponseMetadataEntry.METADATA_SCHEMAS_KEY, _metadataObjectsList);

    String responseStr = NettyTestUtils.generateRegisterResponseV4(entries);

    runRegisterV4("testRegisterV4HappyPath_V4ResponseNoKeySchemas",
                  "4",
                  responseStr,
                  TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_SUCCESS);
  }

  /**
   * Tests client-relay protocol v4 happy path in which the relay omits the (optional)
   * metadata schema(s).
   */
  @Test
  public void testRegisterV4HappyPath_V4ResponseNoMetadataSchemas()
  throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    HashMap<String, List<Object>> entries = new HashMap<String, List<Object>>();
    entries.put(RegisterResponseEntry.SOURCE_SCHEMAS_KEY, _sourceObjectsList);
    entries.put(RegisterResponseEntry.KEY_SCHEMAS_KEY, _keyObjectsList);

    String responseStr = NettyTestUtils.generateRegisterResponseV4(entries);

    runRegisterV4("testRegisterV4HappyPath_V4ResponseNoMetadataSchemas",
                  "4",
                  responseStr,
                  TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_SUCCESS);
  }

  /**
   * Tests variant of client-relay protocol v4 happy path:  v4 request (hardcoded in
   * NettyHttpDatabusRelayConnection) to old relay leads to v3 response with list of
   * RegisterResponseEntry.
   */
  @Test
  public void testRegisterV4HappyPath_V3Response()
  throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    String responseStr =
        NettyTestUtils.generateRegisterResponse(new RegisterResponseEntry(1L, (short)1, SOURCE1_SCHEMA_STR));
    runRegisterV4("testRegisterV4HappyPath_V3Response",
                  "3",
                  responseStr,
                  TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_SUCCESS);
  }

  /**
   * Tests variant of client-relay protocol v4 happy path:  v4 request (hardcoded in
   * NettyHttpDatabusRelayConnection) to old relay leads to v2 response with list of
   * RegisterResponseEntry.
   */
  @Test
  public void testRegisterV4HappyPath_V2Response()
  throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    String responseStr =
        NettyTestUtils.generateRegisterResponse(new RegisterResponseEntry(1L, (short)1, SOURCE1_SCHEMA_STR));
    runRegisterV4("testRegisterV4HappyPath_V2Response",
                  "2",
                  responseStr,
                  TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_SUCCESS);
  }

  /**
   * Tests client-relay protocol v4 unhappy path:  relay sends back a non-numeric
   * protocol-version header.
   */
  @Test
  public void testRegisterV4UnhappyPath_NonNumericProtocolVersionHeader()
  throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    String responseStr = NettyTestUtils.generateRegisterResponseV4(_fullV4ResponseMap);
    runRegisterV4("testRegisterV4UnhappyPath_NonNumericProtocolVersionHeader",
                  "xyzzy",
                  responseStr,
                  TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR);
  }

  /**
   * Tests client-relay protocol v4 unhappy path:  relay sends back a protocol-version
   * header specifying version 1 (which doesn't exist in this part of the codebase).
   */
  @Test
  public void testRegisterV4UnhappyPath_InvalidV1Response()
  throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    String responseStr = NettyTestUtils.generateRegisterResponseV4(_fullV4ResponseMap);
    runRegisterV4("testRegisterV4UnhappyPath_InvalidV1Response",
                  "1",
                  responseStr,
                  TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR);
  }

  /**
   * Tests client-relay protocol v4 unhappy path:  relay sends back a protocol-version
   * header specifying version 7 (which doesn't yet exist).
   */
  @Test
  public void testRegisterV4UnhappyPath_InvalidV7Response()
  throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    String responseStr = NettyTestUtils.generateRegisterResponseV4(_fullV4ResponseMap);
    runRegisterV4("testRegisterV4UnhappyPath_InvalidV7Response",
                  "7",
                  responseStr,
                  TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR);
  }

  /**
   * Tests client-relay protocol v4 unhappy path:  v4 request (hardcoded in
   * NettyHttpDatabusRelayConnection) leads to claimed v3 response, but not
   * with expected list of RegisterResponseEntry.
   */
  @Test
  public void testRegisterV4UnhappyPath_V3ResponseNotList()
  throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    String responseStr = NettyTestUtils.generateRegisterResponseV4(_fullV4ResponseMap);  // wrong type for v3
    runRegisterV4("testRegisterV4UnhappyPath_V3ResponseNotList",
                  "3",
                  responseStr,
                  TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR);
  }

  /**
   * Tests client-relay protocol v4 unhappy path:  v4 request (hardcoded in
   * NettyHttpDatabusRelayConnection) leads to claimed v4 response, but not
   * with expected map of strings to lists of objects.
   */
  @Test
  public void testRegisterV4UnhappyPath_V4ResponseNotMap()
  throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    // this generates wrong type for v4:
    String responseStr =
        NettyTestUtils.generateRegisterResponse(new RegisterResponseEntry(1L, (short)1, SOURCE1_SCHEMA_STR));
    runRegisterV4("testRegisterV4UnhappyPath_V4ResponseNotMap",
                  "4",
                  responseStr,
                  TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR);
  }

  /**
   * Tests client-relay protocol v4 unhappy path:  v4 response that doesn't
   * include mandatory source schemas.
   */
  @Test
  public void testRegisterV4UnhappyPath_V4ResponseNoSourceSchemas()
  throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    HashMap<String, List<Object>> entries = new HashMap<String, List<Object>>();
    entries.put(RegisterResponseEntry.KEY_SCHEMAS_KEY, _keyObjectsList);
    entries.put(RegisterResponseMetadataEntry.METADATA_SCHEMAS_KEY, _metadataObjectsList);

    String responseStr = NettyTestUtils.generateRegisterResponseV4(entries);

    runRegisterV4("testRegisterV4UnhappyPath_V4ResponseNoSourceSchemas",
                  "4",
                  responseStr,
                  TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR);
  }

  /**
   * Tests client-relay protocol v4 unhappy path:  v4 response (map) includes
   * source-schemas key but with wrong value type.
   */
  @Test
  public void testRegisterV4UnhappyPath_V4ResponseSourceSchemasWrongListType()
  throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    List<String> sourceSchemasList = new ArrayList<String>();
    sourceSchemasList.add("foobly");
    @SuppressWarnings("unchecked")
    List<Object> sourceObjectsList = (List<Object>)(List<?>)sourceSchemasList;

    HashMap<String, List<Object>> entries = new HashMap<String, List<Object>>();
    entries.put(RegisterResponseEntry.SOURCE_SCHEMAS_KEY, sourceObjectsList);
    entries.put(RegisterResponseEntry.KEY_SCHEMAS_KEY, _keyObjectsList);
    entries.put(RegisterResponseMetadataEntry.METADATA_SCHEMAS_KEY, _metadataObjectsList);

    String responseStr = NettyTestUtils.generateRegisterResponseV4(entries);

    runRegisterV4("testRegisterV4UnhappyPath_V4ResponseSourceSchemasWrongListType",
                  "4",
                  responseStr,
                  TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR);
  }

  /**
   * Tests client-relay protocol v4 unhappy path:  v4 response (map) includes
   * source-schemas key with correct value type (list of RRE), but list is empty.
   */
  @Test
  public void testRegisterV4UnhappyPath_V4ResponseSourceSchemasEmptyList()
  throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    List<RegisterResponseEntry> sourceSchemasList = new ArrayList<RegisterResponseEntry>();
    @SuppressWarnings("unchecked")
    List<Object> sourceObjectsList = (List<Object>)(List<?>)sourceSchemasList;

    HashMap<String, List<Object>> entries = new HashMap<String, List<Object>>();
    entries.put(RegisterResponseEntry.SOURCE_SCHEMAS_KEY, sourceObjectsList);
    entries.put(RegisterResponseEntry.KEY_SCHEMAS_KEY, _keyObjectsList);
    entries.put(RegisterResponseMetadataEntry.METADATA_SCHEMAS_KEY, _metadataObjectsList);

    String responseStr = NettyTestUtils.generateRegisterResponseV4(entries);

    runRegisterV4("testRegisterV4UnhappyPath_V4ResponseSourceSchemasEmptyList",
                  "4",
                  responseStr,
                  TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR);
  }

  private void runRegisterV4(final String subtestName,
                             final String protocolVersionHeader,
                             final String responseStr,
                             final TestResponseProcessors.TestConnectionStateMessage.State expectedRegisterState)
  throws IOException, ScnNotFoundException, OffsetNotFoundException
  //throws JsonGenerationException, JsonMappingException, IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection." + subtestName);
    //log.setLevel(Level.DEBUG);
    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart(subtestName);

    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);

    Assert.assertEquals(MAX_EVENT_VERSION, conn.getMaxEventVersion()); // verify the version - current DBUS_EVENT_V1
    try
    {
      // connect to server and send /sources
      TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
      conn.requestSources(msg);
      waitForServerConnection(conn, log);

      // introspect connection to server
      Channel channel = conn._channel;
      final SocketAddress clientAddr = channel.getLocalAddress();
      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check()
        {
          return null != _dummyServer.getChildChannel(clientAddr);
        }
      }, "client connection established", 1000, log);

      Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
      ChannelPipeline serverPipeline = serverChannel.getPipeline();
      SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

      // verify server gets the /sources request
      HttpResponse sourcesResp = runHappyPathSources(log,
                                                     callback,
                                                     remoteExceptionHandler,
                                                     clientAddr,
                                                     objCapture);

      // send /register and check result
      doRegisterV4(log,
                   callback,
                   remoteExceptionHandler,
                   conn,
                   msg,
                   clientAddr,
                   objCapture,
                   sourcesResp,
                   protocolVersionHeader,
                   responseStr,
                   expectedRegisterState);

      callback.clearLastMsg();
      objCapture.clear();
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  private void doRegisterV4(final Logger log,
                            TestingConnectionCallback callback,
                            DummyRemoteExceptionHandler remoteExceptionHandler,
                            final NettyHttpDatabusRelayConnection conn,
                            TestResponseProcessors.TestConnectionStateMessage msg,
                            SocketAddress clientAddr,
                            SimpleObjectCaptureHandler objCapture,
                            HttpResponse sourcesResp,
                            final String protocolVersionHeader,
                            final String responseStr,
                            final TestResponseProcessors.TestConnectionStateMessage.State expectedRegisterState)
  throws JsonGenerationException, JsonMappingException, IOException
  {
    HttpRequest msgReq;
    HttpChunk body;
    objCapture.clear();
    conn.requestRegister("1", msg);  // calls createRegisterUrl(), which appends protocolVersion=4

    // verify server gets the /register request
    msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/register"));
    Assert.assertTrue(msgReq.getUri().indexOf(DatabusHttpHeaders.PROTOCOL_VERSION_PARAM + "=4") >= 0);

    sourcesResp.setHeader(DatabusHttpHeaders.DBUS_CLIENT_RELAY_PROTOCOL_VERSION_HDR, protocolVersionHeader);

    body = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer(responseStr.getBytes(Charset.defaultCharset())));
    NettyTestUtils.sendServerResponses(_dummyServer, clientAddr, sourcesResp, body);

    waitForCallback(callback, expectedRegisterState, log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
  }

  private void runServerSourcesReadTimeoutIteration(final Logger log,
                                                   TestingConnectionCallback callback,
                                                   DummyRemoteExceptionHandler remoteExceptionHandler,
                                                   final NettyHttpDatabusRelayConnection conn)
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    Assert.assertTrue(objCapture.waitForMessage(1000, 0));
    Object msgObj = objCapture.getMessages().get(0);
    Assert.assertTrue(msgObj instanceof HttpRequest);

    HttpRequest msgReq = (HttpRequest)msgObj;
    Assert.assertTrue(msgReq.getUri().startsWith("/sources"));  // now has "?protocolVersion=X" appended

    //Trigger a read timeout
    TestUtil.sleep(DEFAULT_READ_TIMEOUT_MS + 100);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.SOURCES_RESPONSE_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());
    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerRegisterReadTimeoutIteration(final Logger log,
                                     TestingConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    runHappyPathSources(log,
                            callback,
                            remoteExceptionHandler,
                            clientAddr,
                            objCapture);


    conn.requestRegister("1", msg);

    //verify server gets the /register request
    HttpRequest msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/register"));

    //Trigger a read timeout
    TestUtil.sleep(DEFAULT_READ_TIMEOUT_MS + 100);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());
    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerStreamReadTimeoutIteration(final Logger log,
                                     TestingConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    HttpResponse sourcesResp =
        runHappyPathSources(log,
                                callback,
                                remoteExceptionHandler,
                                clientAddr,
                                objCapture);

    //send /register
    runHappyPathRegister(log,
                         callback,
                         remoteExceptionHandler,
                         conn,
                         msg,
                         clientAddr,
                         objCapture,
                         sourcesResp);

    //send partial /stream
    callback.clearLastMsg();
    objCapture.clear();
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    CheckpointMult cpm = new CheckpointMult();
    cpm.addCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION, cp);
    conn.requestStream("1", null, 1000, cpm, null, msg);


    //////// verify server gets the /stream request
    HttpRequest msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/stream"));

    //Trigger a read timeout
    TestUtil.sleep(DEFAULT_READ_TIMEOUT_MS + 100);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.STREAM_RESPONSE_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());

    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerPartialStreamTimeoutIteration(final Logger log,
                                     DbusEventBuffer buf,
                                     TestingConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    HttpResponse sourcesResp =
        runHappyPathSources(log,
                                callback,
                                remoteExceptionHandler,
                                clientAddr,
                                objCapture);

    //send /register
    runHappyPathRegister(log,
                         callback,
                         remoteExceptionHandler,
                         conn,
                         msg,
                         clientAddr,
                         objCapture,
                         sourcesResp);

    //send partial /stream
    callback.clearLastMsg();
    objCapture.clear();
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    CheckpointMult cpm = new CheckpointMult();
    cpm.addCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION, cp);
    conn.requestStream("1", null, 1000, cpm, null, msg);


    //////// verify server gets the /stream request
    HttpRequest msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/stream"));

    ////// send back some partial response
    ChannelBuffer tmpBuf = NettyTestUtils.streamToChannelBuffer(buf, cp, 10000, null);
    _dummyServer.sendServerResponse(clientAddr, sourcesResp, 1000);
    _dummyServer.sendServerResponse(clientAddr, new DefaultHttpChunk(tmpBuf), 1000);

    //Trigger a read timeout
    TestUtil.sleep(DEFAULT_READ_TIMEOUT_MS + 100);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());

    callback.clearLastMsg();
    objCapture.clear();
}

  private void runServerSourcesDisconnectIteration(final Logger log,
                                                   TestingConnectionCallback callback,
                                                   DummyRemoteExceptionHandler remoteExceptionHandler,
                                                   final NettyHttpDatabusRelayConnection conn)
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    Assert.assertTrue(objCapture.waitForMessage(1000, 0));
    Object msgObj = objCapture.getMessages().get(0);
    Assert.assertTrue(msgObj instanceof HttpRequest);

    HttpRequest msgReq = (HttpRequest)msgObj;
    Assert.assertTrue(msgReq.getUri().startsWith("/sources"));  // now has "?protocolVersion=X" appended

    serverChannel.close();

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.SOURCES_RESPONSE_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());
    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerRegisterDisconnectIteration(final Logger log,
                                     TestingConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();
    final SocketAddress finalClientAddr = clientAddr;

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check()
      {
        return _dummyServer.getChildChannel(finalClientAddr) != null;
      }
    }, "client connected", 100, log);

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    runHappyPathSources(log,
                            callback,
                            remoteExceptionHandler,
                            clientAddr,
                            objCapture);


    callback.clearLastMsg();
    objCapture.clear();
    conn.requestRegister("1", msg);

    //verify server gets the /register request
    HttpRequest msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/register"));

    serverChannel.close();

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());
    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerRegisterReqDisconnectIteration(final Logger log,
                                     TestingConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    runHappyPathSources(log,
                            callback,
                            remoteExceptionHandler,
                            clientAddr,
                            objCapture);

    callback.clearLastMsg();
    objCapture.clear();

    serverChannel.close();

    conn.requestRegister("1", msg);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_REQUEST_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());
    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerStreamDisconnectIteration(final Logger log,
                                     TestingConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    HttpResponse sourcesResp =
        runHappyPathSources(log,
                                callback,
                                remoteExceptionHandler,
                                clientAddr,
                                objCapture);

    //send /register
    runHappyPathRegister(log,
                         callback,
                         remoteExceptionHandler,
                         conn,
                         msg,
                         clientAddr,
                         objCapture,
                         sourcesResp);

    //send partial /stream
    callback.clearLastMsg();
    objCapture.clear();
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    CheckpointMult cpm = new CheckpointMult();
    cpm.addCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION, cp);
    conn.requestStream("1", null, 1000, cpm, null, msg);


    //////// verify server gets the /stream request
    HttpRequest msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/stream"));

    serverChannel.close();

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.STREAM_RESPONSE_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());

    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerStreamReqDisconnectIteration(final Logger log,
                                     TestingConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    HttpResponse sourcesResp =
        runHappyPathSources(log,
                                callback,
                                remoteExceptionHandler,
                                clientAddr,
                                objCapture);

    //send /register
    runHappyPathRegister(log,
                         callback,
                         remoteExceptionHandler,
                         conn,
                         msg,
                         clientAddr,
                         objCapture,
                         sourcesResp);

    //send partial /stream
    callback.clearLastMsg();
    objCapture.clear();

    serverChannel.close();

    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    CheckpointMult cpm = new CheckpointMult();
    cpm.addCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION, cp);
    conn.requestStream("1", null, 1000, cpm, null, msg);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.STREAM_REQUEST_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());

    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerPartialStreamIteration(final Logger log,
                                     DbusEventBuffer buf,
                                     TestingConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    HttpResponse sourcesResp =
        runHappyPathSources(log,
                                callback,
                                remoteExceptionHandler,
                                clientAddr,
                                objCapture);

    //send /register
    runHappyPathRegister(log,
                         callback,
                         remoteExceptionHandler,
                         conn,
                         msg,
                         clientAddr,
                         objCapture,
                         sourcesResp);

    //send partial /stream
    callback.clearLastMsg();
    objCapture.clear();
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    CheckpointMult cpm = new CheckpointMult();
    cpm.addCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION, cp);
    conn.requestStream("1", null, 1000, cpm, null, msg);


    //////// verify server gets the /stream request
    HttpRequest msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/stream"));

    ////// send back some partial response
    ChannelBuffer tmpBuf = NettyTestUtils.streamToChannelBuffer(buf, cp, 10000, null);
    _dummyServer.sendServerResponse(clientAddr, sourcesResp, 1000);
    _dummyServer.sendServerResponse(clientAddr, new DefaultHttpChunk(tmpBuf), 1000);

    serverChannel.close();

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());

    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runHappyPathIteration(final Logger log,
                                     DbusEventBuffer buf,
                                     TestingConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    final SocketAddress clientAddr = channel.getLocalAddress();

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check()
      {
        return null != _dummyServer.getChildChannel(clientAddr);
      }
    }, "client connection established", 1000, log);

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    HttpResponse sourcesResp =
        runHappyPathSources(log,
                            callback,
                            remoteExceptionHandler,
                            clientAddr,
                            objCapture);

    //send /register
    runHappyPathRegister(log,
                         callback,
                         remoteExceptionHandler,
                         conn,
                         msg,
                         clientAddr,
                         objCapture,
                         sourcesResp);

    //send /stream
    runHappyPathStream(log,
                       buf,
                       callback,
                       remoteExceptionHandler,
                       conn,
                       msg,
                       clientAddr,
                       objCapture,
                       sourcesResp);

    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runHappyPathStream(final Logger log,
                                  DbusEventBuffer buf,
                                  TestingConnectionCallback callback,
                                  DummyRemoteExceptionHandler remoteExceptionHandler,
                                  final NettyHttpDatabusRelayConnection conn,
                                  TestResponseProcessors.TestConnectionStateMessage msg,
                                  SocketAddress clientAddr,
                                  SimpleObjectCaptureHandler objCapture,
                                  HttpResponse sourcesResp) throws ScnNotFoundException,
      OffsetNotFoundException,
      IOException
  {
    HttpRequest msgReq;
    objCapture.clear();
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    CheckpointMult cpm = new CheckpointMult();
    cpm.addCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION, cp);
    conn.requestStream("1", null, 1000, cpm, null, msg);


    //verify server gets the /stream request
    msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/stream"));

    // verify url construction for adding max event version
    String expectedVersion = "&" + DatabusHttpHeaders.MAX_EVENT_VERSION + "=" + MAX_EVENT_VERSION;
    Assert.assertTrue(msgReq.getUri().contains(expectedVersion));

    //send back some response
    ChannelBuffer tmpBuf = NettyTestUtils.streamToChannelBuffer(buf, cp, 10000, null);
    NettyTestUtils.sendServerResponses(_dummyServer, clientAddr, sourcesResp, new DefaultHttpChunk(tmpBuf));

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());

    //wait for the readable byte channel to process the response and verify nothing has changed
    TestUtil.sleep(1000);
    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
  }

  private void runHappyPathRegister(final Logger log,
                                    TestingConnectionCallback callback,
                                    DummyRemoteExceptionHandler remoteExceptionHandler,
                                    final NettyHttpDatabusRelayConnection conn,
                                    TestResponseProcessors.TestConnectionStateMessage msg,
                                    SocketAddress clientAddr,
                                    SimpleObjectCaptureHandler objCapture,
                                    HttpResponse sourcesResp) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    HttpRequest msgReq;
    HttpChunk body;
    objCapture.clear();
    conn.requestRegister("1", msg);

    //verify server gets the /register request
    msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/register"));

    //send back some response
    RegisterResponseEntry entry = new RegisterResponseEntry(1L, (short)1, SOURCE1_SCHEMA_STR);
    String responseStr = NettyTestUtils.generateRegisterResponse(entry);
    body = new DefaultHttpChunk(
        ChannelBuffers.wrappedBuffer(responseStr.getBytes(Charset.defaultCharset())));
    NettyTestUtils.sendServerResponses(_dummyServer, clientAddr, sourcesResp, body);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_SUCCESS,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
  }

  private HttpResponse runHappyPathSources(final Logger log,
                                           TestingConnectionCallback callback,
                                           DummyRemoteExceptionHandler remoteExceptionHandler,
                                           SocketAddress clientAddr,
                                           SimpleObjectCaptureHandler objCapture)
  {
    Assert.assertTrue(objCapture.waitForMessage(1000, 0));
    Object msgObj = objCapture.getMessages().get(0);
    Assert.assertTrue(msgObj instanceof HttpRequest);

    HttpRequest msgReq = (HttpRequest)msgObj;
    Assert.assertTrue(msgReq.getUri().startsWith("/sources"));  // now has "?protocolVersion=X" appended

    callback.clearLastMsg();
    objCapture.clear();

    //send back some response
    HttpResponse sourcesResp = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                                                       HttpResponseStatus.OK);
    sourcesResp.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    sourcesResp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
    HttpChunk body =
        new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("[{\"id\":1,\"name\":\"test.source1\"}]".getBytes(Charset.defaultCharset())));
    NettyTestUtils.sendServerResponses(_dummyServer, clientAddr, sourcesResp, body);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.SOURCES_SUCCESS,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    callback.clearLastMsg();
    objCapture.clear();
    return sourcesResp;
  }

  private HttpRequest captureRequest(SimpleObjectCaptureHandler objCapture)
  {
    Object msgObj;
    Assert.assertTrue(objCapture.waitForMessage(1000, 0));
    msgObj = objCapture.getMessages().get(0);
    Assert.assertTrue(msgObj instanceof HttpRequest);

    HttpRequest msgReq = (HttpRequest)msgObj;
    return msgReq;
  }

  private DbusEventBuffer createSimpleBuffer()
  {
    DbusEventBuffer buf  = new DbusEventBuffer(_bufCfg);
    buf.start(0);
    buf.startEvents();
    buf.appendEvent(new DbusEventKey(1), (short)1, (short)1, System.nanoTime(), (short)1,
                    new byte[16], new byte[100], false, null);
    buf.appendEvent(new DbusEventKey(2), (short)1, (short)1, System.nanoTime(), (short)1,
                    new byte[16], new byte[100], false, null);
    buf.endEvents(10);
    return buf;
  }

  static void waitForServerConnection(final NettyHttpDatabusRelayConnection conn, final Logger log)
  {
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check()
      {
        return null != conn._channel && conn._channel.isConnected();
      }
    }, "waiting to connect to server", 100000, log);
  }

  static void waitForCallback(final TestingConnectionCallback callback,
                       final TestResponseProcessors.TestConnectionStateMessage.State state,
                       final Logger log)
  {
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check()
      {
        TestResponseProcessors.TestConnectionStateMessage lastMsg = callback.getLastMsg();
        return null != lastMsg && lastMsg.getState().equals(state);
      }
    }, "waiting for state " + state, 1000, log);
  }

}


class DummyRemoteExceptionHandler extends RemoteExceptionHandler
{
  Throwable _lastException = null;

  public DummyRemoteExceptionHandler()
  {
    super(null, null, new DbusEventV2Factory());
  }

  @Override
  public void handleException(Throwable remoteException) throws InvalidEventException,
      InterruptedException
  {
    _lastException = remoteException;
  }

  public Throwable getLastException()
  {
    return _lastException;
  }

  public void resetLastException()
  {
    _lastException = null;
  }

}
