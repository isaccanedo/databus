package com.linkedin.databus.client;
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
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;

import com.linkedin.databus.client.netty.HttpResponseProcessor;
import com.linkedin.databus2.core.container.DatabusHttpHeaders;


/**
 * Provides a {@link ReadableByteChannel} interface over a stream of incoming {@link HttpChunk}s.
 * The implementation allows for one thread to add chunks and one thread to read chunks. Incoming
 * chunks are buffered until the reader consumes them. The number buffered HTTP chunks and the
 * total size of buffered chunks is bounded.
 *
 * @author cbotev
 *
 */
public class ChunkedBodyReadableByteChannel implements ReadableByteChannel, HttpResponseProcessor
{
  public static final String MODULE = ChunkedBodyReadableByteChannel.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  /** Buffer chunks until they are consumed */

  //TODO MED make these configurable
  private static final int MAX_BUFFERED_CHUNKS = 1000;
  static final int MAX_BUFFERED_BYTES = 8192000;  // package-private for tests

  static final int MAX_CHUNK_SPACE_WAIT_MS = 15000;

  /** A flag if the channel is open */
  private AtomicBoolean _open = new AtomicBoolean(true);
  /** A flag that there are no more chunks to be read */
  private ChannelBuffer _currentBuffer;
  /** Saves the response object for access to the headers */
  private HttpResponse _response = null;
  /** Saves the trailer object for access to the footers */
  private HttpChunkTrailer _trailer = null;

  /** Protects the access to all chunk queue-related attributes */
  private final Lock _chunkQueueLock = new ReentrantLock(true);
  private final Condition _hasChunksCondition = _chunkQueueLock.newCondition();
  private final Condition _hasChunkSpaceCondition = _chunkQueueLock.newCondition();
  private final Queue<ChannelBuffer> _chunks;
  /** The number of currently buffered bytes */
  private int _totalBufferedBytes;
  /** A flag if we have reached the end of the stream */
  private volatile boolean _noMoreChunks;

  public ChunkedBodyReadableByteChannel()
  {
    _chunks = new ArrayDeque<ChannelBuffer>(MAX_BUFFERED_CHUNKS);
    _currentBuffer = null;
    _noMoreChunks = false;
    _totalBufferedBytes = 0;
  }

  @Override
  public void close() throws IOException
  {
    _open.set(false);
    _chunkQueueLock.lock();
    try
    {
      //awake anyone blocked waiting for chunks
      //getChunk() checks the _open flag and it will exit immediately.
      signalNoMoreChunks();
      _hasChunkSpaceCondition.signalAll();
    }
    finally
    {
      _chunkQueueLock.unlock();
    }
  }

  @Override
  public boolean isOpen()
  {
	return _open.get();
  }

  @Override
  public int read(ByteBuffer buffer) throws IOException
  {
    if (!_open.get()) return -1;

    int destRemaining = buffer.remaining();
    int saveRemaining = destRemaining;
    while (destRemaining > 0)
    {
      if (null == _currentBuffer)
      {
        if (!getChunk())
        {
          int bytesAlreadyWritten = saveRemaining - destRemaining;
          return 0 == bytesAlreadyWritten ? -1 : bytesAlreadyWritten;
        }
      }

      int bufferBytes = _currentBuffer.readableBytes();
      int saveLimit = -1;
      if (bufferBytes < destRemaining)
      {
        //netty does not like it if we try to read into a buffer that has more space than needed
        saveLimit = buffer.limit();
        buffer.limit(buffer.position() + bufferBytes);
      }
      _currentBuffer.readBytes(buffer);

      if (-1 != saveLimit) buffer.limit(saveLimit);
      if (0 == _currentBuffer.readableBytes())
      {
        _currentBuffer.resetReaderIndex();
        _chunkQueueLock.lock();
        try
        {
          _totalBufferedBytes -= _currentBuffer.readableBytes();
          _hasChunkSpaceCondition.signalAll();
        }
        finally
        {
          _chunkQueueLock.unlock();
        }
        _currentBuffer = null;
      }
      destRemaining = buffer.remaining();
    }

    return saveRemaining - destRemaining;
  }

  /**
   * Checks if there is enough space to buffer a chunk with the specified size. The semantics is to
   * enforce the upper bounds of max number of chunks and total size of chunks but also allow the
   * buffering of single chunks that go over the chunk size limit.
   *
   * This method must be called while holding {@link #_chunkQueueLock}.
   *
   * @param  newChunkSize               the size of the new chunk
   * @return true if the chunk can be buffered
   */
  private boolean checkIfEnoughSpace(int newChunkSize)
  {
    boolean result;
    if (newChunkSize >= MAX_BUFFERED_BYTES)
    {
      //allow for the buffering of a single chunk larger than the size threshold
      result = (_chunks.size() == 0);
    }
    else
    {
      result = _chunks.size() < MAX_BUFFERED_CHUNKS &&
               _totalBufferedBytes + newChunkSize <= MAX_BUFFERED_BYTES;
    }

    return result;
  }

  /**
   * Attempts to buffer bytes coming from the network.
   *
   * This method must be called while holding {@link #_chunkQueueLock}.
   *
   * @param buffer          the channel buffer with the bytes
   * @throws TimeoutException if the new bytes cannot be processed in MAX_CHUNK_SPACE_WAIT_MS time
   */
  private void addBytes(ChannelBuffer buffer) throws TimeoutException
  {
    if (_open.get())
    {
      _chunkQueueLock.lock();

      try
      {
        int contentSize = buffer.readableBytes();

        boolean canBuffer = checkIfEnoughSpace(contentSize);
        boolean doLoop = true;
        while (doLoop && ! canBuffer)
        {
          try
          {
            if (!_hasChunkSpaceCondition.await(MAX_CHUNK_SPACE_WAIT_MS, TimeUnit.MILLISECONDS))
            {
              throw new TimeoutException("waiting for chunk space");
            }
          }
          catch (InterruptedException ie)
          {
            LOG.info("interrupted");
            doLoop = false;
          }
          if (!_open.get()) return;
          canBuffer = checkIfEnoughSpace(contentSize);
        }

        if (canBuffer)
        {
          _chunks.add(buffer);
          _totalBufferedBytes += contentSize;
          if (_chunks.size() == 1) _hasChunksCondition.signalAll();
        }
      }
      finally
      {
        _chunkQueueLock.unlock();
      }
    }
  }

  @Override
  public void addChunk(HttpChunk chunk) throws TimeoutException
  {
    if (null == chunk)
    {
      LOG.error("unexpected null chunk");
    }
    else
    {
      addBytes(chunk.getContent());
    }
  }

  private boolean getChunk()
  {

    boolean result = false;

    _chunkQueueLock.lock();
    try
    {
      ChannelBuffer nextChunk = _chunks.poll();
      boolean doLoop = !_noMoreChunks;
      while (doLoop && null == nextChunk && !_noMoreChunks)
      {
        try
        {
          _hasChunksCondition.await();
        }
        catch (InterruptedException ie)
        {
          LOG.info("interrupted");
          doLoop = false;
        }
        if (!_open.get()) break;
        nextChunk = _chunks.poll();
      }

      if (null != nextChunk)
      {
        _currentBuffer = nextChunk;
        if (0 == _currentBuffer.readableBytes()) signalNoMoreChunksWithLock();
        if (0 == _chunks.size() || MAX_BUFFERED_CHUNKS - 1 == _chunks.size()) _hasChunkSpaceCondition.signalAll();
        result = true;
      }
    }
    finally
    {
      _chunkQueueLock.unlock();
    }

    return result;
  }

  @Override
  public void addTrailer(HttpChunkTrailer trailer) throws TimeoutException
  {
    _trailer = trailer;

    addChunk(trailer);
  }

  @Override
  public void finishResponse()
  {
    boolean debugEnabled = LOG.isDebugEnabled();

    if (debugEnabled)
    {
      String dbusReqLatencyStr = getMetadata(DatabusHttpHeaders.DATABUS_REQUEST_LATENCY_HEADER);
      if (null != dbusReqLatencyStr)
      {
        LOG.debug("Databus request processing latency (ms): " + dbusReqLatencyStr);
      }
    }

    signalNoMoreChunksWithLock();
  }

  @Override
  public void startResponse(HttpResponse response) throws TimeoutException
  {
    boolean debugEnabled = LOG.isDebugEnabled();

    if (debugEnabled)
    {
      String dbusReqId = response.getHeader(DatabusHttpHeaders.DATABUS_REQUEST_ID_HEADER);
      if (null != dbusReqId) LOG.debug("Received response for databus reqid: " + dbusReqId);
    }

    String contentLengthStr = response.getHeader(HttpHeaders.Names.CONTENT_LENGTH);

    _chunkQueueLock.lock();
    try
    {
      _response = response;
      _currentBuffer = null;
      if (null == contentLengthStr)
      {
        _noMoreChunks = false;
      }
      else
      {
        addBytes(response.getContent());
        signalNoMoreChunks();
      }
    }
    finally
    {
      _chunkQueueLock.unlock();
    }
  }

  /**
   * Returns the value of a header or a footer in the response. Footers have priority over headers,
   * i.e. if a header and a footer have the same name, the footer value will be returned.
   *
   * <p>Note that footer values will not be available until the entire content has been processed,
   * i.e. read() returns 0.
   * @param  key        the header/footer name
   * @return the meta-data value or null if the meta-data key does not exist
   */
  public String getMetadata(String key)
  {
    String result = null;

    if (null != _trailer)
    {
      result = _trailer.getHeader(key);
    }
    if (null == result && null != _response)
    {
      result = _response.getHeader(key);
    }

    return result;
  }

  @Override
  public void channelException(Throwable cause)
  {
    if (cause instanceof ClosedChannelException)
    {
      LOG.warn("channel unexpectedly closed.");
    }
    else if (cause instanceof RejectedExecutionException)
    {
      LOG.info("shutdown in progress");
    }
    else
    {
      LOG.error("channel exception(" + cause.getClass().getSimpleName() + "):" +
                cause.getMessage(), cause);
    }
    try
    {
      close();
    }
    catch (IOException ioe)
    {
      LOG.error("Error closing channel:" + ioe.getMessage(), ioe);
    }
  }

  private void signalNoMoreChunks()
  {
    _noMoreChunks = true;
    _hasChunksCondition.signalAll();
  }

  private void signalNoMoreChunksWithLock()
  {

    _chunkQueueLock.lock();
    try
    {
      signalNoMoreChunks();
    }
    finally
    {
      _chunkQueueLock.unlock();
    }
  }

  public boolean hasNoMoreChunks()
  {
    return _noMoreChunks;
  }

}
