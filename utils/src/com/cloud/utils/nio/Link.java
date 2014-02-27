// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package com.cloud.utils.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

/**
 */
public class Link {
    private static final Logger s_logger = Logger.getLogger(Link.class);

    private final InetSocketAddress _addr;
    private final NioConnection _connection;
    private SelectionKey _key;
    private final ConcurrentLinkedQueue<ByteBuffer[]> _writeQueue;
    private ByteBuffer _readBuffer;
    private ByteBuffer _plaintextBuffer;
    private Object _attach;
    private boolean _readHeader;
    private boolean _gotFollowingPacket;

    public Link(InetSocketAddress addr, NioConnection connection) {
        _addr = addr;
        _connection = connection;
        _readBuffer = ByteBuffer.allocate(2048);
        _attach = null;
        _key = null;
        _writeQueue = new ConcurrentLinkedQueue<ByteBuffer[]>();
        _readHeader = true;
        _gotFollowingPacket = false;
    }

    public Link (Link link) {
        this(link._addr, link._connection);
    }

    public Object attachment() {
        return _attach;
    }

    public void attach(Object attach) {
        _attach = attach;
    }

    public void setKey(SelectionKey key) {
        _key = key;
    }

    /**
     * No user, so comment it out.
     * 
     * Static methods for reading from a channel in case
     * you need to add a client that doesn't require nio.
     * @param ch channel to read from.
     * @param bytebuffer to use.
     * @return bytes read
     * @throws IOException if not read to completion.
    public static byte[] read(SocketChannel ch, ByteBuffer buff) throws IOException {
    	synchronized(buff) {
	    	buff.clear();
	    	buff.limit(4);

	    	while (buff.hasRemaining()) {
		    	if (ch.read(buff) == -1) {
		    		throw new IOException("Connection closed with -1 on reading size.");
		    	}
	    	}

	    	buff.flip();

	    	int length = buff.getInt();
	    	ByteArrayOutputStream output = new ByteArrayOutputStream(length);
	    	WritableByteChannel outCh = Channels.newChannel(output);

	    	int count = 0;
	    	while (count < length) {
	        	buff.clear();
	    		int read = ch.read(buff);
	    		if (read < 0) {
	    			throw new IOException("Connection closed with -1 on reading data.");
	    		}
	    		count += read;
	    		buff.flip();
	    		outCh.write(buff);
	    	}

	        return output.toByteArray();
    	}
    }
     */

    private static void doWrite(SocketChannel ch, ByteBuffer[] buffers) throws IOException {
        ByteBuffer headBuf = ByteBuffer.allocate(4);

        int totalLen = 0;

        for (ByteBuffer buffer : buffers) {
            totalLen += buffer.limit();
        }

        headBuf.putInt(totalLen);
        headBuf.flip();

        int sent=0;
        while(sent < headBuf.limit()) {
            sent += ch.write(headBuf);
        };

        for (ByteBuffer buffer : buffers) {
            int remaining = buffer.limit();
            while(remaining > 0) {
                long count = ch.write(buffer);
                remaining -= count;
            }
        }
    }

    /**
     * write method to write to a socket.  This method writes to completion so
     * it doesn't follow the nio standard.  We use this to make sure we write
     * our own protocol.
     * 
     * @param ch channel to write to.
     * @param buffers buffers to write.
     * @throws IOException if unable to write to completion.
     */
    public static void write(SocketChannel ch, ByteBuffer[] buffers) throws IOException {
        synchronized (ch) {
            doWrite(ch, buffers);
        }
    }

    /* SSL has limitation of 16k, we may need to split packets. 18000 is 16k + some extra SSL informations */
    protected static final int      MAX_SIZE_PER_PACKET = 18000;
    protected static final int      HEADER_FLAG_FOLLOWING = 0x10000;

    public byte[] read(SocketChannel ch) throws IOException {
        if (_readHeader) {   // Start of a packet
            if (_readBuffer.position() == 0) {
                _readBuffer.limit(4);
            }

            if (ch.read(_readBuffer) == -1) {
                throw new IOException("Connection closed with -1 on reading size.");
            }

            if (_readBuffer.hasRemaining()) {
                s_logger.trace("Need to read the rest of the packet length");
                return null;
            }
            _readBuffer.flip();
            int header = _readBuffer.getInt();
            int readSize = (short)header;
            if (s_logger.isTraceEnabled()) {
                s_logger.trace("Packet length is " + readSize);
            }

            _readBuffer.clear();
            _readHeader = false;

            if (_readBuffer.capacity() < readSize) {
                if (s_logger.isTraceEnabled()) {
                    s_logger.trace("Resizing the byte buffer from " + _readBuffer.capacity());
                }
                _readBuffer = ByteBuffer.allocate(readSize);
            }
            _readBuffer.limit(readSize);
        }

        if (ch.read(_readBuffer) == -1) {
            throw new IOException("Connection closed with -1 on read.");
        }

        if (_readBuffer.hasRemaining()) {   // We're not done yet.
            if (s_logger.isTraceEnabled()) {
                s_logger.trace("Still has " + _readBuffer.remaining());
            }
            return null;
        }

        _readBuffer.flip();

        _readHeader = true;

        byte[] result = new byte[_readBuffer.limit()];
        _readBuffer.get(result);
        _readBuffer.clear();
        return result;
    }

    public void send(byte[] data) throws ClosedChannelException {
        send(data, false);
    }

    public void send(byte[] data, boolean close) throws ClosedChannelException {
        send(new ByteBuffer[] { ByteBuffer.wrap(data) }, close);
    }

    public void send(ByteBuffer[] data, boolean close) throws ClosedChannelException {
        ByteBuffer[] item = new ByteBuffer[data.length + 1];
        int remaining = 0;
        for (int i = 0; i < data.length; i++) {
            remaining += data[i].remaining();
            item[i + 1] = data[i];
        }

        item[0] = ByteBuffer.allocate(4);
        item[0].putInt(remaining);
        item[0].flip();

        if (s_logger.isTraceEnabled()) {
            s_logger.trace("Sending packet of length " + remaining);
        }

        _writeQueue.add(item);
        if  (close) {
            _writeQueue.add(new ByteBuffer[0]);
        }
        synchronized (this) {
            if (_key == null) {
                throw new ClosedChannelException();
            }
            _connection.change(SelectionKey.OP_WRITE, _key, null);
        }
    }

    public void send(ByteBuffer[] data) throws ClosedChannelException {
        send(data, false);
    }

    public synchronized void close() {
        if (_key != null) {
            _connection.close(_key);
        }
    }

    public boolean write(SocketChannel ch) throws IOException {
        ByteBuffer[] data = null;
        while ((data = _writeQueue.poll()) != null) {
            if (data.length == 0) {
                if (s_logger.isTraceEnabled()) {
                    s_logger.trace("Closing connection requested");
                }
                return true;
            }

            ByteBuffer[] raw_data = new ByteBuffer[data.length - 1];
            System.arraycopy(data, 1, raw_data, 0, data.length - 1);

            doWrite(ch, raw_data);
        }
        return false;
    }

    public InetSocketAddress getSocketAddress() {
        return _addr;
    }

    public String getIpAddress() {
        return _addr.getAddress().toString();
    }

    public synchronized void terminated() {
        _key = null;
    }

    public synchronized void schedule(Task task) throws ClosedChannelException {
        if (_key == null) {
            throw new ClosedChannelException();
        }
        _connection.scheduleTask(task);
    }
}
