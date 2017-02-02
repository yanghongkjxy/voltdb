/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltcore.network;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Deque;
import java.util.Properties;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.voltcore.network.TestNIOWriteStream.MockChannel;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.ssl.SSLConfiguration;

import org.voltdb.BackendTarget;
import org.voltdb.Inits;
import org.voltdb.client.TLSHandshaker;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;

import org.junit.After;
import org.junit.Test;

import io.netty_voltpatches.buffer.ByteBuf;
import io.netty_voltpatches.buffer.Unpooled;

public class TestTLSNIOWriteStream extends JUnit4LocalClusterTest {
    private SSLEngine m_sslEngine = null;
    private CipherExecutor m_ce = CipherExecutor.SERVER;
    private NetworkDBBPool m_pool = new NetworkDBBPool(64, 4);
    final String DEFAULT_SSL_PROPS_FILE = "ssl-config";
    SocketChannel m_socket;
    String m_host = "127.0.0.1";
    int m_clientPort = 21212;
    LocalCluster m_config = null;


    private class MockTLSPort extends TLSVoltPort {
        @Override
        public String toString() {
            return null;
        }

        public MockTLSPort (SSLEngine sslEngine, CipherExecutor service, NetworkDBBPool pool) throws UnknownHostException {
            super(null, null, new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 21212), pool, sslEngine, service);
        }

        @Override
        public void setInterests(int opsToAdd, int opsToRemove) {
            this.opsToAdd = opsToAdd;
            //super.setInterests(opsToAdd, opsToRemove);
        }

        @Override
        protected void enableWriteSelection() {
            setInterests(SelectionKey.OP_WRITE, 0);
        }

        public boolean checkWriteSet() {
            if (opsToAdd == SelectionKey.OP_WRITE) {
                opsToAdd = 0;
                return true;
            }
            return false;
        }

        int opsToAdd;
    }

    public void connect() throws IOException {
        try {
            InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(m_host), m_clientPort);
            m_socket = SocketChannel.open(address);
            if (!m_socket.isConnected()) {
                m_socket = null;
                throw new IOException("Could establish connection to remote " + m_host + ":" + m_clientPort);
            }
            m_socket.configureBlocking(false);
            m_socket.socket().setTcpNoDelay(true);
        } catch (IOException excp) {
            excp.printStackTrace();
            throw excp;
        }

        try {
            SSLConfiguration.SslConfig sslConfig;
            String sslPropsFile = Inits.class.getResource(DEFAULT_SSL_PROPS_FILE).getFile();

            if ((sslPropsFile == null || sslPropsFile.trim().length() == 0) ) {
                sslConfig = new SSLConfiguration.SslConfig(null, null, null, null);
                SSLConfiguration.applySystemProperties(sslConfig);
            } else {
                File configFile = new File(sslPropsFile);
                Properties sslProperties = new Properties();
                try ( FileInputStream configFis = new FileInputStream(configFile) ) {
                    sslProperties.load(configFis);
                    sslConfig = new SSLConfiguration.SslConfig(
                            sslProperties.getProperty(SSLConfiguration.KEYSTORE_CONFIG_PROP),
                            sslProperties.getProperty(SSLConfiguration.KEYSTORE_PASSWORD_CONFIG_PROP),
                            sslProperties.getProperty(SSLConfiguration.TRUSTSTORE_CONFIG_PROP),
                            sslProperties.getProperty(SSLConfiguration.TRUSTSTORE_PASSWORD_CONFIG_PROP));
                    SSLConfiguration.applySystemProperties(sslConfig);
                } catch (IOException ioe) {
                    throw new IllegalArgumentException("Unable to access SSL configuration.", ioe);
                }
            }
            SSLContext dummyContext = SSLConfiguration.initializeSslContext(sslConfig);
            m_sslEngine = dummyContext.createSSLEngine(m_host, m_clientPort);
            //m_sslEngine = dummyContext.createSSLEngine();
            m_sslEngine.setUseClientMode(true);
            TLSHandshaker handshaker = new TLSHandshaker(m_socket, m_sslEngine);
            boolean shookHands = false;
            try {
                shookHands = handshaker.handshake();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println(e.toString());
                throw new IOException("SSL handshake failed", e);
            }
            if (! shookHands) {
                throw new IOException("SSL handshake failed");
            }
        } catch (UnrecoverableKeyException | KeyManagementException | NoSuchAlgorithmException | KeyStoreException
                | CertificateException | IOException e) {
            e.printStackTrace();
            return;
        }


    }


    void setUp() throws Exception {
        try {
            //Build the catalog
            VoltProjectBuilder builder = new VoltProjectBuilder();
            String mySchema
                    = "create table A ("
                    + "s varchar(20) default null, "
                    + "); ";
            builder.addLiteralSchema(mySchema);
            String catalogJar = "dummy.jar";

            m_config = new LocalCluster(catalogJar, 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            m_config.portGenerator.enablePortProvider();
            m_config.portGenerator.pprovider.setNextClient(m_clientPort);
            m_config.setHasLocalServer(true);
            boolean success = m_config.compile(builder);
            assertTrue(success);
            m_config.startUp();
        } catch (IOException ex) {
            fail(ex.getMessage());
        }
        connect();
        m_ce = CipherExecutor.CLIENT;
        m_ce.startup();
    }

    void close() {
        if (m_socket != null) {
            try {
                m_socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    @After
    public void tearDown() throws Exception {
        close();
        if (m_config != null) {
            m_config.shutDown();
        }
    }

    private void fillByteBuf(ByteBuf buf) {
        int writeBytes = buf.writableBytes();
        for (int i = 0; i < writeBytes; ++i) {
            buf.writeByte( i % Byte.MAX_VALUE);
        }
    }

    @Test
    public void testDataChunker() {
        byte[] data = new byte[512];
        ByteBuf bb = Unpooled.wrappedBuffer(data).clear();
        fillByteBuf(bb);
        EncryptFrame frame = new EncryptFrame(bb, 1);
        final int buffSize = data.length;

        // exact size
        java.util.List<EncryptFrame> frames;
        frames = frame.chunked(buffSize);
        assertTrue(frames.size() == 1);
        assert (frames.get(0).isLast());

        // get chunk for with biggerSize
        frames = frame.chunked(buffSize + 10);
        assertTrue(frames.size() == 1);
        assert (frames.get(0).isLast());

     // get chunk for with biggerSize
        frames = frame.chunked(buffSize - 10);
        assertTrue(frames.size() == 2);
        assert (!frames.get(0).isLast());
        assert (frames.get(1).isLast());

    }

    void fillByteBuffer(ByteBuffer buffer) {
        int bytesToFill = buffer.remaining();
        for (int i = 0; i < bytesToFill; ++i) {
            buffer.put((byte) (i % Byte.MAX_VALUE));
        }
    }

    private DeferredSerialization deferredSerializeOf(final ByteBuffer buf) {
        return new DeferredSerialization() {
            @Override
            public void serialize(final ByteBuffer outbuf) throws IOException {
                outbuf.put(buf);
            }
            @Override
            public void cancel() {}
            @Override
            public int getSerializedSize() {
                return buf.remaining();
            }
        };
    }

    @Test
    public void testSink() throws Exception {
        setUp();
        MockChannel channel = new MockChannel(MockChannel.SINK, 0);
        MockTLSPort port = new MockTLSPort(m_sslEngine, m_ce, m_pool);
        TLSNIOWriteStream wstream = new TLSNIOWriteStream(port, null, null, null, m_sslEngine, m_ce);
        assertTrue(wstream.isEmpty());

        ByteBuffer tmp = ByteBuffer.allocate(21);
        fillByteBuffer(tmp);
        tmp.flip();
        DeferredSerialization ds = deferredSerializeOf(tmp);
        wstream.enqueue(ds);
        assertTrue(port.checkWriteSet());
        assertTrue(1 == wstream.getOutstandingMessageCount());

//        ByteBuffer tmp2 = ByteBuffer.allocate(2);
//        tmp2.put((byte) 1);
//        tmp2.put((byte) 2);
//        tmp2.flip();
//        wstream.enqueue(tmp2);
//        assertTrue(port.checkWriteSet());
//        assertEquals(2, wstream.getOutstandingMessageCount());

        int processedWrites = wstream.serializeQueuedWrites(m_pool);
        System.out.println(processedWrites);
        wstream.waitForPendingEncrypts();
        // TODO: get encrypted bytes, drain the channel and make sure encrypted bytes
        // is same as # of encrypted bytes and bytes drained are same
        Deque<EncryptFrame>encrptyedFrames = wstream.getEncryptedFrames();
        System.out.println("number of frames: " + encrptyedFrames.size());
        System.out.println(wstream.drainTo(channel));
        assertTrue(wstream.isEmpty());
        assertTrue(wstream.getOutstandingMessageCount() == 0);
        wstream.shutdown();
        port.toString();
    }



}
