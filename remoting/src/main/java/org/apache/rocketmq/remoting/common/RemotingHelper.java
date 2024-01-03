/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.common;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

public class RemotingHelper {
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final String DEFAULT_CIDR_ALL = "0.0.0.0/0";

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    public static final Map<Integer, String> REQUEST_CODE_MAP = new HashMap<Integer, String>() {
        {
            try {
                Field[] f = RequestCode.class.getFields();
                for (Field field : f) {
                    if (field.getType() == int.class) {
                        put((int) field.get(null), field.getName().toLowerCase());
                    }
                }
            } catch (IllegalAccessException ignore) {
            }
        }
    };

    public static final Map<Integer, String> RESPONSE_CODE_MAP = new HashMap<Integer, String>() {
        {
            try {
                Field[] f = ResponseCode.class.getFields();
                for (Field field : f) {
                    if (field.getType() == int.class) {
                        put((int) field.get(null), field.getName().toLowerCase());
                    }
                }
            } catch (IllegalAccessException ignore) {
            }
        }
    };

    public static <T> T getAttributeValue(AttributeKey<T> key, final Channel channel) {
        if (!channel.hasAttr(key)) {
            return null;
        }

        Attribute<T> attribute = channel.attr(key);
        return attribute.get();
    }

    public static <T> void setPropertyToAttr(final Channel channel, AttributeKey<T> attributeKey, T value) {
        if (channel == null) {
            return;
        }
        channel.attr(attributeKey).set(value);
    }

    public static SocketAddress string2SocketAddress(final String addr) {
        int split = addr.lastIndexOf(":");
        String host = addr.substring(0, split);
        String port = addr.substring(split + 1);
        return new InetSocketAddress(host, Integer.parseInt(port));
    }

    public static RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException {

        boolean sendRequestOK = false;
        long beginTime = System.currentTimeMillis();
        SocketChannel socketChannel = openSocketChannel(addr);

        try {
            socketChannel.configureBlocking(true);
            //bugfix  http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4614802
            socketChannel.socket().setSoTimeout((int) timeoutMillis);

            writeRequest(socketChannel, request, addr, beginTime, timeoutMillis);
            sendRequestOK = true;

            int size = getBufferSize(socketChannel, addr, beginTime, timeoutMillis);
            ByteBuffer byteBufferBody = ByteBuffer.allocate(size);
            readBuffer(byteBufferBody, socketChannel, addr, beginTime, timeoutMillis);
            byteBufferBody.flip();

            return RemotingCommand.decode(byteBufferBody);
        } catch (IOException e) {
            return handleSyncException(e, sendRequestOK, addr, timeoutMillis);
        } finally {
            closeSocketChannel(socketChannel);
        }
    }

    private static SocketChannel openSocketChannel(String addr) throws RemotingConnectException {
        SocketAddress socketAddress = NetworkUtils.string2SocketAddress(addr);
        SocketChannel socketChannel = connect(socketAddress);
        if (null == socketChannel) {
            throw new RemotingConnectException(addr);
        }

        return socketChannel;
    }

    private static void closeSocketChannel(SocketChannel socketChannel) {
        if (socketChannel == null) {
            return;
        }

        try {
            socketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void readBuffer(ByteBuffer byteBufferBody, SocketChannel socketChannel, String addr, long beginTime, long timeout) throws RemotingSendRequestException, IOException, InterruptedException {
        while (byteBufferBody.hasRemaining()) {
            int length = socketChannel.read(byteBufferBody);
            checkChannelOperationResult(length, addr, beginTime, timeout, byteBufferBody);

            Thread.sleep(1);
        }
    }

    private static int getBufferSize(SocketChannel socketChannel, String addr, long beginTime, long timeout) throws RemotingSendRequestException, IOException, InterruptedException {
        ByteBuffer byteBufferSize = ByteBuffer.allocate(4);
        while (byteBufferSize.hasRemaining()) {
            int length = socketChannel.read(byteBufferSize);
            checkChannelOperationResult(length, addr, beginTime, timeout, byteBufferSize);

            Thread.sleep(1);
        }

        return byteBufferSize.getInt(0);
    }

    private static void writeRequest(SocketChannel socketChannel, RemotingCommand request, String addr, long beginTime, long timeout) throws RemotingSendRequestException, IOException, InterruptedException {
        ByteBuffer byteBufferRequest = request.encode();
        while (byteBufferRequest.hasRemaining()) {
            int length = socketChannel.write(byteBufferRequest);
            checkChannelOperationResult(length, addr, beginTime, timeout, byteBufferRequest);

            Thread.sleep(1);
        }
    }

    private static void checkChannelOperationResult(int length, String addr, long beginTime, long timeout, ByteBuffer buffer) throws RemotingSendRequestException {
        if (length <= 0) {
            throw new RemotingSendRequestException(addr);
        }

        if (!buffer.hasRemaining()) {
            return;
        }

        if ((System.currentTimeMillis() - beginTime) > timeout) {
            throw new RemotingSendRequestException(addr);
        }
    }

    private static RemotingCommand handleSyncException(IOException e, boolean sendRequestOK, String addr, long timeoutMillis) throws RemotingTimeoutException, RemotingSendRequestException {
        log.error("invokeSync failure", e);

        if (sendRequestOK) {
            throw new RemotingTimeoutException(addr, timeoutMillis);
        } else {
            throw new RemotingSendRequestException(addr);
        }
    }

    public static String parseChannelRemoteAddr(final Channel channel) {
        if (null == channel) {
            return "";
        }
        String addr = getProxyProtocolAddress(channel);
        if (StringUtils.isNotBlank(addr)) {
            return addr;
        }
        Attribute<String> att = channel.attr(AttributeKeys.REMOTE_ADDR_KEY);
        if (att == null) {
            // mocked in unit test
            return parseChannelRemoteAddr0(channel);
        }
        addr = att.get();
        if (addr == null) {
            addr = parseChannelRemoteAddr0(channel);
            att.set(addr);
        }
        return addr;
    }

    private static String getProxyProtocolAddress(Channel channel) {
        if (!channel.hasAttr(AttributeKeys.PROXY_PROTOCOL_ADDR)) {
            return null;
        }

        String proxyProtocolAddr = getAttributeValue(AttributeKeys.PROXY_PROTOCOL_ADDR, channel);
        String proxyProtocolPort = getAttributeValue(AttributeKeys.PROXY_PROTOCOL_PORT, channel);
        if (StringUtils.isBlank(proxyProtocolAddr) || proxyProtocolPort == null) {
            return null;
        }
        return proxyProtocolAddr + ":" + proxyProtocolPort;
    }

    private static String parseChannelRemoteAddr0(final Channel channel) {
        SocketAddress remote = channel.remoteAddress();
        final String addr = remote != null ? remote.toString() : "";

        if (addr.length() <= 0) {
            return "";
        }

        int index = addr.lastIndexOf("/");
        if (index >= 0) {
            return addr.substring(index + 1);
        }

        return addr;
    }

    public static String parseHostFromAddress(String address) {
        if (address == null) {
            return "";
        }

        String[] addressSplits = address.split(":");
        if (addressSplits.length < 1) {
            return "";
        }

        return addressSplits[0];
    }

    public static String parseSocketAddressAddr(SocketAddress socketAddress) {
        if (socketAddress == null) {
            return "";
        }

        // Default toString of InetSocketAddress is "hostName/IP:port"
        final String addr = socketAddress.toString();
        int index = addr.lastIndexOf("/");
        return (index != -1) ? addr.substring(index + 1) : addr;
    }

    public static Integer parseSocketAddressPort(SocketAddress socketAddress) {
        if (socketAddress instanceof InetSocketAddress) {
            return ((InetSocketAddress) socketAddress).getPort();
        }
        return -1;
    }

    public static int ipToInt(String ip) {
        String[] ips = ip.split("\\.");
        return (Integer.parseInt(ips[0]) << 24)
            | (Integer.parseInt(ips[1]) << 16)
            | (Integer.parseInt(ips[2]) << 8)
            | Integer.parseInt(ips[3]);
    }

    public static boolean ipInCIDR(String ip, String cidr) {
        int ipAddr = ipToInt(ip);
        String[] cidrArr = cidr.split("/");
        int netId = Integer.parseInt(cidrArr[1]);
        int mask = 0xFFFFFFFF << (32 - netId);
        int cidrIpAddr = ipToInt(cidrArr[0]);

        return (ipAddr & mask) == (cidrIpAddr & mask);
    }

    public static SocketChannel connect(SocketAddress remote) {
        return connect(remote, 1000 * 5);
    }

    public static SocketChannel connect(SocketAddress remote, final int timeoutMillis) {
        SocketChannel sc = null;
        try {
            sc = SocketChannel.open();
            sc.configureBlocking(true);
            sc.socket().setSoLinger(false, -1);
            sc.socket().setTcpNoDelay(true);
            if (NettySystemConfig.socketSndbufSize > 0) {
                sc.socket().setReceiveBufferSize(NettySystemConfig.socketSndbufSize);
            }
            if (NettySystemConfig.socketRcvbufSize > 0) {
                sc.socket().setSendBufferSize(NettySystemConfig.socketRcvbufSize);
            }
            sc.socket().connect(remote, timeoutMillis);
            sc.configureBlocking(false);
            return sc;
        } catch (Exception e) {
            closeSocketChannel(sc);
        }

        return null;
    }

    public static void closeChannel(Channel channel) {
        final String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        if ("".equals(addrRemote)) {
            channel.close();
            return;
        }

        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                log.info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote,
                    future.isSuccess());
            }
        });
    }

    public static String getRequestCodeDesc(int code) {
        return REQUEST_CODE_MAP.getOrDefault(code, String.valueOf(code));
    }

    public static String getResponseCodeDesc(int code) {
        return RESPONSE_CODE_MAP.getOrDefault(code, String.valueOf(code));
    }
}
