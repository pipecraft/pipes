package org.pipecraft.pipes.async.inter.distributed;

import org.pipecraft.infra.io.Retrier;
import org.pipecraft.pipes.serialization.ByteArrayEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.compression.Lz4FrameEncoder;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.util.AttributeKey;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A netty client for shuffling items across workers. The protocol is: INT - length of message.
 * []BYTES - the msg.
 * <p>
 * When done, the client will send -1 and then the total size of the bytes it send, for validation
 * purposes.
 * <p>
 * TODO: tests.
 * <p>
 * TODO: we can use VARINT for the message length instead of int.
 *
 * @author Zacharya Haitin
 */
class ShuffleClient<T> {
  private static final byte[] LENGTH_PLACEHOLDER = new byte[4];
  private static final int SERVER_AVAILABILITY_MAX_WAIT_MS = 60 * 3 * 1000; // 3 min
  private static final int MS_BETWEEN_RETRY = 1000;
  private static final Retrier RETRIER = new Retrier(MS_BETWEEN_RETRY, 1,
          SERVER_AVAILABILITY_MAX_WAIT_MS / MS_BETWEEN_RETRY + 1);
  private static final byte[] CH_DONE_MESSAGE = new byte[]{};
  private static final byte[] WORKER_DONE_MESSAGE = new byte[]{};
  private static final AttributeKey<ConnectionCounter> BYTE_SENT = AttributeKey.valueOf("byte_sent");
  private final String host;
  private final int port;
  private final ByteArrayEncoder<T> encoder;
  private final ThreadLocal<Channel> threadConnection;
  private final Bootstrap bootstrap;
  private final Map<Long, Channel> connections = new ConcurrentHashMap<>();

  private class BytesEncoder extends MessageToByteEncoder<T> {
    @Override
    protected void encode(ChannelHandlerContext ctx, T msg, ByteBuf out) throws IOException {
      Channel ch = ctx.channel();
      ConnectionCounter counter = ch.attr(BYTE_SENT).get();
      if (counter == null) {
        counter = new ConnectionCounter((InetSocketAddress) ch.remoteAddress());
        ch.attr(BYTE_SENT).set(counter);
      }

      // It's working because netty does some weird casting
      if (msg == CH_DONE_MESSAGE) {
        out.writeInt(-1);
        out.writeLong(counter.getValue());
        return;
      } else if (msg == WORKER_DONE_MESSAGE) {
        out.writeInt(-2);
        return;
      }

      int startIndex = out.writerIndex();
      out.writeBytes(LENGTH_PLACEHOLDER);
      out.writeBytes(encoder.encode(msg));

      int messageLength = out.writerIndex() - startIndex - LENGTH_PLACEHOLDER.length;
      out.setInt(startIndex, messageLength);
      counter.add(messageLength);
    }
  }

  /**
   * Constructor.
   *
   * @param host    Host ip/DNS of the server.
   * @param port    Port of the server.
   * @param encoder
   * @param group   Netty event loop group the client should run in.
   */
  public ShuffleClient(String host, int port, ByteArrayEncoder<T> encoder,
                       ChannelInboundHandlerAdapter channelInboundHandler, EventLoopGroup group) {
    this.host = host;
    this.port = port;
    this.encoder = encoder;

    this.bootstrap = new Bootstrap();
    this.bootstrap.group(group).channel(NettyUtils.getSocketChanneClass())
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .option(ChannelOption.SO_SNDBUF, 1048576)
            .option(ChannelOption.SO_RCVBUF, 1048576)
            .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(2 * 65536, 10 * 65536))
            .handler(new ChannelInitializer<>() {
              @Override
              protected void initChannel(Channel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new Lz4FrameEncoder());
                pipeline.addLast(new FlushConsolidationHandler(256, true));
                pipeline.addLast(new BytesEncoder());
                pipeline.addLast(channelInboundHandler);
              }
            });

    this.threadConnection = ThreadLocal.withInitial(() -> {
      try {
        Channel ch = openNewConnection();
        connections.put(Thread.currentThread().getId(), ch);
        return ch;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
  }

  private Channel openNewConnection() throws IOException {
    try {
      return RETRIER.run(() -> this.bootstrap.connect(host, port).sync().channel());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } catch (Exception e) {
      // https://github.com/netty/netty/issues/2597#issuecomment-49780137
      if (e instanceof ConnectException) {
        throw new IOException(e);
      }

      throw e;
    }
  }

  /**
   * Send a msg to the server.
   *
   * @param msg The msg obj we should send over the socket.
   * @throws InterruptedException In case that the thread is interrupted
   * @throws IOException In case of an IO error while sending the message
   */
  public void send(T msg) throws InterruptedException, IOException {
    try {
      Channel ch = threadConnection.get();
      if (!ch.isWritable()) {
        ch.writeAndFlush(msg).sync();
      } else {
        ch.writeAndFlush(msg, ch.voidPromise());
      }
    } catch (UncheckedIOException e) {
      throw new IOException(e);
    }
  }

  /**
   * Send a done message to the server.
   *
   * @throws InterruptedException In case that the thread is interrupted
   * @throws IOException In case of an IO error while sending the message
   */
  public void done() throws InterruptedException, IOException {
    for (Channel ch : connections.values()) {
      ch.writeAndFlush(CH_DONE_MESSAGE).sync();
    }

    for (Channel ch : connections.values()) {
      ch.closeFuture().sync(); // Waiting for the server to get done message
    }

    Channel ch = openNewConnection();
    ch.writeAndFlush(WORKER_DONE_MESSAGE).sync();
    ch.closeFuture().sync();

    connections.clear();
  }
}
