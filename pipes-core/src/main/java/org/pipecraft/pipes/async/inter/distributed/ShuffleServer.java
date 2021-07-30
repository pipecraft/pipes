package org.pipecraft.pipes.async.inter.distributed;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.pipecraft.pipes.exceptions.PipeException;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.AttributeKey;

/**
 * A netty server for receiving shuffled items from workers. When a client is done (receiving -1), the
 * server will validate it got all the bytes the client sent and increase the num of deadWorkers.
 *
 * @author Zacharya Haitin
 */
class ShuffleServer implements Closeable {
  private static final int PROCESSORS = Runtime.getRuntime().availableProcessors();
  private static final AttributeKey<ConnectionCounter> BYTE_SENT = AttributeKey.valueOf("byte_sent");
  private final int port;
  private final ChannelInboundHandlerAdapter channelInboundHandler;
  private final EpollEventLoopGroup workerGroup;
  private final CountDownLatch doneLatch;
  private final CheckedConsumer<byte[]> handler;
  private EpollEventLoopGroup bossGroup;

  @FunctionalInterface
  public interface CheckedConsumer<T> {
    void accept(T t) throws IOException, PipeException, InterruptedException;
  }

  private class MessageDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out)
            throws IOException, PipeException, InterruptedException {
      Channel ch = ctx.channel();
      ConnectionCounter counter = ch.attr(BYTE_SENT).get();
      if (counter == null) {
        counter = new ConnectionCounter((InetSocketAddress) ch.remoteAddress());
        ch.attr(BYTE_SENT).set(counter);
      }

      while (buf.readableBytes() >= 4) {
        // If we didn't receive the entire msg in the socket yet, we'll reset the index to this part.
        buf.markReaderIndex();
        int dataLength = buf.readInt();

        if (dataLength < 0) {
          if (dataLength == -1) {
            // Waiting to get a long in the socket
            if (buf.readableBytes() < 8) {
              buf.resetReaderIndex();
              return;
            }

            long totalSent = buf.readLong();
            long counterValue = counter.getValue();
            if (counterValue != totalSent) {
              throw new RuntimeException("Got only " + counterValue + " but should've got " + totalSent
                      + " from " + counter.getAddress());
            }
          } else {
            doneLatch.countDown(); // Worker is finished
          }

          ctx.close(); // Telling the client we're cool
          return;
        }

        if (buf.readableBytes() < dataLength) {
          buf.resetReaderIndex();
          return;
        }

        byte[] msg = new byte[dataLength];
        buf.readBytes(msg);
        handler.accept(msg);

        counter.add(dataLength);
      }
    }
  }

  /**
   * Constructor.
   *
   * @param port                  The port number the server will bind to.
   * @param channelInboundHandler Netty Inbound adapter to handle the incoming message.
   * @param workerGroup           Epoll event group for netty workers.
   *                              The class doesn't take ownership and will not shutdown this group.
   * @param handler               A callback to handle incoming messages, in their raw representation
   */
  public ShuffleServer(int port, CountDownLatch doneLatch, ChannelInboundHandlerAdapter channelInboundHandler,
                       EpollEventLoopGroup workerGroup, CheckedConsumer<byte[]> handler) {
    this.port = port;
    this.channelInboundHandler = channelInboundHandler;
    this.workerGroup = workerGroup;
    this.doneLatch = doneLatch;
    this.handler = handler;
  }

  public void start() throws InterruptedException {
    bossGroup = new EpollEventLoopGroup(PROCESSORS);

    ServerBootstrap b = new ServerBootstrap();
    b.group(bossGroup, workerGroup).channel(EpollServerSocketChannel.class)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .childOption(ChannelOption.SO_SNDBUF, 1048576)
            .childOption(ChannelOption.SO_RCVBUF, 1048576)
            .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(2 * 65536, 10 * 65536))
            .childHandler(new ChannelInitializer<SocketChannel>() {
              @Override
              public void initChannel(SocketChannel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new Lz4FrameDecoder());
                pipeline.addLast(new MessageDecoder());
                pipeline.addLast(channelInboundHandler);
              }
            });

    b.bind(port).sync();
  }

  /**
   * Waiting uninterruptibly until the boss shutdown.
   */
  public void close() {
    if (bossGroup.isShuttingDown()) return;
    bossGroup.shutdownGracefully(2, 10, TimeUnit.SECONDS).syncUninterruptibly();
  }
}
