package org.pipecraft.pipes.async.inter.distributed;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Utilities for generating Netty resources dependent on the operating system.
 *
 * @author Eyal Schneider
 */
public class NettyUtils {

  /**
   * @param nThreads The required number of threads in the group
   * @return A new {@link EventLoopGroup}, suitable for the current OS.
   */
  public static EventLoopGroup newEventLoopGroup(int nThreads) {
    if (Epoll.isAvailable()) {
      return new EpollEventLoopGroup(nThreads);
    } else {
      return new NioEventLoopGroup(nThreads);
    }
  }

  /**
   * @return The server socket channel impl class suitable for the OS
   */
  public static Class<? extends ServerSocketChannel> getServerSocketChanneClass() {
    if (Epoll.isAvailable()) {
      return EpollServerSocketChannel.class;
    } else {
      return NioServerSocketChannel.class;
    }
  }

  /**
   * @return The socket channel impl class suitable for the OS
   */
  public static Class<? extends SocketChannel> getSocketChanneClass() {
    if (Epoll.isAvailable()) {
      return EpollSocketChannel.class;
    } else {
      return NioSocketChannel.class;
    }
  }

}
