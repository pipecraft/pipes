package org.pipecraft.pipes.async.inter.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import org.pipecraft.pipes.async.AsyncPipe;
import org.pipecraft.pipes.async.AsyncPipeListener;
import org.pipecraft.pipes.exceptions.PipeException;
import org.pipecraft.infra.net.HostPort;
import org.pipecraft.pipes.exceptions.IOPipeException;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.epoll.EpollEventLoopGroup;

/**
 * An async pipe that takes input and shuffle it across multiple workers.
 * <p>
 * TODO: Add logging.
 *
 * @param <T> The item data type
 * @author Zacharya Haitin
 */
public class DistributedShufflerPipe<T> extends AsyncPipe<T> {
  private final AsyncPipe<T> input;
  private final DistributedShufflerConfig<T> config;
  private final Thread watcher;
  private final CountDownLatch doneLatch;
  private EpollEventLoopGroup group;
  private ShuffleServer shuffleServer;

  /**
   * Another Constructor????? YES!
   *
   * @param input  The input pipe supplying items which will be shuffled between the workers.
   * @param config The config object specifying the shuffler settings
   */
  public DistributedShufflerPipe(AsyncPipe<T> input, DistributedShufflerConfig<T> config) {
    this.input = input;
    this.config = config;
    
    this.doneLatch = new CountDownLatch(config.getWorkers().size() + 1);  // Extra one for the done function
    watcher = new Thread(() -> {
      try {
        doneLatch.await();
        notifyDone();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });    
  }
  
  @Override
  public void start() throws PipeException, InterruptedException {
    group = new EpollEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);
    shuffleServer = new ShuffleServer(config.getPort(), doneLatch, new ServerHandler(), group, bytes -> notifyNext(config.getCodec().decode(bytes)));
    shuffleServer.start();
    watcher.start();
    List<ShuffleClient<T>> clients = new ArrayList<>(config.getWorkers().size());

    for (HostPort host : config.getWorkers()) {
      ShuffleClient<T> client = new ShuffleClient<>(host.getHost(), host.getPort(), config.getCodec(), new ClientHandler(), group);
      clients.add(client);
    }

    input.setListener(new AsyncPipeListener<>() {
      final Function<T, Integer> shardFunction = config.getShardFunc();
      @Override
      public void next(T item) throws InterruptedException, IOPipeException {
        try {
          int shardId = shardFunction.apply(item);
          clients.get(shardId).send(item);
        } catch (IOException e) {
          throw new IOPipeException(e);
        }
      }

      @Override
      public void done() throws InterruptedException {
        try {
          for (ShuffleClient<T> client : clients) {
            client.done();
          }
        } catch (IOException e) {
          notifyError(new IOPipeException(e));
          return;
        }

        doneLatch.countDown();
      }

      @Override
      public void error(PipeException e) throws InterruptedException {
        shuffleServer.close();  // So next will never be called again
        notifyError(e);
      }
    });

    input.start();
  }

  @Override
  public void close() throws IOException {
    super.close();
    input.close();
    if (!group.isShuttingDown()) {
      group.shutdownGracefully();
    }

    shuffleServer.close();
    try {
      watcher.interrupt();
      watcher.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public float getProgress() {
    return input.getProgress();
  }
  
  /**
   * A utility method for determining the shard id a worker is responsible for.
   * Each worker among the N workers exclusively "owns" a shard, whose id is
   * in the range 0..N.
   * 
   * This method is expected to be used only by pipelines which have full control over sharding
   * (i.e. those which provide an explicit sharding function to the shuffler pipe). Such pipelines
   * may need to know the exact shard the worker is working on, in order to for example fetch additional
   * resources belonging to the same shard.
   * 
   * @param workers The list of all workers. Order isn't important.
   * @param workerIndex The index of the worker in the given workers list. Must be between 0 and workers.size() - 1
   * @return The shard id owned by the worker at position workerIndex in the workers list.
   * @deprecated Use DistributedShufflerConfig.getWorkerShardId(..) instead
   */
  public static int getWorkerShardId(List<HostPort> workers, int workerIndex) {
    HostPort worker = workers.get(workerIndex);
    List<HostPort> workersCopy = new ArrayList<>(workers);
    canonicalWorkerSort(workersCopy);
    return workersCopy.indexOf(worker);
  }
  
  /**
   * Re-orders the given worker list such that the resulting list is canonical,
   * and isn't affected by the original ordering.
   * The resulting ordering represents the shard id each worker owns.
   * 
   * @param workers The workers to re-order
   */
  private static void canonicalWorkerSort(List<HostPort> workers) {
    Collections.sort(workers);
  }
  
  @ChannelHandler.Sharable
  private class ServerHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      notifyError(new IOPipeException(cause));
      ctx.close();
    }
  }

  @ChannelHandler.Sharable
  private class ClientHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      notifyError(new IOPipeException(cause));
      ctx.close();
    }
  }

}
