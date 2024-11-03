import com.rabbitmq.client.Channel;
import java.util.concurrent.TimeoutException;
import org.apache.commons.pool2.ObjectPool;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.pool2.impl.GenericObjectPool;

public class RMQChannelPool {
  private final BlockingQueue<Channel> pool;
  private int capacity;
  // used to create channels
  private RMQChannelFactory factory;

  public RMQChannelPool(int maxSize, RMQChannelFactory factory) {
    this.capacity = maxSize;
    pool = new LinkedBlockingQueue<>(capacity);
    this.factory = factory;
    for (int i = 0; i < capacity; i++) {
      Channel chan;
      try {
        chan = factory.create();
        pool.put(chan);
      } catch (IOException | InterruptedException ex) {
        Logger.getLogger(RMQChannelPool.class.getName()).log(Level.SEVERE, null, ex);
        throw new RuntimeException("Failed to initialize channel pool", ex);
      }

    }
  }

  public Channel borrowChannel() throws IOException {
    try {
      return pool.take();
    } catch (InterruptedException e) {
      throw new RuntimeException("Error: no channels available" + e.toString());
    }
  }

  public void returnChannel(Channel channel) throws Exception {
    if (channel != null && channel.isOpen()) {
      pool.add(channel);
    } else {
      // Create a new channel to replace the closed one
      Channel newChannel = factory.create();
      pool.add(newChannel);
    }
  }

  public void close() {
    for (Channel channel : pool) {
      try {
        if (channel != null && channel.isOpen()) {
          channel.close();
        }
      } catch (IOException | TimeoutException e) {
        Logger.getLogger(RMQChannelPool.class.getName()).log(Level.SEVERE, null, e);
      }
    }
  }
}