package dht.chord.rpc;

import dht.chord.ChordNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.ServerSocket;
import java.util.concurrent.Executors;

/**
 * Simple RPC server that will listen for new connections. Each new connection is handled by a
 * RPCHandler instance, which runs as a thread inside a size-fixed thread pool.
 *
 * @author Matteo Filipponi
 * @version 1.0
 */
public class RPCServer implements Runnable {
    private int port;
    private int threadPoolSize;
    private ChordNode node;

    private final Logger logger = LogManager.getLogger(RPCServer.class);

    public RPCServer(int port, int threadPoolSize, ChordNode node) {
        this.port = port;
        this.threadPoolSize = threadPoolSize;
        this.node = node;
    }

    /**
     * Start the RPC server as a thread and listen for new connections.
     */
    @Override
    public void run() {
        logger.info("Starting RPC server on port: {} (thread pool size = {})", this.port, this.threadPoolSize);

        try {
            var listener = new ServerSocket(this.port);
            var pool = Executors.newFixedThreadPool(this.threadPoolSize);
            while (true) {
                pool.execute(new RPCHandler(listener.accept(), this.node));
            }
        } catch (Exception e) {
            logger.fatal("RPC Server has failed: {}", e.getMessage());
        }
    }
}
