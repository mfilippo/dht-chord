package dht.chord.rpc;

import dht.chord.ChordNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * The RPCHandler is run as a thread when a new connection is established in the RPCServer.
 * This handler is responsible of parsing, process and reply to the received messages (one per connection).
 *
 * @author Matteo Filipponi
 * @version 1.0
 */
public class RPCHandler implements Runnable {
    private Socket socket;
    private ChordNode node;

    private final Logger logger = LogManager.getLogger(RPCHandler.class);

    RPCHandler(Socket socket, ChordNode node) {
        this.socket = socket;
        this.node = node;
    }

    // ==============================
    // User interface
    // ==============================

    /**
     * Handle a PUT message. As part of the user interface, it is used to find the successor of the
     * key ID and to store there the <key_id, value> tuple.
     * Message format is:
     * PUT <key_id> <value>
     *
     * @param message The message as a string
     * @return "OK"
     */
    private String handlePut(String message) {
        logger.debug("Handle PUT: {}", message);

        var id = new BigInteger(message.split(" ")[1]);
        var value = message.split(" ")[2];
        this.node.dhtPut(id, value);
        return RPCMessage.OK.name();
    }

    /**
     * Handle a GET message. As part of the user interface, it is used to retrieve the value associated
     * to the given key.
     * Message format is:
     * GET <key_id>
     *
     * @param message The message as a string
     * @return The associated value
     */
    private String handleGet(String message) {
        logger.info("Handle: {}", message);
        var id = new BigInteger(message.split(" ")[1]);
        return this.node.dhtGet(id);
    }

    /**
     * Handle a DELETE message. As part of the user interface, it is used to delete the value associated
     * to the given key.
     * Message format is:
     * DELETE <key_id>
     *
     * @param message The message as a string
     * @return "OK"
     */
    private String handleDelete(String message) {
        var id = new BigInteger(message.split(" ")[1]);
        this.node.dhtDelete(id);
        return RPCMessage.OK.name();
    }

    // ==============================
    // DHT interface
    // ==============================

    /**
     * Handle a STORE message. It is used to store a <key_id, value> tuple in the node DB.
     * Message format is:
     * STORE <key_id> <value>
     *
     * @param message The message as a string
     * @return "OK"
     */
    private String handleStore(String message) {
        logger.debug("Handle STORE: {}", message);

        var id = new BigInteger(message.split(" ")[1]);
        var value = message.split(" ")[2];
        this.node.dhtStore(id, value);
        return RPCMessage.OK.name();
    }

    /**
     * Handle a TRANSFER message. It is used by a joining node to retrieve data from its successor.
     * Message format is:
     * TRANSFER <key_id>
     *
     * @param message The message as a string
     * @return "NULL" if there is nothing to transfer or "<key_id:value> ... <key_id:value>"
     */
    private String handleTransfer(String message) {
        logger.debug("Handle TRANSFER: {}", message);

        var id = new BigInteger(message.split(" ")[1]);
        var mapToTransfer = this.node.dhtGetKeysToTransfer(id);
        return mapToTransfer.isEmpty() ? RPCMessage.NULL.name() : mapToTransfer.entrySet().stream()
                .map(x -> x.getKey() + ":" + x.getValue())
                .collect(Collectors.joining(" "));
    }

    /**
     * Handle a PING message. It is used to check if the node is alive.
     * Message format is:
     * PING
     *
     * @return "PONG"
     */
    private String handlePingMessage() {
        logger.debug("Handle PING");

        return RPCMessage.PONG.name();
    }

    /**
     * Handle FIND_SUCCESSOR message. Used to query for the successor of a given id by invoking the
     * local procedure.
     * Message format is:
     * FIND_SUCCESSOR <id>
     *
     * @param message The message as a string
     * @return The successor address as "<id>@<host>:<port>"
     */
    private String handleFindSuccessorMessage(String message) {
        logger.debug("Handle FIND_SUCCESSOR: {}", message);

        var id = new BigInteger(message.split(" ")[1]);
        return this.node.dhtFindSuccessor(id);
    }

    /**
     * Handle GET_PREDECESSOR message. Used to query for the successor of this node.
     * Message format is :
     * GET_PREDECESSOR
     *
     * @return The predecessor address as "<id>@<host>:<port>"
     */
    private String handleGetPredecessorMessage() {
        logger.debug("Handle GET_PREDECESSOR");

        return this.node.getPredecessorAddress();
    }

    /**
     * Handle NOTIFY message. Used to notify this node about a new potential predecessor by invoking
     * the local procedure.
     * Message format is:
     * NOTIFY <id>@<host>:<port>
     *
     * @param message The message as a string
     * @return "OK"
     */
    private String handleNotifyMessage(String message) {
        logger.debug("Handle NOTIFY: {}", message);

        this.node.dhtNotify(message.split(" ")[1]);
        return RPCMessage.OK.name();
    }

    /**
     * Handle GET_SUCCESSOR_LIST message. Used to query for the successor list of this node.
     * Message format is:
     * GET_SUCCESSOR_LIST
     *
     * @return the list of successors as "<chord_id>@<host>:<port> ... <chord_id>@<host>:<port>"
     */
    private String handleGetSuccessorListMessage() {
        logger.debug("Handle GET_SUCCESSOR_LIST");

        var list = this.node.getSuccessorList();
        return String.join(" ", list);
    }

    /**
     * Handle an undefined message and parse it to determine which specific handler will have to
     * process the message.
     *
     * @param message A message as a string
     * @return The response of the message or "NULL" in case of error
     */
    private String handleMessage(String message) {
        logger.debug("Handling message: {}", message);

        String response = null;
        try {
            switch (RPCMessage.valueOf(message.split(" ")[0])) {

                case STORE:
                    response = this.handleStore(message);
                    break;

                case TRANSFER:
                    response = this.handleTransfer(message);
                    break;

                case FIND_SUCCESSOR:
                    response = this.handleFindSuccessorMessage(message);
                    break;

                case GET_SUCCESSOR_LIST:
                    response = this.handleGetSuccessorListMessage();
                    break;

                case GET_PREDECESSOR:
                    response = this.handleGetPredecessorMessage();
                    break;

                case NOTIFY:
                    response = this.handleNotifyMessage(message);
                    break;

                case PING:
                    response = this.handlePingMessage();
                    break;

                case PUT:
                    response = this.handlePut(message);
                    break;

                case GET:
                    response = this.handleGet(message);
                    break;

                case DELETE:
                    response = this.handleDelete(message);
                    break;

                default:
                    break;
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            response = RPCMessage.ERROR.name();
        }
        return response == null ? RPCMessage.NULL.name() : response;
    }

    /**
     * Start the current handler as a thread upon a new connection. The thread will listen for a
     * message and then write a response. After that the connection is closed.
     */
    @Override
    public void run() {
        logger.debug("New connection: {}", this.socket);

        try {
            var in = new Scanner(socket.getInputStream());
            var out = new PrintWriter(socket.getOutputStream(), true);
            while (in.hasNextLine()) {
                var message = in.nextLine();
                var response = this.handleMessage(message);
                out.println(response);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());

        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
            logger.debug("Closed connection: {}", this.socket);
        }
    }
}
