package dht.chord.rpc;

import dht.chord.exceptions.NodeFailException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.util.*;
import java.util.stream.Collectors;

public class RPCClient {
    private final Logger logger = LogManager.getLogger(RPCClient.class);

    // ==============================
    // User interface
    // ==============================

    public void put(String host, int port, BigInteger keyID, String value) throws NodeFailException {
        logger.debug("PUT");

        var message = String.format("%s %s %s", RPCMessage.PUT.name(), keyID, value);
        this.sendMessage(host, port, message);
    }

    public String get(String host, int port, BigInteger keyID) throws NodeFailException {
        logger.debug("GET");

        var message = String.format("%s %s", RPCMessage.GET.name(), keyID);
        return this.sendMessage(host, port, message);
    }

    public void delete(String host, int port, BigInteger keyID) throws NodeFailException {
        logger.debug("DELETE");

        var message = String.format("%s %s", RPCMessage.DELETE.name(), keyID);
        this.sendMessage(host, port, message);
    }

    // ==============================
    // DHT interface
    // ==============================

    /**
     * Execute a TRANSFER request.
     *
     * @param host The target host
     * @param port The target port
     * @param id   The target id
     * @return A map containing the transferred key/values
     * @throws NodeFailException if the target node has failed
     */
    public Map<BigInteger, String> transfer(String host, int port, BigInteger id) throws NodeFailException {
        logger.debug("Transfer {}", id);

        var message = String.format("%s %s", RPCMessage.TRANSFER.name(), id);
        var response = this.sendMessage(host, port, message);
        return response == null ? new HashMap<>() : Arrays.stream(response.split(" ")).collect(Collectors.toMap(
                x -> new BigInteger(x.split(":")[0]),
                x -> x.split(":")[1]
        ));
    }

    public void store(String host, int port, BigInteger keyID, String value) throws NodeFailException {
        logger.debug("STORE {} at {}:{}", keyID, host, port);

        var message = String.format("%s %s %s", RPCMessage.STORE.name(), keyID, value);
        this.sendMessage(host, port, message);
    }

    public void ping(String host, int port) throws NodeFailException {
        logger.debug("Ping");

        var message = RPCMessage.PING.name();
        var response = this.sendMessage(host, port, message);
        if (response == null || !response.equals(RPCMessage.PONG.name())) {
            throw new NodeFailException(String.format("%s:%s", host, port));
        }
    }

    public List<String> getSuccessorList(String host, int port) throws NodeFailException {
        logger.debug("Get successor list");

        var message = RPCMessage.GET_SUCCESSOR_LIST.name();
        var response = this.sendMessage(host, port, message);
        return response == null ? null : new ArrayList<>(Arrays.asList(response.split(" ")));
    }

    public void notify(String host, int port, String nodeAddress) throws NodeFailException {
        logger.debug("Notify");

        var message = String.format("%s %s", RPCMessage.NOTIFY, nodeAddress);
        this.sendMessage(host, port, message);
    }

    public String findSuccessor(String host, int port, BigInteger chordID) throws NodeFailException {
        logger.debug("Find successor");

        var message = String.format("%s %s", RPCMessage.FIND_SUCCESSOR, chordID);
        return this.sendMessage(host, port, message);
    }

    public String getPredecessor(String host, int port) throws NodeFailException {
        logger.debug("Get predecessor");

        var message = RPCMessage.GET_PREDECESSOR.name();
        return this.sendMessage(host, port, message);
    }

    private String sendMessage(String host, int port, String message) throws NodeFailException {
        logger.debug("Sending {} to {}:{}", message, host, port);

        String response = null;
        try (var socket = new Socket(host, port)) {
            var in = new Scanner(socket.getInputStream());
            var out = new PrintWriter(socket.getOutputStream(), true);
            out.println(message);
            response = in.nextLine();
        } catch (Exception e) {
            logger.error("Error while sending message {}: {}", message, e.getMessage());
            throw new NodeFailException(String.format("%s:%s", host, port));
        }
        return response.equals(RPCMessage.NULL.name()) ? null : response;
    }
}
