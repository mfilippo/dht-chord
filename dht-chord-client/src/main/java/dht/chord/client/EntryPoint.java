package dht.chord.client;

import dht.chord.exceptions.NodeFailException;
import dht.chord.rpc.RPCClient;
import dht.chord.utils.ChordUtils;
import org.apache.commons.codec.binary.Base64;

import java.math.BigInteger;
import java.util.Scanner;
import java.util.UUID;

public class EntryPoint {
    private static String host = "localhost";
    private static int port = 7777;
    private static int m = dht.chord.EntryPoint.M_DEFAULT;
    private static RPCClient rpcClient = new RPCClient();

    public static void main(String[] args) {
        System.out.println("================");
        System.out.println("DHT Chord Client");
        System.out.println("================");
        printSelectedNode();

        var scanner = new Scanner(System.in);

        while (true) {
            try {
                System.out.println();
                System.out.print(">>> ");
                var input = scanner.nextLine().strip().toLowerCase();
                var inputArgs = input.split("\\s+");
                var op = inputArgs[0];

                switch (op) {

                    case "host":
                        handleHost(inputArgs);
                        break;

                    case "port":
                        handlePort(inputArgs);
                        break;

                    case "put":
                        handlePut(inputArgs);
                        break;

                    case "putrandom":
                        handlePutRandom(inputArgs);
                        break;

                    case "get":
                        handleGet(inputArgs);
                        break;

                    case "getid":
                        handleGetID(inputArgs);
                        break;

                    case "delete":
                        handleDelete(inputArgs);
                        break;

                    case "exit":
                        handleExit(inputArgs);
                        break;

                    default:
                        throw new Exception("Unknown command");
                }

            } catch (NodeFailException e) {
                System.out.println("ERROR: Node failed " + e.getMessage());
            } catch (Exception e) {
                System.out.println("ERROR: " + e.getMessage());
            }
        }
    }

    private static void handleHost(String[] args) throws Exception {
        if (args.length != 2) throw new Exception();
        host = args[1];
        printSelectedNode();
    }

    private static void handlePort(String[] args) throws Exception {
        if (args.length != 2) throw new Exception();
        port = Integer.parseInt(args[1]);
        printSelectedNode();
    }

    private static void handlePut(String[] args) throws Exception {
        if (args.length != 3) throw new Exception();
        var keyID = ChordUtils.keyToChordID(args[1], m);
        var value = Base64.encodeBase64String(args[2].getBytes());
        System.out.println(String.format("PUT %s:%s -> %s:%s", keyID, value, host, port));
        rpcClient.put(host, port, keyID, value);
        System.out.println("OK");
    }

    private static void handlePutRandom(String[] args) throws Exception {
        if (args.length != 2) throw new Exception();
        var n = Integer.parseInt(args[1]);
        for (int i = 0; i < n; i++) {
            var keyID = ChordUtils.keyToChordID(UUID.randomUUID().toString(), m);
            var value = Base64.encodeBase64String(UUID.randomUUID().toString().getBytes());
            System.out.println(String.format("PUT %s:%s -> %s:%s", keyID, value, host, port));
            rpcClient.put(host, port, keyID, value);
        }
        System.out.println("OK");
    }

    private static void handleGet(String[] args) throws Exception {
        if (args.length != 2) throw new Exception();
        var keyID = ChordUtils.keyToChordID(args[1], m);
        var value = new String(Base64.decodeBase64(rpcClient.get(host, port, keyID)));
        System.out.println("OK");
        System.out.println(value);
    }

    private static void handleGetID(String[] args) throws Exception {
        if (args.length != 2) throw new Exception();
        var keyID = new BigInteger(args[1]);
        var value = new String(Base64.decodeBase64(rpcClient.get(host, port, keyID)));
        System.out.println("OK");
        System.out.println(value);
    }

    private static void handleDelete(String[] args) throws Exception {
        if (args.length != 2) throw new Exception();
        var keyID = ChordUtils.keyToChordID(args[1], m);
        rpcClient.delete(host, port, keyID);
        System.out.println("OK");
    }

    private static void handleExit(String[] args) throws Exception {
        if (args.length != 1) throw new Exception();
        System.out.println("Bye");
        System.exit(0);
    }

    private static void printSelectedNode() {
        System.out.println(String.format("Selected node: %s:%s", host, port));
    }
}