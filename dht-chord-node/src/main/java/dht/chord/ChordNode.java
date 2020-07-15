package dht.chord;

import dht.chord.exceptions.NodeFailException;
import dht.chord.rpc.RPCClient;
import dht.chord.rpc.RPCServer;
import dht.chord.utils.ChordUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ChordNode extends Thread {
    private String host;
    private int port;

    private int m;
    private int r;
    private int k;
    private int next;
    private BigInteger chordID;
    private String nodeAddress;

    private List<String> finger;
    private AtomicReference<String> predecessorAddress;
    private List<String> successorList;
    private String successorListHash;
    private Map<BigInteger, String> db;
    private Map<BigInteger, String> dbReplicas;

    private RPCClient rpcClient;

    private final Logger logger = LogManager.getLogger(ChordNode.class);

    ChordNode(String host, int port, int m, int r, int k) {
        this.host = host;
        this.port = port;
        this.m = m;
        this.r = r;
        this.k = k > r ? r : k;
        this.next = 0;
        this.chordID = ChordUtils.hostPortToChordID(host, port, m);
        this.nodeAddress = ChordUtils.makeAddress(this.chordID, host, port);
        this.finger = Collections.synchronizedList(new ArrayList<>(m));
        for (int i = 0; i < m; i++) this.finger.add(null);
        this.predecessorAddress = new AtomicReference<>(null);
        this.successorList = Collections.synchronizedList(new ArrayList<>(this.r));
        this.successorListHash = DigestUtils.sha1Hex(this.successorList.toString());
        this.db = new ConcurrentHashMap<>();
        this.dbReplicas = new ConcurrentHashMap<>();
        this.rpcClient = new RPCClient();
    }

    // ========================================
    // User interface
    // ========================================

    public void dhtPut(BigInteger id, String value) {
        logger.debug("PUT {} {}", id, value);

        var attempts = 20;
        while (attempts > 0) {
            try {
                var successorAddress = this.dhtFindSuccessor(id);
                if (successorAddress.equals(ChordUtils.makeAddress(this.chordID, this.host, this.port))) {
                    this.dhtStore(id, value);
                    this.dhtStoreReplicas(id, value);
                    break;
                } else {
                    this.rpcClient.put(
                            ChordUtils.extractHost(successorAddress),
                            ChordUtils.extractPort(successorAddress),
                            id,
                            value
                    );
                    break;
                }
            } catch (NodeFailException e) {
                logger.warn("Node failed: {}", e.getMessage());
                logger.info("Waiting for stabilization. Retrying in 5 seconds");
                attempts = attempts - 1;
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e2) {
                    logger.error(e2.getMessage());
                }
            }
        }
        if (attempts == 0) {
            logger.error("Giving up");
        }
    }

    public String dhtGet(BigInteger id) {
        logger.info("GET {}", id);

        var attempts = 20;
        while (attempts > 0) {

            try {
                var successorAddress = this.dhtFindSuccessor(id);
                if (successorAddress.equals(ChordUtils.makeAddress(this.chordID, this.host, this.port))) {
                    return this.db.get(id);
                } else {
                    return this.rpcClient.get(
                            ChordUtils.extractHost(successorAddress),
                            ChordUtils.extractPort(successorAddress),
                            id
                    );
                }
            } catch (NodeFailException e) {
                logger.warn("Node failed: {}", e.getMessage());
                logger.info("Waiting for stabilization. Retrying in 5 seconds");
                attempts = attempts - 1;
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e2) {
                    logger.error(e2.getMessage());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        logger.error("Giving up");
        return null;
    }

    public void dhtDelete(BigInteger id) {
        logger.debug("DELETE {}", id);

        var attempts = 20;
        while (attempts > 0) {
            try {
                var successorAddress = this.dhtFindSuccessor(id);
                if (successorAddress.equals(ChordUtils.makeAddress(this.chordID, this.host, this.port))) {
                    this.db.remove(id);
                    break;
                } else {
                    this.rpcClient.delete(
                            ChordUtils.extractHost(successorAddress),
                            ChordUtils.extractPort(successorAddress),
                            id
                    );
                    break;
                }
            } catch (NodeFailException e) {
                logger.warn("Node failed: {}", e.getMessage());
                logger.info("Waiting for stabilization. Retrying in 5 seconds");
                attempts = attempts - 1;
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e2) {
                    logger.error(e2.getMessage());
                }
            }
        }
        if (attempts == 0) {
            logger.error("Giving up");
        }
    }

    // ========================================
    // DHT interface
    // ========================================

    public Map<BigInteger, String> dhtGetKeysToTransfer(BigInteger id) {
        logger.debug("Transfer to {}", id);

        var toTransfer = new HashMap<BigInteger, String>();
        var keys = this.db.keySet();
        for (var key : keys) {
            if (!ChordUtils.isInIntervalInclusive(id, key, this.chordID, this.m)) {
                toTransfer.put(key, this.db.get(key));
                this.db.remove(key);
            }
        }
        return toTransfer;
    }

    public String dhtFindSuccessor(BigInteger id) {
        logger.debug("Finding successor of id: {}", id);

        // If this.pred.id < id <= this.id : this is the successor
        if (this.predecessorAddress.get() != null && ChordUtils.isInIntervalInclusive(
                ChordUtils.extractChordID(this.predecessorAddress.get()),
                id,
                this.chordID,
                this.m
        )) {
            return ChordUtils.makeAddress(this.chordID, this.host, this.port);
        }

        // If this.id < id < this.succ.id : this.succ is the successor
        if (ChordUtils.isInIntervalInclusive(
                this.chordID,
                id,
                ChordUtils.extractChordID(this.successorList.get(0)),
                this.m
        )) {
            return this.successorList.get(0);
        }

        // Query closest preceding node for successor
        var closestPrecedingAddress = this.dhtClosestPrecedingNode(id);
        try {
            return this.rpcClient.findSuccessor(
                    ChordUtils.extractHost(closestPrecedingAddress),
                    ChordUtils.extractPort(closestPrecedingAddress),
                    id
            );
        } catch (NodeFailException e) {
            logger.warn("Node failed: {}", e.getMessage());
            return null;
        }
    }

    public void dhtNotify(String nodeAddress) {
        logger.debug("Notified by: {}", nodeAddress);

        if (this.predecessorAddress.get() == null || ChordUtils.isInIntervalExclusive(
                ChordUtils.extractChordID(this.predecessorAddress.get()),
                ChordUtils.extractChordID(nodeAddress),
                this.chordID,
                this.m
        )) {
            this.predecessorAddress.set(nodeAddress);
        }
    }

    public void dhtStore(BigInteger id, String value) {
        logger.debug("Store: {} {}", id, value);

        if (this.predecessorAddress.get() != null && !ChordUtils.isInIntervalInclusive(
                ChordUtils.extractChordID(this.predecessorAddress.get()),
                id,
                this.chordID,
                this.m
        )) {
            this.dbReplicas.put(id, value);
        } else {
            this.db.put(id, value);
        }
    }

    public void dhtStoreReplicas(BigInteger id, String value) {
        logger.debug("Storing replicas for id: {} (k={})", id, this.k);

        var attempts = 20;
        while (attempts > 0) {
            try {
                for (int i = 0; i < this.k; i++) {
                    var nodeAddress = this.successorList.get(0);
                    this.rpcClient.store(
                            ChordUtils.extractHost(nodeAddress),
                            ChordUtils.extractPort(nodeAddress),
                            id,
                            value
                    );
                }
                break;

            } catch (NodeFailException e) {
                logger.warn("Node failed: {}", e.getMessage());
                logger.info("Waiting for stabilization. Retrying in 5 seconds");
                attempts = attempts - 1;
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e2) {
                    logger.error(e2.getMessage());
                }
            }
        }
        if (attempts == 0) {
            logger.error("Giving up");
        }
    }

    public void dhtCreate() {
        logger.info("Creating a new Chord ring");

        // Initialize successor list
        for (int i = 0; i < this.r; i++)
            this.successorList.add(ChordUtils.makeAddress(this.chordID, this.host, this.port));

        // Set predecessor to null
        this.predecessorAddress = new AtomicReference<>(null);
    }

    public void dhtJoin(String bootstrapHost, int bootstrapPort) {
        logger.info("Joining a Chord ring [bootstrapped by: {}:{}]", bootstrapHost, bootstrapPort);

        while (true) {
            try {

                // Query bootstrap node for successor of this node
                var newSuccessorAddress = this.rpcClient.findSuccessor(
                        bootstrapHost,
                        bootstrapPort,
                        this.chordID);

                // Query new successor for successor list
                var newSuccessorList = this.rpcClient.getSuccessorList(
                        ChordUtils.extractHost(newSuccessorAddress),
                        ChordUtils.extractPort(newSuccessorAddress));

                // Update successor list
                this.successorList.add(newSuccessorAddress);
                newSuccessorList.remove(newSuccessorList.size() - 1);
                this.successorList.addAll(newSuccessorList);

                // Set predecessor to null
                this.predecessorAddress = new AtomicReference<>(null);

                // Get and store keys from successor
                var map = this.rpcClient.transfer(
                        ChordUtils.extractHost(newSuccessorAddress),
                        ChordUtils.extractPort(newSuccessorAddress),
                        ChordUtils.hostPortToChordID(this.host, this.port, this.m));
                for (var entry : map.entrySet()) {
                    this.dhtStore(entry.getKey(), entry.getValue());
                }

                break;

            } catch (NodeFailException e1) {
                logger.warn("Node failed: {}", e1.getMessage());
                logger.info("Retrying in 5 seconds");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e2) {
                    logger.error(e2.getMessage());
                }
            }
        }
    }

    private String dhtClosestPrecedingNode(BigInteger id) {
        logger.debug("Finding closest preceding node of id: {}", id);

        for (int i = this.m - 1; i >= 0; i--) {
            var nodeAddress = this.finger.get(i);

            // If this.id < node.id < id : node is the closest node preceding id
            if (nodeAddress != null && ChordUtils.isInIntervalExclusive(
                    this.chordID,
                    ChordUtils.extractChordID(nodeAddress),
                    id,
                    this.m
            )) {
                return nodeAddress;
            }
        }
        return ChordUtils.makeAddress(this.chordID, this.host, this.port);
    }

    // ========================================
    // DHT routines
    // ========================================

    private void dhtStabilize() {
        logger.debug("Stabilizing");

        try {
            var successorAddress = this.successorList.get(0);

            // Get successor's predecessor
            var successorPredecessorAddress = this.rpcClient.getPredecessor(
                    ChordUtils.extractHost(successorAddress),
                    ChordUtils.extractPort(successorAddress));

            // If this.id < this.succ.pred.id < this.succ : succ.pred is the new succ
            if (successorPredecessorAddress != null && ChordUtils.isInIntervalExclusive(
                    this.chordID,
                    ChordUtils.extractChordID(successorPredecessorAddress),
                    ChordUtils.extractChordID(successorAddress),
                    this.m
            )) {
                successorAddress = successorPredecessorAddress;
            }

            // Get successor's successor list
            var successorSuccessorList = this.rpcClient.getSuccessorList(
                    ChordUtils.extractHost(successorAddress),
                    ChordUtils.extractPort(successorAddress));

            // Update successor list
            synchronized (this.successorList) {
                this.successorList.set(0, successorAddress);
                for (int i = 1; i < this.r; i++) {
                    this.successorList.set(i, successorSuccessorList.get(i - 1));
                }
            }

            // Notify successor
            this.rpcClient.notify(
                    ChordUtils.extractHost(successorAddress),
                    ChordUtils.extractPort(successorAddress),
                    ChordUtils.makeAddress(this.chordID, this.host, this.port)
            );

        } catch (NodeFailException e) {
            logger.warn("Node failed: {}", e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error: {}", e.getMessage());
        }
    }

    private void dhtCheckPredecessor() {
        logger.debug("Checking predecessor");

        var address = this.predecessorAddress.get();
        if (address != null) {
            try {
                this.rpcClient.ping(
                        ChordUtils.extractHost(address),
                        ChordUtils.extractPort(address)
                );
            } catch (NodeFailException e) {
                logger.warn("Predecessor has failed: {}", e.getMessage());
                this.predecessorAddress.set(null);
            }
        }
    }

    private void dhtCheckSuccessor() {
        logger.debug("Checking successor");

        var address = this.successorList.get(0);
        if (address != null) {
            try {
                this.rpcClient.ping(
                        ChordUtils.extractHost(address),
                        ChordUtils.extractPort(address)
                );
            } catch (NodeFailException e) {
                logger.warn("Successor has failed: {}", e.getMessage());

                // Replace successor with next successor
                synchronized (this.successorList) {
                    if (this.successorList.size() > 1) {
                        var newSuccessorAddress = this.successorList.get(1);
                        this.successorList.set(0, newSuccessorAddress);
                    }
                }
            }
        }
    }

    private void dhtFixFingers() {
        logger.debug("Fixing fingers (next={})", this.next);

        // fingerId = (id + 2^next) mod 2^m
        var fingerId = this.chordID.add(BigInteger.valueOf(2).pow(this.next)).mod(BigInteger.valueOf(2).pow(this.m));
        this.finger.set(this.next, this.dhtFindSuccessor(fingerId));
        this.next = (this.next + 1) % this.m;
    }

    private void dhtFixSuccessorList() {
        logger.debug("Fixing successor list");

        var successorAddress = this.successorList.get(0);
        try {

            // Get successor's successor list
            var successorSuccessorList = this.rpcClient.getSuccessorList(
                    ChordUtils.extractHost(successorAddress),
                    ChordUtils.extractPort(successorAddress));

            // Update successor list
            synchronized (this.successorList) {
                this.successorList.set(0, successorAddress);
                for (int i = 1; i < this.r; i++) {
                    this.successorList.set(i, successorSuccessorList.get(i - 1));
                }
            }

        } catch (NodeFailException e1) {
            logger.warn("Fixing successor list failed: {}", e1.getMessage());
        }
    }

    private void dhtFixReplication() {
        logger.debug("Fixing replication");

        // Clean DB from replicas
        for (var key : this.db.keySet()) {
            if (this.predecessorAddress.get() != null && !ChordUtils.isInIntervalInclusive(
                    ChordUtils.extractChordID(this.predecessorAddress.get()),
                    key,
                    this.chordID,
                    this.m
            )) {
                this.dbReplicas.put(key, this.db.get(key));
                this.db.remove(key);
            }
        }

        // Put replicas in db if the node is now responsible for those keys
        for (var key : this.dbReplicas.keySet()) {
            if (this.predecessorAddress.get() != null && ChordUtils.isInIntervalInclusive(
                    ChordUtils.extractChordID(this.predecessorAddress.get()),
                    key,
                    this.chordID,
                    this.m
            )) {
                this.db.put(key, this.dbReplicas.get(key));
                this.dbReplicas.remove(key);
            }
        }

        // Re-replicate data when successor list changes
        var newSuccessorListHash = DigestUtils.sha1Hex(this.successorList.toString());
        if (!newSuccessorListHash.equals(this.successorListHash)) {
            logger.info("Successor list has changed");
            this.successorListHash = newSuccessorListHash;
            for (var entry : this.db.entrySet()) {
                this.dhtStoreReplicas(entry.getKey(), entry.getValue());
            }
            for (var entry : this.dbReplicas.entrySet()) {
                this.dhtStoreReplicas(entry.getKey(), entry.getValue());
            }
        }
    }

    // ========================================
    // Getters / Setters
    // ========================================

    public String getPredecessorAddress() {
        return predecessorAddress.get();
    }

    public List<String> getSuccessorList() {
        return successorList;
    }

    // ========================================
    // Thread interface
    // ========================================

    @Override
    public void run() {
        logger.info("Started node {}:{} [chordID={}]", this.host, this.port, this.chordID);

        var serverExecutor = Executors.newSingleThreadExecutor();

        // Run RPC server routine
        serverExecutor.execute(new RPCServer(this.port, 10, this));

        var scheduledExecutor = Executors.newScheduledThreadPool(6);

        var routineDelay = 1;

        // Run stabilize routine
        // scheduledExecutor.scheduleWithFixedDelay(this::dhtStabilize, 1, 2, TimeUnit.SECONDS);
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                this.dhtStabilize();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 1, routineDelay, TimeUnit.SECONDS);

        // Run fix fingers routine
        // scheduledExecutor.scheduleWithFixedDelay(this::dhtFixFingers, 2, 2, TimeUnit.SECONDS);
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                this.dhtFixFingers();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 2, routineDelay, TimeUnit.SECONDS);

        // Run check predecessor routine
        // scheduledExecutor.scheduleWithFixedDelay(this::dhtCheckPredecessor, 3, 2, TimeUnit.SECONDS);
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                this.dhtCheckPredecessor();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 3, routineDelay, TimeUnit.SECONDS);

        // Run check successor routine
        // scheduledExecutor.scheduleWithFixedDelay(this::dhtCheckSuccessor, 4, 2, TimeUnit.SECONDS);
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                this.dhtCheckSuccessor();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 4, routineDelay, TimeUnit.SECONDS);

        // Run fix successor list routine
        // scheduledExecutor.scheduleWithFixedDelay(this::dhtFixSuccessorList, 5, 2, TimeUnit.SECONDS);
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                this.dhtFixSuccessorList();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 5, routineDelay, TimeUnit.SECONDS);

        // Run fix replication routine
        // scheduledExecutor.scheduleWithFixedDelay(this::dhtFixReplication, 6, 10, TimeUnit.SECONDS);
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                this.dhtFixReplication();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 6, 5, TimeUnit.SECONDS);

        // dump
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                this.dump();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 2, TimeUnit.SECONDS);
    }

    // ========================================
    // Utils
    // ========================================

    private void dump() {
        var sep = "=".repeat(100);
        var header = String.format("[NODE %s@%s:%s]", this.chordID, this.host, this.port);
        var line = "-".repeat(header.length());
        var pred = String.format("Predecessor: %s", this.predecessorAddress);
        var succ = String.format("Successors: %s", this.successorList);
        var fingers = String.format("Fingers:\n%s",
                IntStream.range(0, this.m).mapToObj(i -> {
                    var fingerIndex = this.chordID.add(BigInteger.valueOf(2).pow(i)).mod(BigInteger.valueOf(2).pow(this.m));
                    var fingerAddress = this.finger.get(i);
                    return String.format("|%3d --> %s |", fingerIndex, fingerAddress);
                }).collect(Collectors.joining("\n"))
        );
        var store = String.format("DB: %s", this.db.keySet());
        var storeR = String.format("DB Replicas: %s", this.dbReplicas.keySet());
        System.out.println();
        System.out.println(String.join("\n", sep, header, line, pred, succ, fingers, store, storeR, sep));
        System.out.println();
    }

    @Override
    public String toString() {
        return String.format("[Node %s]", this.chordID);
    }
}
