package dht.chord;

public class EntryPointTest {

    public static void main(String[] args) {
        var m = 32;
        var r = 3;
        var k = 1;

        var node = new ChordNode("localhost", 7777, m, r, k);
        node.dhtCreate();
        node.start();

        var node1 = new ChordNode("localhost", 7778, m, r, k);
        node1.dhtJoin("localhost", 7777);
        node1.start();

        var node2 = new ChordNode("localhost", 7780, m, r, k);
        node2.dhtJoin("localhost", 7777);
        node2.start();
    }
}
