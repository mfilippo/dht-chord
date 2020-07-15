DHT Chord
---------

Work In Progress...

---

```bash
$ javac -version
javac 11.0.4

$ mvn -version
Apache Maven 3.6.1 (d66c9c0b3152b2e69ee9bac180bb8fcc8e6af555; 2019-04-04T21:00:29+02:00)
Maven home: /opt/apache-maven-3.6.1
Java version: 11.0.4, vendor: Ubuntu, runtime: /usr/lib/jvm/java-11-openjdk-amd64
Default locale: en_US, platform encoding: UTF-8
OS name: "linux", version: "4.15.0-88-generic", arch: "amd64", family: "unix"
```

Build:

```bash
mvn clean package
```

Show options:

```bash
java -jar dht-chord-node/target/dht-chord-node-0.0.1-SNAPSHOT-jar-with-dependencies.jar -h
```

```
usage: dht-chord
 -b <arg>   Address of the bootstrap node as <host>:<port> (Needed for normal nodes)
 -h <arg>   IP address of the current node. (default=localhost)
 -k <arg>   Replication factor. Cannot be larger than r. (default=1)
 -m <arg>   Size (in bits) of the Chord id space. (default=6)
 -p <arg>   Port number of the current node. (default=7777)
 -r <arg>   Size of the successor list. (default=3)
```

Run bootstrap node:

```bash
java -jar dht-chord-node/target/dht-chord-node-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```

Run node to join:

```bash
java -jar dht-chord-node/target/dht-chord-node-0.0.1-SNAPSHOT-jar-with-dependencies.jar -p 7778 -b localhost:7777
```