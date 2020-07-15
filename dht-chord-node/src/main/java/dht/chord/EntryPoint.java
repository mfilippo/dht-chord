package dht.chord;

import org.apache.commons.cli.*;

public class EntryPoint {
    private static final String M = "m";
    public static final int M_DEFAULT = 6;
    private static final String R = "r";
    private static final int R_DEFAULT = 3;
    private static final String K = "k";
    private static final int K_DEFAULT = 1;
    private static final String HOST = "h";
    private static final String HOST_DEFAULT = "localhost";
    private static final String PORT = "p";
    private static final int PORT_DEFAULT = 7777;
    private static final String BOOTSTRAP_ADDRESS = "b";

    public static void main(String[] args) {
        var parser = new DefaultParser();
        var options = prepareOptions();
        try {
            var commandLine = parser.parse(options, args, true);

            var m = M_DEFAULT;
            if (commandLine.hasOption(M)) {
                m = ((Number) commandLine.getParsedOptionValue(M)).intValue();
            }

            var r = R_DEFAULT;
            if (commandLine.hasOption(R)) {
                r = ((Number) commandLine.getParsedOptionValue(R)).intValue();
            }

            var k = K_DEFAULT;
            if (commandLine.hasOption(K)) {
                k = ((Number) commandLine.getParsedOptionValue(K)).intValue();
            }

            var host = HOST_DEFAULT;
            if (commandLine.hasOption(HOST)) {
                host = commandLine.getOptionValue(HOST);
            }

            var port = PORT_DEFAULT;
            if (commandLine.hasOption(PORT)) {
                port = ((Number) commandLine.getParsedOptionValue(PORT)).intValue();
            }

            if (commandLine.hasOption(BOOTSTRAP_ADDRESS)) {
                var bootstrapAddress = commandLine.getOptionValue(BOOTSTRAP_ADDRESS);
                var bootstrapHost = bootstrapAddress.split(":")[0];
                var bootstrapPort = Integer.parseInt(bootstrapAddress.split(":")[1]);

                // Start a normal node
                var node = new ChordNode(host, port, m, r, k);
                node.dhtJoin(bootstrapHost, bootstrapPort);
                node.start();
            } else {

                // Start a bootstrap node
                var node = new ChordNode(host, port, m, r, k);
                node.dhtCreate();
                node.start();
            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            new HelpFormatter().printHelp("dht-chord", options);
        }

    }

    private static Options prepareOptions() {
        var options = new Options();

        options.addOption(Option.builder(M)
                .desc(String.format("Size (in bits) of the Chord id space. (default=%s)", M_DEFAULT))
                .type(Number.class)
                .hasArg()
                .build());

        options.addOption(Option.builder(R)
                .desc(String.format("Size of the successor list. (default=%s)", R_DEFAULT))
                .type(Number.class)
                .hasArg()
                .build());

        options.addOption(Option.builder(K)
                .desc(String.format("Replication factor. Cannot be larger than r. (default=%s)", K_DEFAULT))
                .type(Number.class)
                .hasArg()
                .build());

        options.addOption(Option.builder(HOST)
                .desc(String.format("IP address of the current node. (default=%s)", HOST_DEFAULT))
                .hasArg()
                .build());

        options.addOption(Option.builder(PORT)
                .desc(String.format("Port number of the current node. (default=%s)", PORT_DEFAULT))
                .type(Number.class)
                .hasArg()
                .build());

        options.addOption(Option.builder(BOOTSTRAP_ADDRESS)
                .desc("Address of the bootstrap node as <host>:<port> (Needed for normal nodes)")
                .hasArg()
                .build());

        return options;
    }
}
