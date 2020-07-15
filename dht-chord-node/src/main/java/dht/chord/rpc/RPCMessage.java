package dht.chord.rpc;

public enum RPCMessage {
    ERROR,
    OK,
    STORE,              // STORE <key> <value>
    TRANSFER,           // TRANSFER <chord_id> (returns <key:value> ... <key:value>)
    SHUT_DOWN,          // No args
    PUT,                // PUT <key> <value>
    GET,                // GET <key> (returns <value>)
    DELETE,             // DELETE <key>
    FIND_SUCCESSOR,     // FIND_SUCCESSOR <chord_id> (returns <chord_id>@<host>:<port>)
    GET_SUCCESSOR_LIST, // No args (returns <chord_id>@<host>:<port> ... <chord_id>@<host>:<port>)
    NOTIFY,             // NOTIFY <chord_id>@<host>:<port>
    GET_PREDECESSOR,    // No args (returns <chord_id>@<host>:<port>)
    PING,               // No args (returns PONG)
    PONG,               // No args
    NULL                // No args (Will be transformed to null)
}