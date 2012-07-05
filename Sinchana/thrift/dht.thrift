namespace java dhtserverclient.thrift

struct Node {
  1: i32 serverId,
  2: string address,
  3: i32 portId
}

enum MessageType {
    GET, 
    JOIN, 
    DISCOVER_NEIGHBOURS, 
    FIND_SUCCESSOR, 
    ERROR, 
    ACCEPT,
    TEST_RING
}

struct Message {
    1: optional i32 id,
    2: required Node source,
    3: required MessageType type,
    4: required i32 lifetime,
    5: optional Node target,
    6: optional Node station,
    7: optional string message,
    8: optional Node predecessor,
    9: optional Node successor,
    10: optional i32 startOfRange,
    11: optional i32 endOfRange,
    12: optional set<Node> neighbourSet,
    13: optional i32 targetKey
}

service DHTServer {
    bool transfer(1: Message message)
}
