namespace java sinchana.thrift

struct Node {
  1: binary serverId,
  2: string address
}

enum MessageType {
    STORE_DATA,
    DELETE_DATA,
    GET_DATA,
    RESPONSE_DATA,
    ACKNOWLEDGE_DATA_STORE,
    ACKNOWLEDGE_DATA_REMOVE,
    GET_SERVICE,
    RESPONSE_SERVICE,
    REQUEST,
    RESPONSE,
    ERROR,
    JOIN,
    DISCOVER_NEIGHBORS,
    VERIFY_RING,
    TEST_RING
}

struct Message {
    1: optional i64 id,
    2: required Node source,
    3: required MessageType type,
    4: required i32 lifetime,
    5: optional Node destination,
    6: optional Node station,
    7: optional binary destinationId,
    8: optional binary key,
    9: optional binary data,
    10: optional Node predecessor,
    11: optional Node successor,
    12: optional set<Node> neighbourSet,
    13: optional set<Node> failedNodeSet,
    14: optional i32 retryCount,
    15: optional i64 timeStamp,
    16: optional bool success,
    17: optional bool responseExpected,
    18: optional string error
}

service DHTServer {
    i32 transfer(1: Message message);
    binary discoverService(1: binary serviceKey);
    binary getService(1: binary reference, 2: binary data);
    bool publishData(1: binary dataKey, 2: binary data);
    bool removeData(1: binary dataKey);
    binary getData(1: binary dataKey);
    binary request(1: binary destination, 2: binary message);
    void ping();
}