namespace java sinchana.thrift

struct Node {
  1: binary serverId,
  2: string address
}

struct Response {
  1: required bool success,
  2: optional binary data
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
    TEST_RING
}

struct Message {
    1: required MessageType type,
    2: required Node source,
    3: required i32 lifetime,
    4: optional i64 id,
    5: optional Node destination,
    6: optional Node station,
    7: optional binary destinationId,
    8: optional binary key,
    9: optional binary data,
    10: optional set<Node> neighbourSet,
    11: optional set<Node> failedNodeSet,
    12: optional bool success,
    13: optional bool responseExpected,
    14: optional binary error,
    15: optional i32 retryCount,
    16: optional i64 timeStamp,
    17: optional bool routedViaPredecessors    
}

service DHTServer {
    i32 transfer(1: Message message);
    Response discoverService(1: binary serviceKey);
    Response invokeService(1: binary reference, 2: binary data);
    Response publishData(1: binary dataKey, 2: binary data);
    Response removeData(1: binary dataKey);
    Response getData(1: binary dataKey);
    Response sendRequest(1: binary destination, 2: binary message);
    void ping();
}