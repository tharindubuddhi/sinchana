namespace java sinchana.thrift

struct Node {
  1: string serverId,
  2: string address
}

struct DataObject {
  1: optional string sourceID,
  2: optional string sourceAddress,
  3: optional string dataValue,
  4: optional string dataKey
}

struct ServiceObject {
  1: string sourceID,
  2: string sourceAddress,
  3: string serviceName
}

enum MessageType {
    STORE_DATA,
    DELETE_DATA,
    GET_DATA,
    RESPONSE_DATA,
    ACKNOWLEDGE_DATA_STORE,
    ACKNOWLEDGE_DATA_REMOVE,
    PUBLISH_SERVICE,
    GET_SERVICE,
    REMOVE_SERVICE,
    RESPONSE_SERVICE,
    ACKNOWLEDGE_SERVICE_PUBLISH,
    ACKNOWLEDGE_SERVICE_REMOVE,
    REQUEST,
    RESPONSE,
    ERROR,
    JOIN,
    DISCOVER_NEIGHBORS, 
    FIND_SUCCESSOR, 
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
    7: optional string message,
    8: optional Node predecessor,
    9: optional Node successor,
    10: optional string startOfRange,
    11: optional string endOfRange,
    12: optional set<Node> neighbourSet,
    13: optional set<Node> failedNodeSet,
    14: optional set<DataObject> dataSet,
    15: optional set<ServiceObject> serviceSet,
    16: optional string targetKey,
    17: optional i32 retryCount,
    18: optional i64 timeStamp,
    19: optional string dataValue,
    20: optional string serviceValue,
    21: optional bool success
}

service DHTServer {
    i32 transfer(1: Message message);
    void ping();
}

service SinchanaClient {
    bool publishService(1: ServiceObject services);
    bool removeService(1: string serviceKey);
    set<ServiceObject> getService(1: string serviceKey);
    bool publishData(1: DataObject data);
    bool removeData(1: string dataKey);
    set<DataObject> getData(1: string dataKey);
    string request(1: string destination);
}
