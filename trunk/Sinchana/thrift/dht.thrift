namespace java sinchana.thrift

struct Node {
  1: string serverId,
  2: string address
}

struct DataObject {
  1: string sourceID,
  2: string sourceAddress,
  3: string DataValue
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
    ACKNOWLEDGE_DATA,
    FAILURE_DATA,
    PUBLISH_SERVICE,
    GET_SERVICE,
    REMOVE_SERVICE,
    RESPONSE_SERVICE,
    ACKNOWLEDGE_SERVICE,
    FAILURE_SERVICE,
    REQUEST,
    RESPONSE,
    ERROR,
    JOIN, 
    DISCOVER_NEIGHBOURS, 
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
    12: optional map<string, Node> neighbourSet,
    13: optional map<string, Node> failedNodeSet,
    14: optional set<DataObject> dataSet,
    15: optional set<ServiceObject> serviceSet,
    16: optional string targetKey,
    17: optional i32 retryCount,
    18: optional i64 timeStamp,
    19: optional string dataValue,
    20: optional string serviceValue
}

service DHTServer {
    i32 transfer(1: Message message)
}
