/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package sinchana.thrift;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum MessageType implements org.apache.thrift.TEnum {
  STORE_DATA(0),
  DELETE_DATA(1),
  GET_DATA(2),
  RESPONSE_DATA(3),
  ACKNOWLEDGE_DATA_STORE(4),
  ACKNOWLEDGE_DATA_REMOVE(5),
  GET_SERVICE(6),
  RESPONSE_SERVICE(7),
  REQUEST(8),
  RESPONSE(9),
  ERROR(10),
  JOIN(11),
  DISCOVER_NEIGHBORS(12),
  FIND_SUCCESSOR(13),
  VERIFY_RING(14),
  TEST_RING(15);

  private final int value;

  private MessageType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static MessageType findByValue(int value) { 
    switch (value) {
      case 0:
        return STORE_DATA;
      case 1:
        return DELETE_DATA;
      case 2:
        return GET_DATA;
      case 3:
        return RESPONSE_DATA;
      case 4:
        return ACKNOWLEDGE_DATA_STORE;
      case 5:
        return ACKNOWLEDGE_DATA_REMOVE;
      case 6:
        return GET_SERVICE;
      case 7:
        return RESPONSE_SERVICE;
      case 8:
        return REQUEST;
      case 9:
        return RESPONSE;
      case 10:
        return ERROR;
      case 11:
        return JOIN;
      case 12:
        return DISCOVER_NEIGHBORS;
      case 13:
        return FIND_SUCCESSOR;
      case 14:
        return VERIFY_RING;
      case 15:
        return TEST_RING;
      default:
        return null;
    }
  }
}