/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

/**
 *
 * @author Hiru
 */
public class CONFIGURATIONS {

	/**
	 * 
	 */
	public static final int NUM_OF_MAX_OPENED_CONNECTION = 24;
	/**
	 * 
	 */
	public static final int NODE_POOL_SIZE = 240;
	/**
	 * 
	 */
	public static final int REQUEST_MESSAGE_LIFETIME = 120;
	/**
	 * 
	 */
	public static final int JOIN_MESSAGE_LIFETIME = 1024;
	/**
	 * 
	 */
	public static final int NUMBER_BASE = 16;
	/**
	 * 
	 */
	public static final int INPUT_MESSAGE_BUFFER_SIZE = 8192;
	/**
	 * 
	 */
	public static final int MESSAGE_BUFFER_LIMIT_RATIO = 0; //%
	/**
	 * 
	 */
	public static final int NUMBER_OF_OUTPUT_MESSAGE_QUEUE_THREADS = 3;
	/**
	 * 
	 */
	public static final int ROUTING_OPTIMIZATION_TIME_OUT = 5;	//Seconds
	/**
	 * 
	 */
	public static final int NUM_OF_MAX_SEND_RETRIES = 3;
	/**
	 * 
	 */
	public static final int NUM_OF_MAX_CONNECT_RETRIES = 3;
	/**
	 * 
	 */
	public static final long FAILED_REACCEPT_TIME_OUT = 120 * 1000;	//milliseconds
	/**
	 * 
	 */
	public static final int JOIN_RETRY_TIME_OUT = 10; //Seconds
	/**
	 * 
	 */
	public static final int MAX_JOIN_RETRIES = 6;
	/**
	 * 
	 */
	public static final int CHOKE_LIMIT = 200; //Number of request per second. Roughly, = (system throughput / number of testing nodes).
	/**
	 * 
	 */
	public static final long ASYNCHRONOUS_REQUEST_TIME_OUT = 60 * 1000; //milliseconds
	/**
	 * 
	 */
	public static final byte[] SERVICE_TAG = "-@-SINCHANA_SERVICE".getBytes();
}
