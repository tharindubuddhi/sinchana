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

	/**Sinchana DHT Configurations**/
	public static final int NUM_OF_MAX_OPENED_CONNECTION = 12;
	public static final int NODE_POOL_SIZE = 72;
	public static final int DEFAUILT_MESSAGE_LIFETIME = 1024;
	public static final int NUMBER_BASE = 16;
	public static final int INPUT_MESSAGE_BUFFER_SIZE = 4096;
	public static final int OUTPUT_MESSAGE_BUFFER_SIZE = 1024;
	public static final int NUMBER_OF_OUTPUT_MESSAGE_QUEUE_THREADS = 3;
	public static final int ROUTING_OPTIMIZATION_TIME_OUT = 10;	//Seconds
	public static final int NUM_OF_MAX_SEND_RETRIES = 1000;
	public static final int NUM_OF_MAX_CONNECT_RETRIES = 3;
	public static final int FAILED_REACCEPT_TIME_OUT = 10 * 1000;	//milliseconds
	public static final int JOIN_RETRY_TIME_OUT = 5; //Seconds
	public static final byte[] SERVICE_TAG = "-@-SINCHANA_SERVICE".getBytes();
	/**Test Configurations**/
	public static final boolean GUI_ON = false;
	public static final boolean USE_REMOTE_CACHE_SERVER = false;
	public static final boolean CLEAR_CACHE_SERVER = true;
	public static final int ROUND_TRIP_TIME = 0;
	public static final boolean DO_LOG = false;
}
