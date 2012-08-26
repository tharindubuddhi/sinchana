/************************************************************************************

 * Sinchana Distributed Hash table 

 * Copyright (C) 2012 Sinchana DHT - Department of Computer Science &               
 * Engineering, University of Moratuwa, Sri Lanka. Permission is hereby 
 * granted, free of charge, to any person obtaining a copy of this 
 * software and associated documentation files of Sinchana DHT, to deal 
 * in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the 
 * Software is furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * Redistributions in binary form must reproduce the above copyright notice, 
 * this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.

 * Neither the name of University of Moratuwa, Department of Computer Science 
 * & Engineering nor the names of its contributors may be used to endorse or 
 * promote products derived from this software without specific prior written 
 * permission.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
 * SOFTWARE.                                                                    
 ************************************************************************************/
package sinchana;

/**
 *
 * @author S.A.H.S.Subasinghe
 */
public class CONFIGURATIONS {

	/**
	 * Number of the maximum opened connections in the connection pool.
	 */
	public static final int NUM_OF_MAX_OPENED_CONNECTION = 24;
	/**
	 * Number of maximum nodes kept in the connection pool. This includes opened, 
	 * closed and failed connections.
	 */
	public static final int NODE_POOL_SIZE = 240;
	/**
	 * Life time of a message. At each hop, a message's life time is reduced by one.
	 * When it reaches zero, message is discarded to prevent going on infinite loops.
	 */
	public static final int REQUEST_MESSAGE_LIFETIME = 160;
	/**
	 * Life time of a join message. At each hop, a message's life time is reduced by one.
	 * When it reaches zero, message is discarded to prevent going on infinite loops.
	 */
	public static final int JOIN_MESSAGE_LIFETIME = 1024;
	/**
	 * The size of the input messages buffer.
	 */
	public static final int INPUT_MESSAGE_BUFFER_SIZE = 8192;
	/**
	 * Number of threads that is used to send out going Thrift messages.
	 */
	public static final int NUMBER_OF_OUTPUT_MESSAGE_QUEUE_THREADS = 3;
	/**
	 * Time out for optimizing routing table in Seconds. If no update operation is performed 
	 * within this time out, optimize functions will be called.  
	 */
	public static final int ROUTING_OPTIMIZATION_TIME_OUT = 5;
	/**
	 * Number of max send tries. If the other node is busy and not accepting the 
	 * message, it will retry to send it until this limit is exceeded.
	 */
	public static final int NUM_OF_MAX_SEND_RETRIES = 3;
	/**
	 * Number of max connect tries. If a connection to other node is unable to establish, 
	 * it will retry to connect it until this limit is exceeded. If the limit is exceeded, 
	 * the node is considered 'failed'.
	 */
	public static final int NUM_OF_MAX_CONNECT_RETRIES = 3;
	/**
	 * Time out to re-accept a failed node in Seconds. If a failed node tried to contact this node,
	 * it will be rejected until this time out is exceeded.
	 */
	public static final long FAILED_REACCEPT_TIME_OUT = 120;
	/**
	 * Time out to retry the join procedure in Seconds. If the node is not accepted to the ring, 
	 * it will resend the 'JOIN' message after this time out.
	 */
	public static final int JOIN_RETRY_TIME_OUT = 10; //Seconds
	/**
	 * Number of maximum join retries. A <code>SinchanaJoinException</code> will be thrown 
	 * if this limit is exceeded
	 */
	public static final int MAX_JOIN_RETRIES = 6;
	/**
	 * Default time out for requests in Seconds. If the response for a client request is 
	 * not received within this time out, <code>SinchanaTimeOutException> will be thrown. 
	 */
	public static final long ASYNCHRONOUS_REQUEST_TIME_OUT = 120;
	/**
	 * Service tag which is used to separate service keys from data keys. This tag will be
	 * appended to each service key automatically, before publishing the service.
	 */
	public static final byte[] SERVICE_TAG = "-@-SINCHANA_SERVICE".getBytes();
	/**
	 * Error message to indicate a termination of a message because it's life time is expired. 
	 */
	public static final byte[] ERROR_MSG_LIFE_TIME_EXPIRED = "Messaage is terminated as lifetime expired!".getBytes();
	/**
	 * Error message to indicate a termination of a message because the maximum number of send retries is exceeded.
	 */
	public static final byte[] ERROR_MSG_MAX_SEND_RETRIES_EXCEEDED = "Messaage is terminated as maximum number of retries is exceeded!".getBytes();
	/**
	 * Error message to indicate that the response handler for the request is not found.
	 */
	public static final byte[] ERROR_MSG_RESPONSE_HANDLER_NOT_FOUND = "Response handler not found".getBytes();
	/**
	 * Error message to indicate that the service handler for the service invocation is not found.
	 */
	public static final byte[] ERROR_MSG_SERVICE_HANDLER_NOT_FOUND = "Service handler not found".getBytes();
	/**
	 * Error message to indicate that the data store handler for the data store operation is not found.
	 */
	public static final byte[] ERROR_MSG_DATA_STORE_HANDLER_NOT_FOUND = "Data handler not found".getBytes();
	/**
	 * Error message to indicate a join failure because of the join re-acceptance time out is not over.
	 */
	public static final byte[] ERROR_MSG_JOIN_REACCEPTANCE = "You have to wait...".getBytes();
	/**
	 * Error message to indicate a termination of a request because the time out is exceeded.
	 */
	public static final byte[] ERROR_MSG_TIMED_OUT = "Timed out!".getBytes();
	/**
	 * Error message to indicate a join failure because of the maximum number of join retries is exceeded.
	 */
	public static final String ERROR_MSG_JOIN_FAILED = "Join failed. Maximum number of retries exceeded!";
	/**
	 * Test ring separator.
	 */
	public static final String TEST_RING_SEPARATOR = " > ";
}
