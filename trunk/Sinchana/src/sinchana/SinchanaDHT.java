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

import java.util.concurrent.TimeUnit;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import sinchana.dataStore.SinchanaDataCallback;
import sinchana.dataStore.SinchanaDataStoreInterface;
import sinchana.exceptions.SinchanaInvalidArgumentException;
import sinchana.exceptions.SinchanaTimeOutException;
import sinchana.service.SinchanaServiceCallback;
import sinchana.service.SinchanaServiceInterface;

/**
 *
 * @author S.A.H.S.Subasinghe
 */
public interface SinchanaDHT {

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
	public static final long ASYNCHRONOUS_REQUEST_TIME_OUT = 10;
	/**
	 * Test ring separator.
	 */
	public static final String TEST_RING_SEPARATOR = " > ";
	/**
	 * Chord routing algorithm
	 */
	public static final String CHORD = "CHORD";
	/**
	 * Tapestry routing algorithm
	 */
	public static final String TAPESTRY = "TAPESTRY";
	/**
	 * Pastry routing algorithm
	 */
	public static final String PASTRY = "PASTRY";
	/**
	 * Error message to indicate a join failure because of the maximum number of join retries is exceeded.
	 */
	public static final String ERROR_MSG_JOIN_FAILED = "Join failed. Maximum number of retries exceeded!";
	/**
	 * Error message to indicate the service which is required is not found (on service remove).
	 */
	public static final String ERROR_MSG_NO_SUCH_SERVICE_FOUND = "No such service found in this server!";
	/**
	 * Error message to indicate an invalid address length. The length of an address should be exactly 20 bytes.
	 */
	public static final String ERROR_MSG_INVALID_ADDRESS_LENGTH = "Destination address should be exactly 20 byte length.";
	/**
	 * Error message to indicate null arguments.
	 */
	public static final String ERROR_MSG_NULL_ARGUMENTS = "Arguments cannot be null!";
	/**
	 * Error message to indicate initializing with an invalid routing algorithm.
	 */
	public static final String ERROR_MSG_INVALID_ROUTING_ALGORITHM = "Invalid routing algorithm! Use '" 
			+ CHORD + "', '" + PASTRY + "' or '" + TAPESTRY + ".";
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
	 * Service tag which is used to separate service keys from data keys. This tag will be
	 * appended to each service key automatically, before publishing the service.
	 */
	public static final byte[] SERVICE_TAG = "-@-SINCHANA_SERVICE".getBytes();
	
	/**
	 * Starts the server. Thread is blocked until the Thrift server is ready.
	 * @throws TTransportException if the Thrift server has encountered a problem while starting on the given address and the port id.
	 * @throws InterruptedException if the thread is interrupted before the thrift server is ready.
	 */
	public abstract void startServer() throws TTransportException, InterruptedException;
	/**
	 * Stops the server.
	 */
	public void stopServer();
	/**
	 * Returns whether the server is running or not.
	 * @return <code>true</code> if the Thrift server is serving. <code>false</code> otherwise.
	 */
	public boolean isRunning();
	/**
	 * Join the ring by connecting to the Sinchana server in the given address. Thread is blocked until the join is completed.
	 * @param remoteNodeAddress address of the remote Sinchana server in [host name]:[port id] format. Passing <code>null</code> or
	 * the same address which is used to initialize the server will cause the server to start alone as a root node.
	 * @throws TException if a problem encountered while connecting to the remote server.
	 * @throws InterruptedException if the thread is interrupted before the join is completed.
	 */
	public void join(String remoteNodeAddress) throws TException, InterruptedException;
	/**
	 * Initiates the ring as the root node.
	 */
	public void join();
	/**
	 * Returns whether this server has joined to a ring or not.
	 * @return <code>true</code> if the server has joined a ring. <code>false</code> otherwise.
	 */
	public boolean isJoined();
	/**
	 * Prints routing table information in console.
	 */
	public void printTableInfo();
	/**
	 * Returns server id in hex string format.
	 * @return server id.
	 */
	public String getServerIdAsString();
	/**
	 * 
	 * @param sinchanaRequestCallback
	 */
	public void registerSinchanaRequestCallback(SinchanaRequestCallback sinchanaRequestCallback);
	/**
	 * 
	 * @param sinchanaTestInterface
	 */
	public void registerSinchanaTestInterface(SinchanaTestInterface sinchanaTestInterface);
	/**
	 * 
	 * @param sinchanaDataStoreInterface
	 */
	public void registerSinchanaStoreInterface(SinchanaDataStoreInterface sinchanaDataStoreInterface);
	/**
	 * Tests the ring. If the ring is completed, it will be printed on console.
	 */
	public void testRing();
	/**
	 * Sends a synchronous request to the given destination. It will be received by the successor of that destination. 
	 * The thread is blocked until the response is received or time out occurs. 
	 * @param destination Destination id to send request. This should be exactly 20 byte length.
	 * @param message Message to send to the destination.
	 * @return Response from the destination.
	 * @throws SinchanaTimeOutException If the response is not arrived within the time out.
	 * @throws InterruptedException If the thread is interrupted before the response is received.
	 * @throws SinchanaInvalidArgumentException  If the destination id has a length other than 20 bytes.
	 */
	public byte[] sendRequest(byte[] destination, byte[] message) 
			throws SinchanaTimeOutException, SinchanaInvalidArgumentException, InterruptedException;
	/**
	 * Sends a synchronous request to the given destination. It will be received by the successor of that destination. 
	 * The thread is blocked until the response is received or time out occurs. 
	 * @param destination Destination id to send request. This should be exactly 20 byte length.
	 * @param message Message to send to the destination.
	 * @param timeOut Time out.
	 * @param timeUnit Time Unit.
	 * @return Response from the destination.
	 * @throws SinchanaTimeOutException If the response is not arrived within the time out.
	 * @throws InterruptedException If the thread is interrupted before the response is received.
	 * @throws SinchanaInvalidArgumentException If the destination id has a length other than 20 bytes.
	 */
	public byte[] sendRequest(byte[] destination, byte[] message, long timeOut, TimeUnit timeUnit) 
			throws SinchanaTimeOutException, SinchanaInvalidArgumentException, InterruptedException;
	/**
	 * Sends an asynchronous request to the given destination. It will be received by the successor of that destination.
	 * The thread is blocked until the request is queued to send.
	 * @param destination Destination id to send request. This should be exactly 20 byte length.
	 * @param message Message to send to the destination.
	 * @param callBack SinchanaResponseCallback to handle responses. Pass <code>null</code> if no response is expected.
	 * @throws InterruptedException If the thread is interrupted before the request is queued.
	 * @throws SinchanaInvalidArgumentException If the destination id has a length other than 20 bytes.
	 */
	public void sendRequest(byte[] destination, byte[] message, SinchanaResponseCallback callBack) 
			throws InterruptedException, SinchanaInvalidArgumentException;
	/**
	 * Stores a given key-value pair synchronously. The thread is blocked until the task is completed.
	 * @param key Data key
	 * @param data Data
	 * @return <code>true</code> if the key-data is stored in the DHT successfully. <code>false</code> otherwise.
	 * @throws InterruptedException If the thread is interrupted before the task is completed.
	 * @throws SinchanaTimeOutException If the response is not arrived within the time out.
	 */
	public boolean storeData(byte[] key, byte[] data) throws InterruptedException, SinchanaTimeOutException;
	/**
	 * Stores a given key-value pair synchronously. The thread is blocked until the task is completed.
	 * @param key Data key
	 * @param data Data
	 * @param timeOut Time out
	 * @param timeUnit Time Unit
	 * @return <code>true</code> if the key-data is stored in the DHT successfully. <code>false</code> otherwise.
	 * @throws InterruptedException If the thread is interrupted before the task is completed.
	 * @throws SinchanaTimeOutException If the response is not arrived within the time out.
	 */
	public boolean storeData(byte[] key, byte[] data, long timeOut, TimeUnit timeUnit) 
			throws InterruptedException, SinchanaTimeOutException;
	/**
	 * Stores a given key-value pair asynchronously. The thread is blocked until the task is queued to send.
	 * @param key Data key
	 * @param data Data
	 * @param callBack SinchanaDataCallback to handle responses. Pass <code>null</code> if no response is expected.
	 * @throws InterruptedException If the thread is interrupted before the task is queued.
	 */
	public void storeData(byte[] key, byte[] data, SinchanaDataCallback callBack) throws InterruptedException;
	/**
	 * Retrieves value for a given key synchronously. The thread is blocked until the task is completed.
	 * @param key Data key to retrieve
	 * @return Value which is associated with the key. <code>null</code> if the value is not found.
	 * @throws InterruptedException If the thread is interrupted before the task is completed.
	 * @throws SinchanaTimeOutException If the response is not arrived within the time out.
	 */
	public byte[] getData(byte[] key) throws InterruptedException, SinchanaTimeOutException;
	/**
	 * Retrieves value for a given key synchronously. The thread is blocked until the task is completed.
	 * @param key Data key to retrieve
	 * @param timeOut Time out
	 * @param timeUnit Time Unit
	 * @return Value which is associated with the key. <code>null</code> if the value is not found.
	 * @throws InterruptedException If the thread is interrupted before the task is completed.
	 * @throws SinchanaTimeOutException If the response is not arrived within the time out.
	 */
	public byte[] getData(byte[] key, long timeOut, TimeUnit timeUnit) throws InterruptedException, SinchanaTimeOutException;
	/**
	 * Retrieves value for a given key asynchronously. The thread is blocked until the task is queued.
	 * @param key Data key to retrieve
	 * @param callBack SinchanaDataCallback to handle responses. Pass <code>null</code> if no response is expected.
	 * @throws InterruptedException If the thread is interrupted before the task is queued.
	 */
	public void getData(byte[] key, SinchanaDataCallback callBack) throws InterruptedException;
	/**
	 * Deletes value for a given key synchronously. The thread is blocked until the task is completed.
	 * @param key Data key to delete
	 * @return <code>true</code> if the deletion is completed, <code>false</code> otherwise.
	 * @throws InterruptedException If the thread is interrupted before the task is completed.
	 * @throws SinchanaTimeOutException If the response is not arrived within the time out.
	 */
	public boolean deleteData(byte[] key) throws InterruptedException, SinchanaTimeOutException;
	/**
	 * Deletes value for a given key synchronously. The thread is blocked until the task is completed.
	 * @param key Data key to delete
	 * @param timeOut Time out
	 * @param timeUnit Time Unit
	 * @return <code>true</code> if the deletion is completed, <code>false</code> otherwise.
	 * @throws InterruptedException If the thread is interrupted before the task is completed.
	 * @throws SinchanaTimeOutException If the response is not arrived within the time out.
	 */
	public boolean deleteData(byte[] key, long timeOut, TimeUnit timeUnit) 
			throws InterruptedException, SinchanaTimeOutException;
	/**
	 * Deletes value for a given key asynchronously. The thread is blocked until the task is queued.
	 * @param key Data key to delete
	 * @param callBack SinchanaDataCallback to handle responses. Pass <code>null</code> if no response is expected.
	 * @throws InterruptedException If the thread is interrupted before the task is completed.
	 */
	public void deleteData(byte[] key, SinchanaDataCallback callBack) throws InterruptedException;
	/**
	 * Invokes a service given by the references synchronously. The thread is blocked until the task is completed.
	 * @param reference 
	 * @param data Data to process.
	 * @return Response from the service. <code>null</code> if no such service is found.
	 * @throws InterruptedException If the thread is interrupted before the task is completed.
	 * @throws SinchanaTimeOutException If the response is not arrived within the time out.
	 */
	public byte[] invokeService(byte[] reference, byte[] data) throws InterruptedException, SinchanaTimeOutException;
	/**
	 * Invokes a service given by the references synchronously. The thread is blocked until the task is completed.
	 * @param reference 
	 * @param data Data to process.
	 * @param timeOut Time out
	 * @param timeUnit Time Unit
	 * @return Response from the service. <code>null</code> if no such service is found.
	 * @throws InterruptedException If the thread is interrupted before the task is completed.
	 * @throws SinchanaTimeOutException If the response is not arrived within the time out.
	 */
	public byte[] invokeService(byte[] reference, byte[] data, long timeOut, TimeUnit timeUnit) 
			throws InterruptedException, SinchanaTimeOutException;
	/**
	 * Invokes a service given by the references synchronously. The thread is blocked until the task is queued.
	 * @param reference 
	 * @param data Data to process.
	 * @param callBack SinchanaServiceCallback to handle responses. Pass <code>null</code> if no response is expected.
	 * @throws InterruptedException If the thread is interrupted before the task is queued.
	 */
	public void invokeService(byte[] reference, byte[] data, SinchanaServiceCallback callBack) throws InterruptedException;
	/**
	 * Discovers the reference for a given service key synchronously. The thread is blocked until the task is completed.
	 * @param key Service key to discover
	 * @return Reference associated with the service key. <code>null</code> if the service is not found.
	 * @throws InterruptedException If the thread is interrupted before the task is completed.
	 * @throws SinchanaTimeOutException If the response is not arrived within the time out.
	 */
	public byte[] discoverService(byte[] key) throws InterruptedException, SinchanaTimeOutException;
	/**
	 * Discovers the reference for a given service key synchronously. The thread is blocked until the task is completed.
	 * @param key Service key to discover
	 * @param timeOut Time out
	 * @param timeUnit Time Unit
	 * @return Reference associated with the service key. <code>null</code> if the service is not found.
	 * @throws InterruptedException If the thread is interrupted before the task is completed.
	 * @throws SinchanaTimeOutException If the response is not arrived within the time out.
	 */
	public byte[] discoverService(byte[] key, long timeOut, TimeUnit timeUnit) 
			throws InterruptedException, SinchanaTimeOutException;
	/**
	 * Discovers the reference for a given service key asynchronously. The thread is blocked until the task is queued.
	 * @param key Service key to discover
	 * @param callBack SinchanaServiceCallback to handle responses. Pass <code>null</code> if no response is expected.
	 * @throws InterruptedException If the thread is interrupted before the task is queued.
	 */
	public void discoverService(byte[] key, SinchanaServiceCallback callBack) throws InterruptedException;
	/**
	 * Publish the service asynchronously. The thread is blocked until the task is queued to send.
	 * @param key service key
	 * @param sinchanaServiceInterface SinchanaServiceInterface to handle responses.
	 * @throws InterruptedException If the thread is interrupted before the task is queued.
	 * @throws SinchanaInvalidArgumentException If the SinchanaServiceInterface is null
	 */
	public void publishService(byte[] key, SinchanaServiceInterface sinchanaServiceInterface) 
			throws InterruptedException, SinchanaInvalidArgumentException;
	/**
	 * Removes the service asynchronously. The thread is blocked until the task is queued to send.
	 * @param key service key
	 * @throws InterruptedException If the thread is interrupted before the task is queued.
	 * @throws SinchanaInvalidArgumentException If the SinchanaServiceInterface is null
	 */
	public void removeService(byte[] key) throws InterruptedException, SinchanaInvalidArgumentException;
	
}
