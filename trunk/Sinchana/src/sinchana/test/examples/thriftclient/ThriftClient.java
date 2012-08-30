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
package sinchana.test.examples.thriftclient;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import sinchana.thrift.DHTServer;
import sinchana.thrift.Response;
import sinchana.util.tools.Hash;

/**
 *This class use by the thrift client UI in order to process client operations. Thrift client doesn't need to be 
 * part of the ring. But the client can store,retrieve data, discover, invoke services
 * @author Tharindu Jayasinghe
 */
public class ThriftClient {

    /*address of the Sinchana node to connect*/
    private String sinchanaNodeAddress;
    /*port id of the sinchana node to connect*/
    private int sinchanaNodePort;
    TTransport transport;
    TProtocol protocol;
    DHTServer.Client client;
    Response response;

    /**
     * opens a Thrift connection to a sinchana node which is in the machine of the given ip address and the given port id
     * @param address
     * @param port
     */
    public void openConnection(String address, int port) {
        this.sinchanaNodeAddress = address;
        this.sinchanaNodePort = port;
        transport = new TSocket(sinchanaNodeAddress, sinchanaNodePort);
        try {
            transport.open();
        } catch (TTransportException ex) {
            Logger.getLogger(ThriftClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        protocol = new TBinaryProtocol(transport);
        client = new DHTServer.Client(protocol);
        System.out.println("connected....");
    }

    
    /** the method send a message to a random picked node id
     * @param messageText the message content to be sent to a random destination
     */
    public void sendRequest(String messageText) {
        Random random = new Random();
        String val = new BigInteger(160, random).toString(16);
        System.out.println(val);
        byte[] destination = Hash.generateId(val);
        byte[] message = messageText.getBytes();
        try {
            response = client.sendRequest(ByteBuffer.wrap(destination), ByteBuffer.wrap(message));
            System.out.println(new String(response.getData()));
        } catch (TException ex) {
            Logger.getLogger(ThriftClient.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     * the method stores the key, data in a node in the ring
     * @param key data key to be stored
     * @param data dataValue to be stored
     */
    public void storeData(String key, String data) {
        byte[] dataKey = key.getBytes();
        byte[] dataValue = data.getBytes();
        try {
            response = client.publishData(ByteBuffer.wrap(dataKey), ByteBuffer.wrap(dataValue));
            if (response.success) {
                System.out.println(data + " stored success.");
            } else {
                System.out.println(data + " storing failed");
            }

        } catch (TException ex) {
            Logger.getLogger(ThriftClient.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
   
    /**
     * the method retrieve data which maps to the given key
     * @param key datakey to be retrieved
     */
    public void retrieveData(String key) {
        try {
            response = client.getData(ByteBuffer.wrap(key.getBytes()));
        } catch (TException ex) {
            Logger.getLogger(ThriftClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * the method remove data which maps to the given key
     * @param key datakey to be removed
     */
    public void removeData(String key) {
        try {
            response = client.removeData(ByteBuffer.wrap(key.getBytes()));
            if(response.success){
                System.out.println("data removed success");
            }
        } catch (TException ex) {
            Logger.getLogger(ThriftClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
