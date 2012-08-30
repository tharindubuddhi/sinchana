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
package sinchana.dataStore;

import java.util.concurrent.ConcurrentHashMap;
import sinchana.SinchanaServer;

/**
 *
 * @author Tharindu Jayasinghe
 */
public class SinchanaDataStoreImpl implements SinchanaDataStoreInterface {

	private final ConcurrentHashMap<String, byte[]> dataMap = new ConcurrentHashMap<String, byte[]>();
	private final SinchanaServer server;

    /**
     * 
     * @param ss Sinchana server which has this data store
     */
    public SinchanaDataStoreImpl(SinchanaServer ss) {
		this.server = ss;
	}

    /**
     * the method stores a key, value pair in the concurrent hash map
     * @param key the data key with the data needs to be stored
     * @param data the data to be stored 
     * @return
     */
    @Override
	public boolean store(byte[] key, byte[] data) {
		dataMap.put(new String(key), data);
		return true;
	}

    /**
     * the method returns the data for the given key
     * @param key the key which the data to be retrieved
     * @return
     */
    @Override
	public byte[] get(byte[] key) {
		return dataMap.get(new String(key));
	}

    /**
     * the method removes the data for the given key
     * @param key the key which the data to be removed 
     * @return
     */
    @Override
	public boolean remove(byte[] key) {
		dataMap.remove(new String(key));
		return true;
	}
}
