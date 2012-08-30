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

package sinchana.test;

import sinchana.dataStore.SinchanaDataCallback;

/**
 *
 * @author Tharindu Jayasinghe
 */
public class SinchanaDataHandlerImpl implements SinchanaDataCallback {

	public long startStoreTime, startRetrieveTime;
	public int storeSuccessCount, storeFailureCount, retrieveSuccessCount, FailureCount,totalCount,newCount;
    public boolean retrieveContinous = false;
	@Override
	public void isStored(byte[] key, boolean success) {
		if (success) {
			storeSuccessCount++;
		} else {
			storeFailureCount++;
		}
		if ((storeSuccessCount + storeFailureCount) % 100 == 0) {
			System.out.println("success/failure: " + storeSuccessCount + "/" + (storeFailureCount+FailureCount)
					+ "\ttotal: " + (storeSuccessCount + storeFailureCount+FailureCount)
					+ "\ttime: " + (System.currentTimeMillis() - startStoreTime));
		}
	}

	@Override
	public void isRemoved(byte[] key, boolean success) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void response(byte[] key, byte[] data) {
		retrieveSuccessCount++;totalCount++;
        if(!retrieveContinous){
            System.out.println("retrieved : " + (data==null?data:new String(data))
				+ "\tcount: " + retrieveSuccessCount
				+ "\ttime: " + (System.currentTimeMillis() - startRetrieveTime));
        }		
	}
    

	@Override
	public void error(byte[] error) {
		FailureCount++;
		System.out.println("response error : " + (error==null?error:new String(error))
				+ "\tcount: " + FailureCount
				+ "\ttime: " + (System.currentTimeMillis() - startRetrieveTime));
	}
}
