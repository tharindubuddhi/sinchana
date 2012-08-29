/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import sinchana.dataStore.SinchanaDataCallback;

/**
 *
 * @author DELL
 */
public class SinchanaDataHandlerImpl implements SinchanaDataCallback {

	public long startStoreTime, startRetrieveTime;
	public int storeSuccessCount, storeFailureCount, retrieveSuccessCount, FailureCount;
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
		retrieveSuccessCount++;
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
