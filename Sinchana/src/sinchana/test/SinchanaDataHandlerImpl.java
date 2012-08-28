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
	public int storeSuccessCount, storeFailureCount, retrieveSuccessCount, retrieveFailureCount;

	@Override
	public void isStored(byte[] key, boolean success) {
		if (success) {
			storeSuccessCount++;
		} else {
			storeFailureCount++;
		}
		if ((storeSuccessCount + storeFailureCount) % 100 == 0) {
			System.out.println("success/failure: " + storeSuccessCount + "/" + storeFailureCount
					+ "\ttotal: " + (storeSuccessCount + storeFailureCount)
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
		System.out.println("response: " + new String(data)
				+ "\tcount: " + retrieveSuccessCount
				+ "\ttime: " + (System.currentTimeMillis() - startRetrieveTime));
//        retrieveSuccessCount++;
//        if((retrieveSuccessCount+retrieveFailureCount)%1000==0){
//            System.out.println("");
//        }
	}

	@Override
	public void error(byte[] error) {
		retrieveFailureCount++;
		System.out.println("response: " + new String(error)
				+ "\tcount: " + retrieveFailureCount
				+ "\ttime: " + (System.currentTimeMillis() - startRetrieveTime));
	}
}