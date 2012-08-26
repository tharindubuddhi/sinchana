/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.dataStore;

import java.util.concurrent.ConcurrentHashMap;
import sinchana.SinchanaServer;

public class SinchanaDataStoreImpl implements SinchanaDataStoreInterface {

	private final ConcurrentHashMap<String, byte[]> dataMap = new ConcurrentHashMap<String, byte[]>();
	private final SinchanaServer server;
    public static long startStoreTime, lastTime,startRetrieveTime;
	public static int storeSuccessCount, lastCount,storeFailureCount, retrieveSuccessCount, retrieveFailureCount;
    
	public SinchanaDataStoreImpl(SinchanaServer ss) {
		this.server = ss;
	}

	@Override
	public boolean store(byte[] key, byte[] data) {
		dataMap.put(new String(key), data);
        storeSuccessCount++;
        if ((storeSuccessCount) % 1000 == 0) {
            long nowTime = System.currentTimeMillis();
            long tp = ((storeSuccessCount-lastCount)*1000)/(nowTime-lastTime);
			System.out.println("success count: " + storeSuccessCount + "\ttime: " + (nowTime-startStoreTime)+" throughput: "+tp);
            lastCount = storeSuccessCount;
            lastTime = nowTime;
		}
		return true;
	}

	@Override
	public byte[] get(byte[] key) {
		return dataMap.get(new String(key));
	}

	@Override
	public boolean remove(byte[] key) {
		dataMap.remove(new String(key));
		return true;
	}
}
