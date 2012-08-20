/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.dataStore;

import java.util.concurrent.ConcurrentHashMap;
import sinchana.SinchanaServer;

/**
 *The class use to 
 * @author DELL
 */
public class SinchanaDataStoreImpl implements SinchanaDataStoreInterface {

	private final ConcurrentHashMap<String, byte[]> dataMap = new ConcurrentHashMap<String, byte[]>();
	private final SinchanaServer server;

	public SinchanaDataStoreImpl(SinchanaServer ss) {
		this.server = ss;
	}

	@Override
	public boolean store(byte[] key, byte[] data) {
		dataMap.put(new String(key), data);
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
