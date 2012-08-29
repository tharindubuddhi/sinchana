/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.service;

import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author DELL
 */
public class SinchanaServiceStore {

	private ConcurrentHashMap<String, SinchanaServiceInterface> services = new ConcurrentHashMap<String, SinchanaServiceInterface>();

	public SinchanaServiceInterface get(byte[] key) {
		return services.get(new String(key));
	}

	public boolean publishService(byte[] key, SinchanaServiceInterface sinchanaServiceInterface) {
		services.put(new String(key), sinchanaServiceInterface);
		return true;
	}

	public boolean removeService(byte[] key) {
		services.remove(new String(key));
		return true;
	}
}
