/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.service;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author DELL
 */
public class SinchanaServiceStore {

	private Map<String, SinchanaServiceInterface> services = new HashMap<String, SinchanaServiceInterface>();

	/**
	 * 
	 * @return the relevant Set for a given serviceKey
	 */
	public SinchanaServiceInterface get(byte[] key) {
		return services.get(new String(key));
	}

	/**
	 * Method add a dataObject to the Map with the serviceKey
	 * @param dataObject which need to be added to the Set
	 * @return the success value of the storing action
	 */
	public boolean publishService(byte[] key, SinchanaServiceInterface sinchanaServiceInterface) {
		services.put(new String(key), sinchanaServiceInterface);
		return true;
	}

	/**
	 * Method remove the particular dataObject from the Map and returns the removal is success or not. 
	 * @param dataObject which needs to be removed from the Map
	 * @return  the success value of the removal action
	 */
	public boolean removeService(byte[] key) {
		services.remove(new String(key));
		return true;
	}
}
