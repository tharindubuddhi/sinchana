/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.dataStore;

public interface SinchanaDataStoreInterface {

	public abstract boolean store(byte[] key, byte[] data);

	public abstract byte[] get(byte[] key);

	public abstract boolean remove(byte[] key);
}
