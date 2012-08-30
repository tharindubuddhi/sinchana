/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.dataStore;

/**
 * 
 * @author Tharindu Jayasinghe
 */
public interface SinchanaDataStoreInterface {

    /**
     * the  method to be called at the stored node
     * @param key which the data stored
     * @param data which stored
     * @return
     */
    public abstract boolean store(byte[] key, byte[] data);

    /**
     * the method to be called when a data retrieve is called
     * @param key which the data to be retrieved
     * @return
     */
    public abstract byte[] get(byte[] key);

    /**
     * the method to be called at the removed node.
     * @param key the key which the data to be removed
     * @return
     */
    public abstract boolean remove(byte[] key);
}
