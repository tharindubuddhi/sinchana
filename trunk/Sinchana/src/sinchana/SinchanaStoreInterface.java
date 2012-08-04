/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;


/**
 *
 * @author Hiru
 */
public interface SinchanaStoreInterface {

		public abstract boolean store(String key, String data);

		public abstract String get(String key);

		public abstract boolean delete(String key);
}
