/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;


/**
 *
 * @author Hiru
 */
public interface SinchanaServiceInterface {
		
		public abstract boolean publish(long key, String service);
		public abstract String get(long key);
		public abstract boolean remove(long key);
		
}
