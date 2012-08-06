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

	public abstract boolean publish(String key, String service);

	public abstract String get(String key);

	public abstract boolean remove(String key);
}
