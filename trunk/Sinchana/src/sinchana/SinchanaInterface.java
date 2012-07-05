/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import sinchana.thrift.Message;

/**
 *
 * @author Hiru
 */
public interface SinchanaInterface {
		
		public abstract Message transfer(Message message);
		
}
