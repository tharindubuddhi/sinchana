/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.exceptions;

import org.apache.thrift.TException;

/**
 *
 * @author Hiru
 */
public class SinchanaJoinException extends TException {

	private static final long serialVersionUID = 1L;

	public SinchanaJoinException() {
		super();
	}

	public SinchanaJoinException(String message) {
		super(message);
	}

	public SinchanaJoinException(Throwable cause) {
		super(cause);
	}

	public SinchanaJoinException(String message, Throwable cause) {
		super(message, cause);
	}
}
