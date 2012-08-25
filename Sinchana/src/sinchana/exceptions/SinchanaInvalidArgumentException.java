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
public class SinchanaInvalidArgumentException extends TException {

	private static final long serialVersionUID = 1L;

	public SinchanaInvalidArgumentException() {
		super();
	}

	public SinchanaInvalidArgumentException(String message) {
		super(message);
	}

	public SinchanaInvalidArgumentException(Throwable cause) {
		super(cause);
	}

	public SinchanaInvalidArgumentException(String message, Throwable cause) {
		super(message, cause);
	}
}
