/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import java.util.Set;
import sinchana.Server;
import sinchana.SinchanaServiceInterface;
import sinchana.thrift.DataObject;

/**
 *
 * @author DELL
 */
public class TestService {

	private Server server;
	private int testId;
	private ServerUI gui = null;
	private TesterController testerController;

	public TestService(Tester tester, TesterController tc) {

		this.testId = tester.getTestId();
		this.testerController = tc;
		this.server = tester.getServer();
		this.gui = tester.getGui();


		server.registerSinchanaServiceInterface(new SinchanaServiceInterface() {

            @Override
            public void publish(DataObject dataObject) {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void get(Set<DataObject> dataObjectSet) {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void remove(DataObject dataObject) {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void isPublished(Boolean success) {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void isRemoved(Boolean success) {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        });

	}
}
