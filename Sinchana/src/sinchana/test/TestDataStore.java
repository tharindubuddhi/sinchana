/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import java.util.Set;
import sinchana.Server;
import sinchana.SinchanaStoreInterface;
import sinchana.thrift.DataObject;

/**
 *
 * @author DELL
 */
public class TestDataStore {
    private Server server;
    private int testId;
    private ServerUI gui = null;
    private TesterController testerController;

    public TestDataStore(Tester tester,TesterController tc) {
        
        this.testId = tester.getTestId();
            this.testerController = tc;
            this.server = tester.getServer();
            this.gui = tester.getGui();
            
            server.registerSinchanaStoreInterface(new SinchanaStoreInterface() {

            @Override
            public void store(DataObject dataObject) {
                System.out.println("storing object success "+dataObject.DataValue);
            }

            @Override
            public void get(Set<DataObject> dataObjectSet) {
                System.out.print("data set retrieved : ");
                for (DataObject dataObject : dataObjectSet) {
                    System.out.print(dataObject.DataValue+" ");
                }
                System.out.println("");
            }

            @Override
            public void remove(DataObject dataObject) {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void isStored(Boolean success) {
                System.out.println("Stored "+success);
            }

            @Override
            public void isRemoved(Boolean success) {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        });
    }
    
    
}
