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

    public static  int storeResponseCount=0,storeCount=0, removeCount=0, removeResponseCount=0;
    public static long storeStartTime=0,retrieveStartTime=0,removeStartTime=0;
    public TestDataStore(Tester tester,TesterController tc) {
        
        
        this.testId = tester.getTestId();
            this.testerController = tc;
            this.server = tester.getServer();
            this.gui = tester.getGui();
            
            server.registerSinchanaStoreInterface(new SinchanaStoreInterface() {

            @Override
            public void store(DataObject dataObject) {
                storeCount++;
                System.out.println("storing object success "+dataObject.dataValue+" count: "+storeCount);
            }

            @Override
            public void get(Set<DataObject> dataObjectSet) {
                System.out.print("data set retrieved : ");
                for (DataObject dataObject : dataObjectSet) {
                    System.out.print(dataObject.dataValue+" ");
                }
                System.out.println("completed in : "+(System.currentTimeMillis()-retrieveStartTime));
            }

            @Override
            public void remove(DataObject dataObject) {
                removeCount++;
                System.out.println(dataObject.dataKey+" data key removed from the root, count : "+removeCount);
            }

            @Override
            public void isStored(Boolean success) {
                storeResponseCount++;
                System.out.println("Stored response count : "+storeResponseCount+" "+success+" completed in : "+(System.currentTimeMillis()-storeStartTime));
            }

            @Override
            public void isRemoved(Boolean success) {
                removeResponseCount++;
                System.out.println("Data removed "+success+" response count : "+removeResponseCount+" completed in : "+(System.currentTimeMillis()-removeStartTime));
            }
        });
            
            
    }
    
    
}
