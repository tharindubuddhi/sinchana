/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import sinchana.thrift.DataObject;

/**
 *
 * @author DELL
 */
public class SinchanaDataStore {

    private Map<String, Set<DataObject>> rootObjects = new HashMap<String, Set<DataObject>>();
    private Set<String> storedObjects = new HashSet<String>();

    public void setrootObjects(Map<String, Set<DataObject>> rootObjects) {
        this.rootObjects = rootObjects;
    }

    public Map<String, Set<DataObject>> getrootObjects() {
        return rootObjects;
    }

    public void addStoredObjects(Set<String> storedObjects) {
        this.storedObjects = storedObjects;
    }

    public Set<String> getStoredObjects() {
        return storedObjects;
    }

    public boolean store(DataObject dataObject) {

        Set<DataObject> sourceServers = rootObjects.get(dataObject.dataKey);
        if (sourceServers == null) {
            sourceServers = new HashSet<DataObject>();
        }
        
        boolean success = sourceServers.add(dataObject);
        rootObjects.put(dataObject.dataKey, sourceServers);
        return success;
    }

    public boolean removeData(DataObject dataObject) {

        Set<DataObject> sourceServers = rootObjects.get(dataObject.dataKey);
        if (sourceServers == null) {
            System.out.println("no objects for that key");
        } else if (sourceServers.contains(dataObject)) {
            sourceServers.remove(dataObject);
            return true;           
        } else {
            System.out.println("no objects published from that source for this object");           
        }
        return false;
    }
}
