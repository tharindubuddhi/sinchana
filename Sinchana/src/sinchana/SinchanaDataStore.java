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
    private Set<String> storedObject = new HashSet<String>();

    public void setrootObjects(Map<String, Set<DataObject>> rootObjects) {
        this.rootObjects = rootObjects;
    }

    public Map<String, Set<DataObject>> getrootObjects() {
        return rootObjects;
    }
    
    
}
