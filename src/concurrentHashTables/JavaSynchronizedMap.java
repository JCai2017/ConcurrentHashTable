package concurrentHashTables;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JavaSynchronizedMap implements TableType {
	HashMap<Integer, Integer> hashMap = new HashMap<Integer, Integer>();
	Map<Integer, Integer> m = (Map<Integer, Integer>) Collections.synchronizedMap(hashMap);
	
	private int hash(int key) {
	    key = ((key >>> 16) ^ key) * 0x45d9f3b;
	    key = ((key >>> 16) ^ key) * 0x45d9f3b;
	    key = (key >>> 16) ^ key;
	    return Math.abs(key) % 3000;
    }
	
	@Override
	public void put(int value) {
		Integer key = hash(value);
		m.put(key, value);
		
	}
	
	@Override
	public void remove(int value) {
		Integer key = hash(value);
		m.remove(key, value);
		
	}
	
	@Override
	public boolean get(int value) {
		int key = hash(value);
		return (m.get(key) == value);
	}

	
}
