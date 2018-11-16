package concurrentHashTables;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JavaSynchronizedMap implements TableType {
	HashMap<Integer, Integer> hashMap = new HashMap<Integer, Integer>();
	Map<Integer, Integer> m = (Map<Integer, Integer>) Collections.synchronizedMap(hashMap);
	@Override
	public void put(int value) {
		Integer key = value % 1500;
		m.put(key, value);
		
	}
	
	@Override
	public void remove(int value) {
		Integer key = value % 1500;
		m.remove(key, value);
		
	}
	
	@Override
	public boolean get(int value) {
		int key = value % 1500;
		return (m.get(key) == value);
	}

	
}
