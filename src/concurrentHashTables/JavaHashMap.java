package concurrentHashTables;

import java.util.concurrent.ConcurrentHashMap;

public class JavaHashMap implements TableType {
	public ConcurrentHashMap<Integer, Integer> javaHashMap = new ConcurrentHashMap<>();
	
	private int hash(int key) {
	    key = ((key >>> 16) ^ key) * 0x45d9f3b;
	    key = ((key >>> 16) ^ key) * 0x45d9f3b;
	    key = (key >>> 16) ^ key;
	    return Math.abs(key) % 3000;
	}

	@Override
	public void put(int value) {
		Integer key = hash(value);
		javaHashMap.put(key, value);
	}

	@Override
	public void remove(int value) {
		Integer key = hash(value);
		javaHashMap.remove(key, value);
	}
	
	@Override
	public boolean get(int value) {
		int key = hash(value);
		return (javaHashMap.get(key) == value);
	}
}
