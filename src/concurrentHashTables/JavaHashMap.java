package concurrentHashTables;

import java.util.concurrent.ConcurrentHashMap;

public class JavaHashMap implements TableType {
	public ConcurrentHashMap<Integer, Integer> javaHashMap = new ConcurrentHashMap<>();

	@Override
	public void put(int value) {
		Integer key = value % 1500;
		javaHashMap.put(key, value);
	}

	@Override
	public void remove(int value) {
		Integer key = value % 1500;
		javaHashMap.remove(key, value);
	}
}
