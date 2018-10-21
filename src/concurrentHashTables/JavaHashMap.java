package concurrentHashTables;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class JavaHashMap implements TableType {
	public ConcurrentHashMap<String, Integer> javaHashMap = new ConcurrentHashMap<>();

	@Override
	public void put(int value) {
		String key = "" + value;
		javaHashMap.put(key, value);
	}

	@Override
	public void remove(int value) {
		String key = "" + value;
		javaHashMap.remove(key, value);
	}
}
