package concurrentHashTables;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

public class CoarseGrainedChainHashing implements TableType {
	public HashMap<String, LinkedList<Integer>> hashTable = new HashMap<>();
	private final ReentrantLock lock = new ReentrantLock();

	@Override
	public void put(int value) {
		lock.lock();
		
		try {
			String key = "" + value;
			LinkedList<Integer> list = hashTable.get(key);
			if(list == null) {
				list = new LinkedList<Integer>();
			}
			
			list.add(value);
			hashTable.put(key, list);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void remove(int value) {
		lock.lock();
		
		try {
			String key = "" + value;
			LinkedList<Integer> list = hashTable.get(key);
			if(list != null) {
				list.remove(list.indexOf(new Integer(value)));
			}
			
			hashTable.put(key, list);
		} finally {
			lock.unlock();
		}
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		for(String s: hashTable.keySet()) {
			for(Integer i: hashTable.get(s)) {
				str.append("" + i).append(",");
			}
		}
		return str.toString();
	}

}
