package concurrentHashTables;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

public class CoarseGrainedChainHashing implements TableType {
	public HashMap<Integer, LinkedList<Integer>> hashTable = new HashMap<>();
	private final ReentrantLock lock = new ReentrantLock();

	@Override
	public void put(int value) {
		lock.lock();
		
		try {
			Integer key = value % 1500;
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
			Integer key = value % 1500;
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
		for(Integer key: hashTable.keySet()) {
			for(Integer i: hashTable.get(key)) {
				str.append("" + i).append(",");
			}
		}
		return str.toString();
	}

}
