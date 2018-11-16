package concurrentHashTables;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

public class CoarseGrainedChainHashing implements TableType {
	public HashMap<Integer, LinkedList<Integer>> hashTable = new HashMap<>();
	private final ReentrantLock lock = new ReentrantLock();

	private int hash(int key) {
	    key = ((key >>> 16) ^ key) * 0x45d9f3b;
	    key = ((key >>> 16) ^ key) * 0x45d9f3b;
	    key = (key >>> 16) ^ key;
	    return Math.abs(key) % 3000;
	}
	
	@Override
	public void put(int value) {
		lock.lock();
		
		try {
			Integer key = hash(value);
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
			Integer key = hash(value);
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
	public boolean get(int value) {
		int key = hash(value);
		LinkedList<Integer> list = hashTable.get(key);
		if(list != null) {
			for(int i = 0; i < list.size(); i++) {
				if(list.get(i) == value)
					return true;
			}
		}
		
		return false;
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
