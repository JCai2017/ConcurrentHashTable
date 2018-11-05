package concurrentHashTables;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class CoarseGrainedRobinHoodHashing implements TableType {
	HashMap<Integer, Integer> hashMap = new HashMap<Integer, Integer>();
	static final int probeValue = 2;
	int maxSize = 0;
	private static final ReentrantLock lock = new ReentrantLock();

	@Override
	public void put(int value) {
		lock.lock();
		try {
		    Integer key = new Integer(value);
		
		    Integer entry = new Integer(value);
		    Integer existingEntry = hashMap.get(key);
		    if(existingEntry == null) {
			    hashMap.put(key, entry);
			    maxSize++;
			    return;
		    } else if(existingEntry == value) {
		    	return;
		    }
		
		    while(existingEntry != null) {
		    	if(existingEntry == value) {
		    		return;
		    	}
		    	
			    int diff = Math.abs(key - existingEntry);
			    if(diff < probeValue) {
				    hashMap.put(key, entry);
				    entry = existingEntry;
			    }
			
			    key++;
			    existingEntry = hashMap.get(key);
		    }
		    
		    hashMap.put(key, entry);
		    maxSize++;
		} finally {
			lock.unlock();
		}
		
		
	}

	@Override
	public void remove(int value) {
		lock.lock();
		try {
		    Integer key = new Integer(value);
		
		    Integer entry = hashMap.get(key);
		    if(entry != null && entry == value) {
			    hashMap.remove(key);
			    return;
		    }
		
		    while(entry == null || entry != value) {
			    key = (key + 1) % maxSize;
			    if(key == value) {
			    	return;
			    }
			    entry = hashMap.get(key);
		    }
		
		    if(entry != null && entry == value) {
			    hashMap.remove(key);
		    }
		} finally {
			lock.unlock();
		}
		
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		for(Integer i: hashMap.keySet()) {
			Integer current = hashMap.get(i);
			if(current != null) {
			    str.append("" + current).append(", ");
			}
		}
		return str.toString();
	}

}
