package concurrentHashTables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class CoarseGrainedRobinHoodHashing implements TableType {
	HashMap<Integer, Integer> hashMap = new HashMap<Integer, Integer>();
	static final int probeValue = 2;
	int maxSize = 0;
	private static final ReentrantLock lock = new ReentrantLock();

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
		    	
			    int diff = Math.abs(key - hash(existingEntry));
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
		    Integer key = hash(value);
		
		    Integer entry = hashMap.get(key);
		    if(entry != null && entry == value) {
			    hashMap.remove(key);
			    return;
		    }
		
		    while(entry == null || entry != value) {
			    key = (key + 1) % maxSize;
			    if(key == hash(value)) {
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
	public boolean get(int value) {
		int key = hash(value);
		Integer current = hashMap.get(key);
		List<Integer> keys = new ArrayList<Integer>(hashMap.keySet());
		int keyToSearch = (keys.indexOf(key) + 1) % keys.size();
		while(keyToSearch != keys.indexOf(key)) {
			if(current != null && current == value)
				return true;
			
			current = hashMap.get(keys.get(keyToSearch));
			keyToSearch = (keyToSearch + 1) % keys.size();
		}
		
		if(current != null && current == value)
			return true;
		
		return false;
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
