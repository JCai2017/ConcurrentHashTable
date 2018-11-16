package concurrentHashTables;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class FineGrainedRobinHoodHashing implements TableType {
	// Java's ConcurrentHashMap locks on the key level, so we're reusing it
	// instead of using our own implementation
	ConcurrentHashMap<Integer, Integer> hashMap = new ConcurrentHashMap<Integer, Integer>();
	int maxSize = 0;
	static final int probe = 2;
	
	int hash(int key) {
	    key = ((key >>> 16) ^ key) * 0x45d9f3b;
	    key = ((key >>> 16) ^ key) * 0x45d9f3b;
	    key = (key >>> 16) ^ key;
	    return Math.abs(key) % 3000;
	}

	@Override
	public void put(int value) {
		Integer key = hash(value);
		Integer entry = value;
		
		Integer existingEntry = hashMap.computeIfAbsent(key, tempKey -> update(value));
		if(existingEntry == value || existingEntry == null) {
			return;
		}
		
		while(existingEntry.intValue() != entry.intValue() && existingEntry != null) {
			int diff = Math.abs(hash(existingEntry) - key);
			if(diff < probe) {
				int valToReplace = entry;
				hashMap.computeIfPresent(key, (tempKey, tempEntry) -> valToReplace);
				entry = existingEntry;
			}
			
			key++;
			int valToReplace = entry;
			existingEntry = hashMap.computeIfAbsent(key, tempKey -> update(valToReplace));
		}
	}
	
	private int update(Integer entry) {
		maxSize++;
		return entry;
	}

	@Override
	public void remove(int value) {
		Integer key = hash(value);
		BiFunction<Integer, Integer, Integer> function = (tempKey, tempEntry) -> {
			if(tempEntry.intValue() == value) {
				tempEntry = null;
			}
			
			return tempEntry;	
		};
		
		Integer existingEntry = value;
		if(hashMap.containsKey(key)) {
		    existingEntry = hashMap.computeIfPresent(key, function);
		
		    if(existingEntry == null) {
			    return;
		    }
		}
		
		if(maxSize <= 0) {
			return;
		}
		
		List<Integer> keys = new ArrayList<Integer>(hashMap.keySet());
		while(existingEntry != null) {
			key = keys.get((keys.indexOf(key) + 1) % keys.size());
			if(key == hash(value)) {
				return;
			}
			
			existingEntry = hashMap.computeIfPresent(key, function);
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
