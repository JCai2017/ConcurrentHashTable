package concurrentHashTables;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class FineGrainedRobinHoodHashing implements TableType {
	// Java's ConcurrentHashMap locks on the key level, so we're reusing it
	// instead of using our own implementation
	ConcurrentHashMap<Integer, Integer> hashMap = new ConcurrentHashMap<Integer, Integer>();
	int maxSize = 0;
	static final int probe = 2;

	@Override
	public void put(int value) {
		Integer key = value;
		Integer entry = value;
		
		Integer existingEntry = hashMap.computeIfAbsent(key, tempKey -> update(value));
		if(existingEntry == entry || existingEntry == null) {
			return;
		}
		
		while(existingEntry != entry && existingEntry != null) {
			int diff = Math.abs(existingEntry - key);
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
		Integer key = value;
		BiFunction<Integer, Integer, Integer> function = (tempKey, tempEntry) -> {
			if(tempEntry == value) {
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
		
		while(existingEntry != null) {
			key = (key + 1) % maxSize;
			if(key == value) {
				return;
			}
			
			if(!hashMap.containsKey(key)) {
				continue;
			}
			existingEntry = hashMap.computeIfPresent(key, function);
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
