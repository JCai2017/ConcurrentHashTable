package concurrentHashTables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LockFreeRobinHoodHashing implements TableType {
	HashMap<Integer, AtomicInteger> hashMap = new HashMap<Integer, AtomicInteger>();
	static final int probeValue = 2;
	AtomicInteger maxSize = new AtomicInteger(0);
	
	public LockFreeRobinHoodHashing() {
		for(int i = 0; i < 3000; i++) {
			hashMap.put(i, new AtomicInteger(-1));
		}
	}
	
	private int hash(int key) {
	    key = ((key >>> 16) ^ key) * 0x45d9f3b;
	    key = ((key >>> 16) ^ key) * 0x45d9f3b;
	    key = (key >>> 16) ^ key;
	    return Math.abs(key) % 3000;
	}

	@Override
	public void put(int value) {
		Integer key = hash(value);
		int valToAdd = value;
		AtomicInteger existingEntry = hashMap.get(key);
		
		while(existingEntry == null) {
			existingEntry = hashMap.get(key);
		}
		
        if(existingEntry.get() == -1) {
			if(existingEntry.compareAndSet(-1, valToAdd)) {
				maxSize.getAndIncrement();
				return;
			}
		}
		
		while(true) {
			key = hash(valToAdd);
			existingEntry = hashMap.get(key);
			
			while(existingEntry != null) {
				int currentVal = existingEntry.get();
				if(currentVal == valToAdd)
					return;
				int diff = Math.abs(hash(currentVal) - key);
				if(diff < probeValue) {
					if(existingEntry.compareAndSet(currentVal, valToAdd)) {
						valToAdd = currentVal;
					} else
						break;
						
				}
				
				key++;
				existingEntry = hashMap.get(key);
			}
			
			if(existingEntry == null) {
				hashMap.put(key, new AtomicInteger(valToAdd));
				maxSize.getAndIncrement();
				return;
			}
		}
		
	}

	@Override
	public void remove(int value) {
		Integer key = hash(value);
		AtomicInteger existingEntry = hashMap.get(key);
		
		if(existingEntry == null) {
			return;
		}
		
		while(true) {
			List<Integer> keys = new ArrayList<Integer>(hashMap.keySet());
			boolean found = false;
			key = hash(value);
			existingEntry = hashMap.get(key);
			int entry = existingEntry.get();
			
			if(entry == value) {
				if(existingEntry.compareAndSet(entry, -1))
					return;
				else
					continue;
			}
			
			key = keys.get((keys.indexOf(key) + 1) % keys.size());
			existingEntry = hashMap.get(key);
			if(existingEntry != null) {
			    entry = existingEntry.get();
			}
			
			while(key != hash(value)) {
				if(existingEntry != null && existingEntry.get() != -1) {
					entry = existingEntry.get();
					if(entry == value) {
					    if(existingEntry.compareAndSet(entry, -1))
						    return;
					    else {
						    found = true;
						    break;
					    }
					}
					
				}
				
				key = keys.get((keys.indexOf(key) + 1) % keys.size());
				existingEntry = hashMap.get(key);
			}
			
			if(!found)
				return;
		}

	}
	
	@Override
	public boolean get(int value) {
		int key = hash(value);
		Integer current = hashMap.get(key).get();
		List<Integer> keys = new ArrayList<Integer>(hashMap.keySet());
		int keyToSearch = (keys.indexOf(key) + 1) % keys.size();
		while(keyToSearch != keys.indexOf(key)) {
			if(current != null && current == value)
				return true;
			
			current = hashMap.get(keys.get(keyToSearch)).get();
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
			AtomicInteger current = hashMap.get(i);
			if(current.get() != -1) {
			    str.append("" + current.get()).append(", ");
			}
		}
		return str.toString();
	}
}
