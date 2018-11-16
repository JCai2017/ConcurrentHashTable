package concurrentHashTables;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LockFreeRobinHoodHashing implements TableType {
	HashMap<Integer, AtomicInteger> hashMap = new HashMap<Integer, AtomicInteger>();
	static final int probeValue = 2;
	AtomicInteger maxSize = new AtomicInteger(0);
	
	public LockFreeRobinHoodHashing() {
		for(int i = 0; i < 1500; i++) {
			hashMap.put(i, new AtomicInteger(-1));
		}
	}

	@Override
	public void put(int value) {
		Integer key = value % 1500;
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
			key = valToAdd % 1500;
			existingEntry = hashMap.get(key);
			
			while(existingEntry != null) {
				int currentVal = existingEntry.get();
				if(currentVal == valToAdd)
					return;
				int diff = Math.abs(currentVal % 1500 - key);
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
		Integer key = value % 1500;
		AtomicInteger existingEntry = hashMap.get(key);
		
		if(existingEntry == null) {
			return;
		}
		
		while(true) {
			boolean found = false;
			key = value % 1500;
			existingEntry = hashMap.get(key);
			int entry = existingEntry.get();
			
			if(entry == value) {
				if(existingEntry.compareAndSet(entry, -1))
					return;
				else
					continue;
			}
			
			key = (key + 1) % maxSize.get();
			existingEntry = hashMap.get(key);
			if(existingEntry != null) {
			    entry = existingEntry.get();
			}
			
			while(key != value % 1500) {
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
				
				key = (key + 1) % maxSize.get();
				existingEntry = hashMap.get(key);
			}
			
			if(!found)
				return;
		}

	}
	
	@Override
	public boolean get(int value) {
		int key = value % 1500;
		AtomicInteger current = hashMap.get(key);
		int keyToSearch = (key + 1) % maxSize.get();
		while(keyToSearch != key) {
			if(current != null && current.get() == value)
				return true;
			
			current = hashMap.get(key);
			keyToSearch = (keyToSearch + 1) % maxSize.get();
		}
		
		if(current != null && current.get() == value)
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
