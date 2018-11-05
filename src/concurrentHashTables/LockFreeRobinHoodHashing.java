package concurrentHashTables;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LockFreeRobinHoodHashing implements TableType {
	HashMap<Integer, AtomicReference<Node>> hashMap = new HashMap<Integer, AtomicReference<Node>>();
	static final int probeValue = 2;
	AtomicInteger maxSize = new AtomicInteger(0);

	@Override
	public void put(int value) {
		Integer key = value;
		int valToAdd = value;
		Node entry = new Node(value);
		AtomicReference<Node> existingEntry = hashMap.get(key);
		if(existingEntry == null) {
			while(hashMap.get(key) == null) {
				AtomicReference<Node> newNode = new AtomicReference<Node>(entry);
				hashMap.put(key, newNode);
				try {
				    Thread.sleep(0, 1000);
			    } catch (InterruptedException e) {
				    e.printStackTrace();
			    }
			}
			
			if(hashMap.get(key).get().value == value) {
				maxSize.getAndIncrement();
				return;
			}
		} else if(existingEntry.get() == null) {
			if(existingEntry.compareAndSet(null, entry)) {
				return;
			}
		} else if(contains(value)) {
			return;
		}
		
		while(true) {
			key = valToAdd;
			existingEntry = hashMap.get(key);
			
			while(existingEntry != null && existingEntry.get() != null) {
				Node current = existingEntry.get();
				int currentVal = current.value;
				int diff = Math.abs(currentVal - key);
				if(diff < probeValue) {
					if(existingEntry.compareAndSet(current, entry)) {
						entry = new Node(currentVal);
						valToAdd = currentVal;
					} else
						break;
						
				}
				
				key++;
				existingEntry = hashMap.get(key);
			}
			
			if(existingEntry == null) {
				hashMap.put(key, new AtomicReference<Node>(entry));
				maxSize.getAndIncrement();
				return;
			}
		}
		
	}
	
	public boolean contains(int value) {
		for(Integer i: hashMap.keySet()) {
			if(hashMap.get(i).get().value == value)
				return true;
		}
		
		return false;
	}

	@Override
	public void remove(int value) {
		Integer key = value;
		AtomicReference<Node> existingEntry = hashMap.get(key);
		
		if(existingEntry == null) {
			return;
		}
		
		while(true) {
			boolean found = false;
			key = value;
			existingEntry = hashMap.get(key);
			Node entry = existingEntry.get();
			
			if(existingEntry.get() != null && existingEntry.get().value == value) {
				if(existingEntry.compareAndSet(entry, null))
					return;
				else
					continue;
			}
			
			key = (key + 1) % maxSize.get();
			existingEntry = hashMap.get(key);
			if(existingEntry != null) {
			    entry = existingEntry.get();
			}
			
			while(key != value) {
				if(existingEntry != null && existingEntry.get() != null) {
					entry = existingEntry.get();
					if(entry.value == value) {
					    if(existingEntry.compareAndSet(entry, null))
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
	
	public class Node {
		int value;
		public Node(int value) {
			this.value = value;
		}
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		for(Integer i: hashMap.keySet()) {
			AtomicReference<Node> current = hashMap.get(i);
			if(current != null && current.get() != null) {
			    str.append("" + current.get().value).append(", ");
			}
		}
		return str.toString();
	}
}
