package concurrentHashTables;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicMarkableReference;

public class LockFreeChainHashing implements TableType {
	private HashMap<Integer, Node> atomicHashMap = new HashMap<>();
	
	public LockFreeChainHashing() {
		for(int i = 0; i < 3000; i++) {
			atomicHashMap.put(i, new Node(null));
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
		Node n = new Node(value);
		Node head = atomicHashMap.get(key);

		if(head.next.getReference() == null) {
			if(head.next.compareAndSet(null, n, false, false))
				return;
		}
		
		while(true) {
			Node current = atomicHashMap.get(key).next.getReference();
			Node prev = atomicHashMap.get(key);
			while(current.next.getReference() != null && !current.next.isMarked()) {
				prev = current;
				current = current.next.getReference();
			}
			
			if(!current.next.isMarked()) {
				if(current.next.compareAndSet(null, n, false, false))
					return;
			}
		}
	}

	@Override
	public void remove(int value) {
		Integer key = hash(value);
		Node current = atomicHashMap.get(key);
		if(current == null || current.next.getReference() == null) {
			return;
		}
		
		while(true) {
			current = atomicHashMap.get(key);
			boolean found = false;
			while(current != null && current.next.getReference() != null && !current.next.isMarked()) {
				if(current.next.getReference().value == value) {
					found = true;
					Node nextNode = current.next.getReference();
					boolean logicalRemove = current.next.attemptMark(nextNode, true);
					nextNode = nextNode.next.getReference();
					if(logicalRemove) {
						while(nextNode != null && nextNode.next.isMarked()) {
							nextNode = current.next.getReference();
						}
						
						if(current.next.compareAndSet(current.next.getReference(), nextNode, true, false))
							return;
					}
				}
				
				current = current.next.getReference();
			}
			
			if(!found)
				return;
			
		}
	}
	
	@Override
	public boolean get(int value) {
		int key = hash(value);
		Node current = atomicHashMap.get(key);
		if(current != null) {
			while(current.next != null) {
				if(current.value != null && current.value == value)
					return true;
				
				current = current.next.getReference();
			}
		}
		
		if(current.value == value) 
			return true;
		
		return false;
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		int numElem = 0;
		for(Integer i: atomicHashMap.keySet()) {
			Node current = atomicHashMap.get(i).next.getReference();
			while(current != null) {
				str.append("" + current.value).append(",");
				numElem ++;
				current = current.next.getReference();
			}
		}
		
		//return "" + numElem;
		return str.toString();
	}

	private static class Node {
		public AtomicMarkableReference<Node> next;
		Integer value;
		
		public Node(Integer value) {
			this.value = value;
			next = new AtomicMarkableReference<Node>(null, false);
		}
	}
}
