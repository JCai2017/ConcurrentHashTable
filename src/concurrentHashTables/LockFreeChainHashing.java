package concurrentHashTables;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicMarkableReference;

public class LockFreeChainHashing implements TableType {
	private HashMap<String, Node> atomicHashMap = new HashMap<>();

	@Override
	public void put(int value) {
		String key = "" + value;
		Node n = new Node(value);
		Node head = atomicHashMap.get(key);

		if(head == null) {
			Node newHead = new Node(null);
			while(!newHead.next.compareAndSet(null, n, false, false)) {}
			
			while(atomicHashMap.get(key) == null) {
			    atomicHashMap.put(key, newHead);
			    try {
				    Thread.sleep(0, 1000);
			    } catch (InterruptedException e) {
				    e.printStackTrace();
			    }
			}
			
			if(atomicHashMap.get(key).next.getReference().value == value) {
			    return;
			}
		} else if(head.next.getReference() == null) {
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
		String key = "" + value;
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
	public String toString() {
		StringBuilder str = new StringBuilder();
		int numElem = 0;
		for(String s: atomicHashMap.keySet()) {
			Node current = atomicHashMap.get(s).next.getReference();
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
