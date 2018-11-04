package concurrentHashTables;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

public class FineGrainedChainHashing implements TableType {
	public HashMap<String, Node> hashTable = new HashMap<>();

	@Override
	public void put(int value) {	
		String key = "" + value;
		Node n = new Node(value);
		Node current = hashTable.get(key);
		if(current == null) {
			hashTable.put(key, n);
			return;
		}
		
		current.lock.lock();
		Node prev;
		while(current.next != null) {
			prev = current;
			current = current.next;
			
			current.lock.lock();
			prev.lock.unlock();
		}
		
		current.next = n;
		current.lock.unlock();
	}

	@Override
	public void remove(int value) {
		String key = "" + value;
		Node current = hashTable.get(key);
		if(current != null) {
			if(current.next == null && current.value == value) {
				hashTable.put(key, null);
				return;
			} else if(current.next == null) {
				return;
			}
			
			Node prev = current;
			current = current.next;
			current.lock.lock();
			while(current.next != null) {
				if(current.value == value) {
					break;
				}
				
				Node next = current.next;
				next.lock.lock();
				
				prev.lock.unlock();
				prev = current;
				current = next;
			}
			
			if(current.value == value) {
				prev.next = current.next;
			}
		}
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		for(String s: hashTable.keySet()) {
			Node current = hashTable.get(s);
			while(current != null) {
				str.append("" + current.value).append(", ");
				current = current.next;
			}
		}
		return str.toString();
	}
	
	private static class Node {
		Node next = null;
		int value;
		ReentrantLock lock = new ReentrantLock();
		
		public Node(int value) {
			this.value = value;
		}
	}


}
