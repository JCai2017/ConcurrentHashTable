package concurrentHashTables;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

public class FineGrainedChainHashing implements TableType {
	public HashMap<Integer, Node> hashTable = new HashMap<>();
	
	public FineGrainedChainHashing() {
		for(int i = 0; i < 1500; i++) {
			hashTable.put(i, new Node());
		}
	}

	@Override
	public void put(int value) {	
		Integer key = value % 1500;
		Node n = new Node(value);
		Node current = hashTable.get(key);
		current.lock.lock();
		if(current.value == -1) {
			current.value = value;
			current.lock.unlock();
			return;
		}
		
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
		Integer key = value % 1500;
		Node current = hashTable.get(key);
		if(current != null) {
			current.lock.lock();
			if(current.value == value) {
				hashTable.put(key, current.next);
				current.lock.unlock();
				return;
			} else if(current.next == null) {
				current.lock.unlock();
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
				prev.lock.unlock();
			}
			
			current.lock.unlock();
		}
	}
	
	@Override
	public boolean get(int value) {
		int key = value % 1500;
		Node current = hashTable.get(key);
		if(current != null) {
			while(current.next != null) {
				if(current.value == value)
					return true;
			}
		}
		
		if(current.value == value) 
			return true;
		
		return false;
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		for(Integer i: hashTable.keySet()) {
			Node current = hashTable.get(i);
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
		
		public Node() {
			this.value = -1;
		}
		
		public Node(int value) {
			this.value = value;
		}
	}


}
