package concurrentHashTables;

public class HashTableTiming {
	private static Thread[] threads = new Thread[3];
	public static void main(String[] args) {
		//Create concurrent hashtables with different hashing schemes
		JavaHashMap javaHashMap = new JavaHashMap();
		CoarseGrainedChainHashing coarseChainTable = new CoarseGrainedChainHashing();
		FineGrainedChainHashing fineChainTable = new FineGrainedChainHashing();
		LockFreeChainHashing lockFreeChainTable = new LockFreeChainHashing();
		CoarseGrainedRobinHoodHashing coarseRobin = new CoarseGrainedRobinHoodHashing();
		FineGrainedRobinHoodHashing fineRobin = new FineGrainedRobinHoodHashing();
		LockFreeRobinHoodHashing lockFreeRobin = new LockFreeRobinHoodHashing();
		
		long start, end;
		// Insert 3000 elements into Hashtables
	    start = System.nanoTime();
		addElements(javaHashMap);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to java ConcurrentHashMap: " + (end - start));
		
		start = System.nanoTime();
		addElements(coarseChainTable);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Coarse-grained locking Hash table with Chaining: " + (end - start));
		
		start = System.nanoTime();
		addElements(fineChainTable);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Fine-grained locking Hash table with Chaining: " + (end - start));
		
		start = System.nanoTime();
		addElements(lockFreeChainTable);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Lock-free Hash table with Chaining: " + (end - start));
		
		start = System.nanoTime();
		addElements(coarseRobin);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Coarse-grained Robin Hood Hashing: " + (end - start));
		
		start = System.nanoTime();
		addElements(fineRobin);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Fine-grained Robin Hood Hashing: " + (end - start));
		
		start = System.nanoTime();
		addElements(lockFreeRobin);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Lock-free Robin Hood Hashing: " + (end - start));
		
		// Delete 3000 elements from Hashtables (?)
		start = System.nanoTime();
		removeElements(javaHashMap);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elementsjava ConcurrentHashMap: " + (end - start));
		
		start = System.nanoTime();
		removeElements(coarseChainTable);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements to Coarse-grained locking Hash table with Chaining: " + (end - start));
		
		start = System.nanoTime();
		removeElements(fineChainTable);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements to Fine-grained locking Hash table with Chaining: " + (end - start));
		
		start = System.nanoTime();
		removeElements(lockFreeChainTable);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements to Lock-free Hash table with Chaining: " + (end - start));
		
		start = System.nanoTime();
		removeElements(coarseRobin);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements to Coarse-grained Robin Hood Hashing: " + (end - start));
		
		start = System.nanoTime();
		removeElements(fineRobin);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements to Fine-grained Robin Hood Hashing: " + (end - start));
		
		start = System.nanoTime();
		removeElements(lockFreeRobin);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements to Lock-free Robin Hood Hashing: " + (end - start));
		
	}
	
	public static void addElements(TableType list) {
		threads[0] = new Thread(new myThreadAdd(list, 0, 15));
		threads[1] = new Thread(new myThreadAdd(list, 10, 20));
		threads[2] = new Thread(new myThreadAdd(list, 20, 30));
		
		threads[1].start(); threads[0].start(); threads[2].start();
        for (Thread thread : threads) {
        	try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
	}
	
	public static void removeElements(TableType list) {
		threads[0] = new Thread(new myThreadRm(list, 0, 15));
		threads[1] = new Thread(new myThreadRm(list, 10, 20));
		threads[2] = new Thread(new myThreadRm(list, 20, 30));
		
		threads[1].start(); threads[0].start(); threads[2].start();
        for (Thread thread : threads) {
        	try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
	}
	
	private static class myThreadAdd implements Runnable {
		
		TableType list;
		int start;
		int end;
		
		public myThreadAdd(TableType list, int start, int end) {
			this.list = list;
			this.start = start;
			this.end = end;
		}

		@Override
		public void run() {
			for(int i = start; i < end; i ++) {
				list.put(i);
			}
		}
		
	}
	
	private static class myThreadRm implements Runnable {
		
		TableType list;
		int start;
		int end;
		
		public myThreadRm(TableType list, int start, int end) {
			this.list = list;
			this.start = start;
			this.end = end;
		}

		@Override
		public void run() {
			for(int i = start; i < end; i ++) {
				list.remove(i);
			}
		}
		
	}
}
