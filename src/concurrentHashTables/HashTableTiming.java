package concurrentHashTables;

public class HashTableTiming {
	private static Thread[] threads = new Thread[3];
	public static void main(String[] args) {
		//Create concurrent hashtables with different hashing schemes
		JavaHashMap javaHashMap = new JavaHashMap();
		JavaSynchronizedMap javaSynch = new JavaSynchronizedMap();
		CoarseGrainedChainHashing coarseChainTable = new CoarseGrainedChainHashing();
		FineGrainedChainHashing fineChainTable = new FineGrainedChainHashing();
		LockFreeChainHashing lockFreeChainTable = new LockFreeChainHashing();
		CoarseGrainedRobinHoodHashing coarseRobin = new CoarseGrainedRobinHoodHashing();
		FineGrainedRobinHoodHashing fineRobin = new FineGrainedRobinHoodHashing();
		LockFreeRobinHoodHashing lockFreeRobin = new LockFreeRobinHoodHashing();
		CoarseGrainedCuckooHashing coarseCuckoo = new CoarseGrainedCuckooHashing();
		FineGrainedCuckooHashing fineCuckoo = new FineGrainedCuckooHashing();
		LockFreeCuckooHashing lockFreeCuckoo = new LockFreeCuckooHashing();
		CoarseGrainedHopscotchHashing coarseHopscotch = new CoarseGrainedHopscotchHashing();
		FineGrainedHopscotchHashing fineHopscotch = new FineGrainedHopscotchHashing();
		
		long start, end;
		// Insert 3000 elements into Hashtables
	    start = System.nanoTime();
		addElements(javaHashMap);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Java ConcurrentHashMap: " + (end - start));
		
		start = System.nanoTime();
		addElements(javaSynch);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Java Collections.synchronizedMap: " + (end - start));
		
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

		start = System.nanoTime();
		addElements(coarseCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements from Coarse-grained Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		addElements(fineCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements from Fine-Grained Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		addElements(lockFreeCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements from Lock-Free Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		addElements(coarseHopscotch);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Coarse-grained Hopscotch Hashing: " + (end - start));

		start = System.nanoTime();
		addElements(fineHopscotch);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Fine-Grained Hopscotch Hashing: " + (end - start));
		
		// Delete 3000 elements from Hashtables (?)
		start = System.nanoTime();
		removeElements(javaHashMap);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Java ConcurrentHashMap: " + (end - start));
		
		start = System.nanoTime();
		removeElements(javaSynch);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Java Collections.synchronizedMap: " + (end - start));
		
		start = System.nanoTime();
		removeElements(coarseChainTable);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Coarse-grained locking Hash table with Chaining: " + (end - start));
		
		start = System.nanoTime();
		removeElements(fineChainTable);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Fine-grained locking Hash table with Chaining: " + (end - start));
		
		start = System.nanoTime();
		removeElements(lockFreeChainTable);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Lock-free Hash table with Chaining: " + (end - start));
		
		start = System.nanoTime();
		removeElements(coarseRobin);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Coarse-grained Robin Hood Hashing: " + (end - start));
		
		start = System.nanoTime();
		removeElements(fineRobin);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Fine-grained Robin Hood Hashing: " + (end - start));
		
		start = System.nanoTime();
		removeElements(lockFreeRobin);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Lock-free Robin Hood Hashing: " + (end - start));

		start = System.nanoTime();
		removeElements(coarseCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Coarse-grained Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		removeElements(fineCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Fine-Grained Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		removeElements(lockFreeCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Lock-Free Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		removeElements(coarseHopscotch);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Coarse-grained Hopscotch Hashing: " + (end - start));

		start = System.nanoTime();
		removeElements(fineHopscotch);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Fine-Grained Hopscotch Hashing: " + (end - start));
		
	}
	
	public static void addElements(TableType list) {
		threads[0] = new Thread(new myThreadAdd(list, 0, 1000));
		threads[1] = new Thread(new myThreadAdd(list, 1000, 2000));
		threads[2] = new Thread(new myThreadAdd(list, 2000, 3000));
		
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
		threads[0] = new Thread(new myThreadRm(list, 0, 1000));
		threads[1] = new Thread(new myThreadRm(list, 1000, 2000));
		threads[2] = new Thread(new myThreadRm(list, 2000, 3000));
		
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
