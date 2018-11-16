package concurrentHashTables;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

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
		//Insert 3000 elements into Hashtables
		start = System.nanoTime();
		addElementsOrdered(javaHashMap);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Java ConcurrentHashMap: " + (end - start));

		start = System.nanoTime();
		addElementsOrdered(javaSynch);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Java Collections.synchronizedMap: " + (end - start));

		start = System.nanoTime();
		addElementsOrdered(coarseChainTable);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Coarse-grained locking Hash table with Chaining: " + (end - start));

		start = System.nanoTime();
		addElementsOrdered(fineChainTable);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Fine-grained locking Hash table with Chaining: " + (end - start));

		start = System.nanoTime();
		addElementsOrdered(lockFreeChainTable);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Lock-free Hash table with Chaining: " + (end - start));

		start = System.nanoTime();
		addElementsOrdered(coarseRobin);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Coarse-grained Robin Hood Hashing: " + (end - start));

		start = System.nanoTime();
		addElementsOrdered(fineRobin);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Fine-grained Robin Hood Hashing: " + (end - start));

		start = System.nanoTime();
		addElementsOrdered(lockFreeRobin);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Lock-free Robin Hood Hashing: " + (end - start));

		start = System.nanoTime();
		addElementsOrdered(coarseCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Coarse-grained Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		addElementsOrdered(fineCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Fine-Grained Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		addElementsOrdered(lockFreeCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Lock-Free Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		addElementsOrdered(coarseHopscotch);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Coarse-grained Hopscotch Hashing: " + (end - start));

		start = System.nanoTime();
		addElementsOrdered(fineHopscotch);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements to Fine-Grained Hopscotch Hashing: " + (end - start));

		// Get 3000 elements from Hashtables
		start = System.nanoTime();
		getElementsOrdered(javaHashMap);
		end = System.nanoTime();
		System.out.println("Total time to get 3000 elements from Java ConcurrentHashMap: " + (end - start));

		start = System.nanoTime();
		getElementsOrdered(javaSynch);
		end = System.nanoTime();
		System.out.println("Total time to get 3000 elements from Java Collections.synchronizedMap: " + (end - start));

		start = System.nanoTime();
		getElementsOrdered(coarseChainTable);
		end = System.nanoTime();
		System.out.println("Total time to get 3000 elements from Coarse-grained locking Hash table with Chaining: " + (end - start));

		start = System.nanoTime();
		getElementsOrdered(fineChainTable);
		end = System.nanoTime();
		System.out.println("Total time to get 3000 elements from Fine-grained locking Hash table with Chaining: " + (end - start));

		start = System.nanoTime();
		getElementsOrdered(lockFreeChainTable);
		end = System.nanoTime();
		System.out.println("Total time to get 3000 elements from Lock-free Hash table with Chaining: " + (end - start));

		start = System.nanoTime();
		getElementsOrdered(coarseRobin);
		end = System.nanoTime();
		System.out.println("Total time to get 3000 elements from Coarse-grained Robin Hood Hashing: " + (end - start));

		start = System.nanoTime();
		getElementsOrdered(fineRobin);
		end = System.nanoTime();
		System.out.println("Total time to get 3000 elements from Fine-grained Robin Hood Hashing: " + (end - start));

		start = System.nanoTime();
		getElementsOrdered(lockFreeRobin);
		end = System.nanoTime();
		System.out.println("Total time to get 3000 elements from Lock-free Robin Hood Hashing: " + (end - start));

		start = System.nanoTime();
		getElementsOrdered(coarseCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to get 3000 elements from Coarse-grained Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		getElementsOrdered(fineCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to get 3000 elements from Fine-Grained Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		getElementsOrdered(lockFreeCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to get 3000 elements from Lock-Free Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		getElementsOrdered(coarseHopscotch);
		end = System.nanoTime();
		System.out.println("Total time to get 3000 elements from Coarse-grained Hopscotch Hashing: " + (end - start));

		start = System.nanoTime();
		getElementsOrdered(fineHopscotch);
		end = System.nanoTime();
		System.out.println("Total time to get 3000 elements from Fine-Grained Hopscotch Hashing: " + (end - start));
		
		// Delete 3000 elements from Hashtables
		start = System.nanoTime();
		removeElementsOrdered(javaHashMap);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Java ConcurrentHashMap: " + (end - start));

		start = System.nanoTime();
		removeElementsOrdered(javaSynch);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Java Collections.synchronizedMap: " + (end - start));

		start = System.nanoTime();
		removeElementsOrdered(coarseChainTable);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Coarse-grained locking Hash table with Chaining: " + (end - start));

		start = System.nanoTime();
		removeElementsOrdered(fineChainTable);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Fine-grained locking Hash table with Chaining: " + (end - start));

		start = System.nanoTime();
		removeElementsOrdered(lockFreeChainTable);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Lock-free Hash table with Chaining: " + (end - start));

		start = System.nanoTime();
		removeElementsOrdered(coarseRobin);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Coarse-grained Robin Hood Hashing: " + (end - start));

		start = System.nanoTime();
		removeElementsOrdered(fineRobin);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Fine-grained Robin Hood Hashing: " + (end - start));

		start = System.nanoTime();
		removeElementsOrdered(lockFreeRobin);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Lock-free Robin Hood Hashing: " + (end - start));

		start = System.nanoTime();
		removeElementsOrdered(coarseCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Coarse-grained Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		removeElementsOrdered(fineCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Fine-Grained Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		removeElementsOrdered(lockFreeCuckoo);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Lock-Free Cuckoo Hashing: " + (end - start));

		start = System.nanoTime();
		removeElementsOrdered(coarseHopscotch);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Coarse-grained Hopscotch Hashing: " + (end - start));

		start = System.nanoTime();
		removeElementsOrdered(fineHopscotch);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements from Fine-Grained Hopscotch Hashing: " + (end - start));
		
	}
	
	public static void addElementsOrdered(TableType list) {
		threads[0] = new Thread(new myThreadAdd(list, 0, 1000, false));
		threads[1] = new Thread(new myThreadAdd(list, 1000, 2000, false));
		threads[2] = new Thread(new myThreadAdd(list, 2000, 3000, false));
		
		threads[1].start(); threads[0].start(); threads[2].start();
        for (Thread thread : threads) {
        	try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
	}
	
	public static void getElementsOrdered(TableType list) {
		threads[0] = new Thread(new myThreadGet(list, 0, 1000, false));
		threads[1] = new Thread(new myThreadGet(list, 1000, 2000, false));
		threads[2] = new Thread(new myThreadGet(list, 2000, 3000, false));

		threads[1].start(); threads[0].start(); threads[2].start();
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void removeElementsOrdered(TableType list) {
		threads[0] = new Thread(new myThreadRm(list, 0, 1000, false));
		threads[1] = new Thread(new myThreadRm(list, 1000, 2000, false));
		threads[2] = new Thread(new myThreadRm(list, 2000, 3000, false));
		
		threads[1].start(); threads[0].start(); threads[2].start();
        for (Thread thread : threads) {
        	try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
	}

	public static void addElementsShuffled(TableType list) {
		threads[0] = new Thread(new myThreadAdd(list, 0, 1000, true));
		threads[1] = new Thread(new myThreadAdd(list, 1000, 2000, true));
		threads[2] = new Thread(new myThreadAdd(list, 2000, 3000, true));

		threads[1].start(); threads[0].start(); threads[2].start();
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void getElementsShuffled(TableType list) {
		threads[0] = new Thread(new myThreadGet(list, 0, 1000, true));
		threads[1] = new Thread(new myThreadGet(list, 1000, 2000, true));
		threads[2] = new Thread(new myThreadGet(list, 2000, 3000, true));

		threads[1].start(); threads[0].start(); threads[2].start();
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void removeElementsShuffled(TableType list) {
		threads[0] = new Thread(new myThreadRm(list, 0, 1000, true));
		threads[1] = new Thread(new myThreadRm(list, 1000, 2000, true));
		threads[2] = new Thread(new myThreadRm(list, 2000, 3000, true));

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
		boolean shuffle;
		
		public myThreadAdd(TableType list, int start, int end, boolean shuffle) {
			this.list = list;
			this.start = start;
			this.end = end;
			this.shuffle = shuffle;
		}

		@Override
		public void run() {
			if (!shuffle) {
				for (int i = start; i < end; i++) {
					list.put(i);
				}
			} else {
				int[] array = IntStream.range(start, end).toArray();
				shuffle(array);
				for (int i = 0; i < array.length; i++) {
					list.put(array[i]);
				}
			}
		}
		
	}
	
	private static class myThreadGet implements Runnable {
		
		TableType list;
		int start;
		int end;
		boolean shuffle;
		
		public myThreadGet(TableType list, int start, int end, boolean shuffle) {
			this.list = list;
			this.start = start;
			this.end = end;
			this.shuffle = shuffle;
		}

		@Override
		public void run() {

			if (!shuffle) {
				for (int i = start; i < end; i++) {
					list.get(i);
				}
			} else {
				int[] array = IntStream.range(start, end).toArray();
				shuffle(array);
				for (int i = 0; i < array.length; i++) {
					list.get(array[i]);
				}
			}
		}
		
	}
	
	private static class myThreadRm implements Runnable {
		
		TableType list;
		int start;
		int end;
		boolean shuffle;
		
		public myThreadRm(TableType list, int start, int end, boolean shuffle) {
			this.list = list;
			this.start = start;
			this.end = end;
			this.shuffle = shuffle;
		}

		@Override
		public void run() {

			if (!shuffle) {
				for (int i = start; i < end; i++) {
					list.remove(i);
				}
			} else {
				int[] array = IntStream.range(start, end).toArray();
				shuffle(array);
				for (int i = 0; i < array.length; i++) {
					list.remove(array[i]);
				}
			}
		}
		
	}

	// Implement Fisher-Yates shuffle
	private static void shuffle(int[] array) {
		Random randy = ThreadLocalRandom.current();
		int idx;
		int tmp;
		for (int i = array.length - 1; i > 0; i--) {
			idx = randy.nextInt(i + 1);

			tmp = array[idx];
			array[idx] = array[i];
			array[i] = tmp;
		}

	}
	
}
