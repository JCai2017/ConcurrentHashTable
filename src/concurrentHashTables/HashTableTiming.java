package concurrentHashTables;

public class HashTableTiming {
	private static Thread[] threads = new Thread[3];
	public static void main(String[] args) {
		//Create concurrent hashtables with different hashing schemes
		JavaHashMap javaHashMap = new JavaHashMap();
		
		long start, end;
		// Insert 3000 elements into Hashtables
		start = System.nanoTime();
		addElements(javaHashMap);
		end = System.nanoTime();
		System.out.println("Total time to add 3000 elements: " + (end - start));
		
		
		// Delete 3000 elements from Hashtables (?)
		start = System.nanoTime();
		removeElements(javaHashMap);
		end = System.nanoTime();
		System.out.println("Total time to remove 3000 elements: " + (end - start));
	}
	
	public static void addElements(ListType list) {
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
	
	public static void removeElements(ListType list) {
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
		
		ListType list;
		int start;
		int end;
		
		public myThreadAdd(ListType list, int start, int end) {
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
		
		ListType list;
		int start;
		int end;
		
		public myThreadRm(ListType list, int start, int end) {
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
