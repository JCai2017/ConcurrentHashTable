package concurrentHashTables;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class FineGrainedCuckooHashing implements TableType {
  private static final int nests = 2;
  private List<List<Node>> tables;

  // Protect size
  private final ReentrantLock slock = new ReentrantLock();
  private int size = 0;


  // Global lock for resizing table
  private final ReentrantLock rlock = new ReentrantLock();
  private final Condition resized = rlock.newCondition();
  private final Condition doneAltering = rlock.newCondition();
  private boolean resizing = false;
  private int rehashDepth = 0;
  private Thread resizingThread = null;
  private int numAltering = 0;

  private int maxSize = 13;
  private Random randy = new Random();
  private int a;
  private int b;


  // Initialize the hash tables
  public FineGrainedCuckooHashing () {
    tables = new ArrayList<>();

    for (int i = 0; i < nests; i++) {
      List<Node> table = new ArrayList<>();

      for (int j = 0; j < maxSize; j++) {
        table.add(new Node(null));
      }

      tables.add(table);
    }

    a = randy.nextInt();
    a = (a % 2 == 0) ? a + 1 : a;
    b = randy.nextInt();
  }

  @Override
  public void put(int value) {
    Integer val = new Integer(value);


    // TODO: LOOK AT SIZE!!!! Maybe take size out and check it inside of putR?
    putR(val, 0, 0);

    //System.out.printf("Cuckoo state:\n%s\n", this);
  }

  private void putR(Integer val, int tableIdx, int cnt) {
    //boolean cycle = false;
    int[] positions = new int[nests];
    Integer swapval;


    //System.out.printf("Val: %d, TableIdx: %d\n", val, tableIdx);
    // Make sure nobody else is trying to rehash
    checkRehashAndUpdateTable();

    // Maybe move these into separate function
    // Check the size of the table
//    slock.lock();
//    try {
//      if (cnt > size) {
//        cycle = true;
//      }
//    } finally {
//      slock.unlock();
//    }

    //if (cycle) {
    if (cnt > getSize()) {
      //System.out.printf("%d unpositioned. Cycle present. REHASH.\n", val);
      rehash();

      putR(val, 0, 0);
      // numAltering gets decremented in rehash()
      return;
    }

    // Fill the possible positions for val and check if it is already present at any of the positions
    // If yes, return true (successfully placed)
    for (int i = 0; i < nests; i++) {
      positions[i] = hash(i, val);

      tables.get(i).get(positions[i]).lock.lock();
      try {
        if (tables.get(i).get(positions[i]).value != null && tables.get(i).get(positions[i]).value.equals(val)) {
          //System.out.printf("%d already in the table!\n", val);
          finishUpdateTable();
          return;
        }
      } finally {
        tables.get(i).get(positions[i]).lock.unlock();
      }
    }

    tables.get(tableIdx).get(positions[tableIdx]).lock.lock();
    try {
      // Put the new key in the table and move the old key if necessary
      if (tables.get(tableIdx).get(positions[tableIdx]).value != null) {
        swapval = new Integer(tables.get(tableIdx).get(positions[tableIdx]).value);
        tables.get(tableIdx).get(positions[tableIdx]).setVal(val);
        finishUpdateTable();
//        tables.get(tableIdx).get(positions[tableIdx]).lock.unlock();
//        putR(swapval, (tableIdx + 1) % nests, cnt + 1);
//        return;
      } else {
        // Successfully add value without any replacements
        tables.get(tableIdx).get(positions[tableIdx]).setVal(val);
//        slock.lock();
//        try {
//          size++;
//        } finally {
//          slock.unlock();
//        }
        incrementSize();  // implicit return here
        finishUpdateTable();
        return;
      }
    } finally {
//      if (tables.get(tableIdx).get(positions[tableIdx]).lock.isHeldByCurrentThread()) {
//        tables.get(tableIdx).get(positions[tableIdx]).lock.unlock();
//      }
      tables.get(tableIdx).get(positions[tableIdx]).lock.unlock();
    }

    // Place the swapped value back into the table
    putR(swapval, (tableIdx + 1) % nests, cnt + 1);
  }

  @Override
  public void remove(int value) {
    //lock.lock();
    checkRehashAndUpdateTable();
    int idx;

    Integer val = new Integer(value);
    // Fill positions
    for (int i = 0; i < nests; i++) {
      //positions[i] = hash(i, val);
      idx = hash(i, val);

      // Acquire lock to check this entry
      tables.get(i).get(idx).lock.lock();
      try {
        if (tables.get(i).get(idx).value != null && tables.get(i).get(idx).value.equals(val)) {
          tables.get(i).get(idx).setVal(null);
          //          slock.lock();
          //          try {
          //            size--;
          //          } finally {
          //            slock.unlock();
          //          }
          decrementSize();
          finishUpdateTable();
          return;
        }
      } finally {
        tables.get(i).get(idx).lock.unlock();
      }
    }

    //System.out.printf("Value %d not found. No value removed.\n", value);
    return;
  }

  // Called by threads that are not rehashing to wait while another rehashes
  private void checkRehashAndUpdateTable() {
    // If current thread is the one rehashing, return immediately
    if (rlock.isHeldByCurrentThread()) {
      return;
    }

    rlock.lock();
    try {
      while (resizing) {
        resized.await();
      }
      numAltering++;
    } catch (InterruptedException e) {
      System.err.printf("Thread checking rehash got interrupted!\n");
      e.printStackTrace();
    } finally {
      rlock.unlock();
    }
  }

  private void finishUpdateTable() {
    // If current thread is the one rehashing, return immediately
    if (rlock.isHeldByCurrentThread()) {
      return;
    }
    rlock.lock();
    try {
      numAltering--;
      // TODO: see if you can replace this with .signal() instead of .signalAll()
      doneAltering.signalAll();
    } finally {
      rlock.unlock();
    }
  }

  // Regrow hash table to capacity 2 * old capacity + 1 and re-insert all key, value pairs
  private void rehash() {
    // Does it matter that thread might already hold rlock?
    rlock.lock();

    try {
      if (resizing != false && !resizingThread.equals(Thread.currentThread())) {
        //System.out.printf("Inside rehash but other thread set resize to true...\n");
      }
      resizing = true;
      rehashDepth++;
      //System.out.printf("RehashDepth: %d\n", rehashDepth);
      resizingThread = Thread.currentThread();
      // Sleep while I'm not the only one trying to alter the table
      while (numAltering > 1) {
        //System.out.printf("numAltering: %d\n", numAltering);
        doneAltering.await();
      }

      // want to actually get all vals from the oldtable
      List<Integer> oldVals = getValues();
      //int oldMax = maxSize;
      //boolean allMoved = true;

      maxSize = (maxSize * 2) + 1;
//      slock.lock();
//      try {
//        size = 0; // re-initialize size for all of the values you add
//      } finally {
//        slock.unlock();
//      }
      resetSize();

      // Initialize new, empty table
      tables = new ArrayList<>();

      for (int i = 0; i < nests; i++) {
        List<Node> table = new ArrayList<Node>();

        for (int j = 0; j < maxSize; j++) {
          table.add(new Node(null));
        }

        tables.add(table);
      }

      a = randy.nextInt();
      a = (a % 2 == 0) ? a + 1 : a;  // Make sure a is odd
      b = randy.nextInt();

      for (int i = 0; i < oldVals.size(); i++) {
        putR(oldVals.get(i), 0, 0);  // This will automatically increment size
      }

      // Only want to do this when done resizing...
      rehashDepth--;
      if (rehashDepth <= 0) {
        numAltering--;  // should have only been incremented the first time (when trying to add the value that triggered initial rehash)
        resizing = false;
        resized.signalAll();
      }
    } catch (InterruptedException e) {
      System.err.printf("Thread trying to rehash got interrupted!\n");
      e.printStackTrace();
    } finally {
      rlock.unlock();
    }
  }

  private int getSize() {
    slock.lock();
    try {
      return size;
    } finally {
      slock.unlock();
    }
  }

  private void incrementSize() {
    slock.lock();
    try {
      size++;
    } finally {
      slock.unlock();
    }
  }

  private void decrementSize() {
    slock.lock();
    try {
      size--;
    } finally {
      slock.unlock();
    }
  }

  private void resetSize() {
    slock.lock();
    try {
      size = 0;
    } finally {
      slock.unlock();
    }
  }

  int hash(int fn, int key) {
    if (fn == 0) {
      return key % maxSize;
    } else {
      //System.out.printf("a: %d, b: %d\n", a, b);

      int hashval = ((a * key + b) & ((1 << 31 << 1) - 1));
      if (hashval < 0) hashval = hashval * -1;

      return hashval % maxSize;
    }
  }

  // when getValues called during rehash, current thread already owns lock
  private List<Integer> getValues() {
    rlock.lock();
    try {
      List<Integer> vals = new ArrayList<Integer>();
      // Be careful with maxSize (make sure not to check it after it's been increased)
      for (int i = 0; i < nests; i++) {
        for (int j = 0; j < maxSize; j++) {
          if (tables.get(i).get(j).value != null) {
            vals.add(tables.get(i).get(j).value);
          }
        }
      }
      return vals;
    } finally {
      rlock.unlock();
    }
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    for (int i = 0; i < nests; i++) {
      // print both columns of values
      for (int j = 0; j < maxSize; j++) {
        if (tables.get(i).get(j).value != null) {
          str.append("" + tables.get(i).get(j).value + "\t");
        } else {
          str.append("\t");
        }
      }
      str.append("\n");  // Add a newline between the 2 rows
    }
    return str.toString();
  }

  private static class Node {
    Integer value;
    ReentrantLock lock = new ReentrantLock();

    public Node(Integer value) {
      this.value = value;
    }

    public void setVal(Integer value) {
      this.value = value;
    }
  }
}
