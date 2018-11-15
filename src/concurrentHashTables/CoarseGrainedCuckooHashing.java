package concurrentHashTables;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class CoarseGrainedCuckooHashing implements TableType {
  private static final int nests = 2;
  private List<List<Integer>> tables;
  private int size = 0;
  private int maxSize = 13;
  private int maxCycle = 200;
  private Random randy = new Random();
  private int a;
  private int b;

  private static final ReentrantLock lock = new ReentrantLock();

  // Initialize the hash tables
  public CoarseGrainedCuckooHashing () {
    tables = new ArrayList<>();

    for (int i = 0; i < nests; i++) {
      tables.add(new ArrayList<>(Collections.nCopies(maxSize, null)));
    }

    a = randy.nextInt();
    a = (a % 2 == 0) ? a + 1 : a;
    b = randy.nextInt();
  }

  @Override
  public void put(int value) {
    lock.lock();

    try {
        Integer val = new Integer(value);

        int cycleSize = (size > maxCycle) ? maxCycle : size;

        putR(val, 0, 0, cycleSize);

    } finally {
      lock.unlock();
    }
  }


  private void putR(Integer val, int tableIdx, int cnt, int max) {
    int[] positions = new int[nests];
    Integer swapval;


    if (cnt > max) {
      //System.out.printf("%d unpositioned. Cycle present. REHASH.\n", val);
      rehash();

      putR(val, 0, 0, max);
      return;
    }

    // Fill the possible positions for val and check if it is already present at any of the positions
    // If yes, return true (successfully placed)
    for (int i = 0; i < nests; i++) {
      positions[i] = hash(i, val);
      if (tables.get(i).get(positions[i]) != null && tables.get(i).get(positions[i]).equals(val)) {
        //System.out.printf("%d already in the table!\n", val);
        return;
      }
    }

    // Put the new key in the table and move the old key if necessary
    if (tables.get(tableIdx).get(positions[tableIdx]) != null) {
      swapval = tables.get(tableIdx).get(positions[tableIdx]);
      tables.get(tableIdx).set(positions[tableIdx], val);
      putR(swapval, (tableIdx + 1) % nests, cnt + 1, max);
      return;
    } else {
      // Successfully add value without any replacements
      tables.get(tableIdx).set(positions[tableIdx], val);
      size++;
    }
  }

  @Override
  public void remove(int value) {
    lock.lock();
    int idx;
    try {
      Integer val = new Integer(value);
      // Fill positions
      for (int i = 0; i < nests; i++) {
        idx = hash(i, val);

        if (tables.get(i).get(idx) != null && tables.get(i).get(idx).equals(val)) {
          tables.get(i).set(idx, null);
          size--;
          return;
        }

      }
      //System.out.printf("Value %d not found. No value removed.\n", value);
      return;
    } finally {
      lock.unlock();
    }
  }

  // Regrow hash table to capacity 2 * old capacity + 1 and re-insert all key, value pairs
  private void rehash() {
    List<Integer> oldVals = getValues();

    maxSize = (maxSize * 2) + 1;
    size = 0; // re-initialize size for all of the values you add
    tables = new ArrayList<>();

    for (int i = 0; i < nests; i++) {
      tables.add(new ArrayList<>(Collections.nCopies(maxSize, null)));
    }

    // Maybe if this happens here, we're safe...
    a = randy.nextInt();
    a = (a % 2 == 0) ? a + 1 : a;
    b = randy.nextInt();

    for (int i = 0; i < oldVals.size(); i++) {
      int cycleSize = (i > maxCycle) ? maxCycle : i;
      putR(oldVals.get(i), 0, 0, cycleSize);
    }

  }

  public int size() {
    return size;
  }

  int hash(int fn, int key) {
    if (fn == 0) {
      return key % 1500 % maxSize;
    } else {
      //System.out.printf("a: %d, b: %d\n", a, b);

      int hashval = ((a * key + b) & ((1 << 31 << 1) - 1));
      if (hashval < 0) hashval = hashval * -1;

      return hashval % maxSize;
    }
  }

  // Lock should be acquired by caller
  private List<Integer> getValues() {
    List<Integer> vals = new ArrayList<Integer>();
    // Be careful with maxSize (make sure not to check it after it's been increased)
    for (int i = 0; i < nests; i++) {
      for (int j = 0; j < maxSize; j++) {
        if (tables.get(i).get(j) != null) {
          vals.add(tables.get(i).get(j));
        }
      }
    }
    return vals;
  }

  @Override
  public String toString() {
    lock.lock();
    try {
      StringBuilder str = new StringBuilder();
      for (int i = 0; i < nests; i++) {
        for (int j = 0; j < maxSize; j++) {
          if (tables.get(i).get(j) != null) {
            str.append("" + tables.get(i).get(j) + "\t");
          } else {
            str.append("\t");
          }
        }
        str.append("\n");  // indicate break between 2 tables
      }
      return str.toString();
    } finally {
      lock.unlock();
    }
  }
}
