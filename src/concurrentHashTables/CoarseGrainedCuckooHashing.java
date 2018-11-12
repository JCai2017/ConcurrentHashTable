package concurrentHashTables;

//import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.ArrayList;
//import java.util.Optional;
import java.util.Collections;
import java.util.Random;

public class CoarseGrainedCuckooHashing implements TableType {
  //HashMap<Integer, Integer> hashMap = new HashMap<Integer, Integer>();
  private static final int nests = 2;
  //private ArrayList<Integer> hashMap;
  private List<List<Integer>> tables;
  private int size = 0;
  private int maxSize = 13;
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
    //hashMap = new ArrayList<Integer>(Collections.nCopies(maxSize, null));

    a = randy.nextInt();
    a = (a % 2 == 0) ? a + 1 : a;
    b = randy.nextInt();
  }

  @Override
  public void put(int value) {
    lock.lock();

    try {
      //Integer key1 = new Integer(value % maxSize);
      //Integer key2 = new Integer((key1 / maxSize) % maxSize);

        Integer val = new Integer(value);
//        while (!putR(val, 0, 0, size)) {
//          rehash();
//        }

        putR(val, 0, 0, size);

//      int key1 = value % maxSize;
//      int key2 = (key1 / maxSize) % maxSize;
//
//      int position;
//      Integer val;
//      Integer swapval;
//
//      if (hashMap.get(key1) != null && hashMap.get(key1).equals(value)) {
//        // Value already in the hashmap at index key1
//        return;
//      }
//
//      if (hashMap.get(key2) != null && hashMap.get(key2).equals(value)) {
//        // Value already in the hashmap at index key2
//        return;
//      }
//
//      position = key1;
//      val = value;
//
//      // Allow up to size relocations before rehashing
//      for (int i = 0; i <= size; i++) {
//        if (hashMap.get(position) == null) {
//          // Position is empty. Add the value to the hash table.
//          hashMap.set(position, val);
//          size++;
//          return;
//        } else {
//          swapval = hashMap.get(position);
//          hashMap.set(position, val);
//          val = swapval;
//        }
//
//        if (position == key1)
//        {
//          position = key2;
//        }
//      }
      System.out.printf("Cuckoo state:\n%s\n", this);
    } finally {
      lock.unlock();
    }
  }

  // FUCK FUCK FUCK! This will change the array, but some OTHER value may cause it to cycle
  // i.e. 10 may displace 11, then 10 will be added to the table after rehashing instead of 11
//  private boolean putR(Integer val, int tableIdx, int cnt, int max) {
//    int[] positions = new int[nests];
//    Integer swapval;
//
//    if (cnt > max) {
//      System.out.printf("%d unpositioned. Cycle present. REHASH.\n", val);
//      // rehash here instead??
//      return false;
//    }
//
//    // Fill the possible positions for val and check if it is already present at any of the positions
//    // If yes, return true (successfully placed)
//    for (int i = 0; i < nests; i++) {
//      positions[i] = hash(i, val);
//      if (tables.get(i).get(positions[i]) == val) return true;
//    }
//
//    // Put the new key in the table and move the old key if necessary
//    if (tables.get(tableIdx).get(positions[tableIdx]) != null) {
//      //swapval = new Integer(tables.get(tableIdx).get(positions[tableIdx]));
//      swapval = tables.get(tableIdx).get(positions[tableIdx]);
//      tables.get(tableIdx).set(positions[tableIdx], val);
//      return putR(swapval, (tableIdx + 1) % nests, cnt + 1, max);
//    } else {
//      // Successfully add value without any replacements
//      tables.get(tableIdx).set(positions[tableIdx], val);
//      size++;
//      return true;
//    }
//  }

  private void putR(Integer val, int tableIdx, int cnt, int max) {
    int[] positions = new int[nests];
    Integer swapval;

    System.out.printf("Val: %d, TableIdx: %d\n", val, tableIdx);

    if (cnt > max) {
      System.out.printf("%d unpositioned. Cycle present. REHASH.\n", val);
      // rehash here instead??
      rehash();

      putR(val, 0, 0, max);
      //return false;
      return;
    }

    // Fill the possible positions for val and check if it is already present at any of the positions
    // If yes, return true (successfully placed)
    for (int i = 0; i < nests; i++) {
      positions[i] = hash(i, val);
      if (tables.get(i).get(positions[i]) != null && tables.get(i).get(positions[i]).equals(val)) {
        System.out.printf("%d already in the table!\n", val);
        return;
      }
    }

    // Put the new key in the table and move the old key if necessary
    if (tables.get(tableIdx).get(positions[tableIdx]) != null) {
      //swapval = new Integer(tables.get(tableIdx).get(positions[tableIdx]));
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
    //int[] positions = new int[nests];
    int idx;
    try {
      Integer val = new Integer(value);
      // Fill positions
      for (int i = 0; i < nests; i++) {
        //positions[i] = hash(i, val);
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
    //List<List<Integer>> oldtables = new ArrayList<List<Integer>>(tables);
    // want to actually get all vals from the oldtable
    List<Integer> oldVals = getValues();
    int oldMax = maxSize;
    //boolean allMoved = true;

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

    // I think so, since any subsequent/recursive calls to rehash() will set new a and b, which will be the values
    // used in putR for all subsequent things being added... hopefully

    // iterate through the list of values and add to rehashed table
    // THIS IS ALL FUCKED UP BC IF PUTR REQUIRES REHASHING, THEN FUCK! Since rehashing called from inside...
    // Maybe this isn't all fucked up... bc theoretically, we'd want rehashing to happen over only those smaller vals...
    // eventually it SHOULD return and then the next value in the set will get added...
    for (int i = 0; i < oldVals.size(); i++) {
      putR(oldVals.get(i), 0, 0, i);
    }


    // This is all sorts of fucked up bc the values for a and b should be THE SAME FOR ALL THE VALUES IN THE TABLE
    // (otherwise how are we going to find values)
//    a = randy.nextInt();
//    a = (a % 2 == 0) ? a + 1 : a;
//    b = randy.nextInt();
  }

  public int size() {
    return size;
  }

  int hash(int fn, int key) {
    if (fn == 0) {
      return key % maxSize;
    } else {
      //System.out.printf("a: %d, b: %d\n", a, b);
      int x1 = a * key + b;
      //int x2 = (1 << 32) - 1;
      int x2 = (1 << 31 << 1) - 1;
      int x3 = x1 & x2;
      int x4 = x3 % maxSize;

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
  // Remember to resize to prime number!

  @Override
  public String toString() {
    lock.lock();
    try {
      //System.out.printf("Inside toString\n");
      StringBuilder str = new StringBuilder();
      for (int i = 0; i < nests; i++) {
        // print both columns of values
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
