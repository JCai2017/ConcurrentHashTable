package concurrentHashTables;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Arrays.fill;

public class FineGrainedHopscotchHashing implements TableType {
  private ConcurrentHashMap<Integer, Node> table = new ConcurrentHashMap<>();
  private final ReentrantLock slock = new ReentrantLock();
  private int size = 0;

  private final ReentrantLock rlock = new ReentrantLock();
  private int maxSize = 5000;
  private int H = 2 * getLog2(maxSize);
  private int buckets = (int)Math.ceil((double)maxSize / (double)H);

  // Initialize the hash table
  public FineGrainedHopscotchHashing() {
  }

  @Override
  public void put(int value) {
    if (find(value) != null) {
      System.out.printf("Value %d already in hash table.\n", value);
      return;
    }

    Integer val = new Integer(value);

    Integer key = getNextPos(val);

    while (key == null) {
      resize();
      key = getNextPos(val);
    }

    if (table.get(key) == null) {
      table.put(key, new Node(val));
    } else {
      table.get(key).setVal(val);
    }
    table.get(key).setBit(0,'1');
    incrementSize();

    //System.out.printf("Successfully placed %d at key %d\n", value, key);
  }

  private Integer getNextPos(Integer val) {
    int start = hash(val);
    Integer current = new Integer(start);

    // TODO:
    Integer original = new Integer(val);

    boolean flag0 = false;
    boolean flag1 = false;

    char[] neighborArr;


    // While the buckets we land on are full
    while (table.get(current) != null) {// && table.get(current).value != null) {
      current++;

      // Check if discovered bucket is outside of our neighborhood
      if ((current - start) >= H || ((current < start) && (maxSize - start + current) >= H)) {
        flag0 = true;
      }

      // Find bucket in another neighborhood to begin swapping
      if (flag0 && table.get(start) != null && table.get(start).value != null &&
              ((hash(table.get(start).value) - current) >= H ||
              (current - hash(table.get(start).value)) >= H ||
              ((current - hash(table.get(start).value) < 0) && (maxSize - hash(table.get(start).value) + current) >= H))) {
        flag1 = true;
        current = jumpPos(start);
        if (current == null) {
          // No slot can be found
          // TODO: clean up
          System.out.printf("%d could not be placed. Original val: %d, Starting Key: %d, Bucket size: %d Rehash.\n", hash(table.get(start).value), original, start, H);
          for (int x = 0; x < H; x++) {
            if ((start + x) >= maxSize) break;
            System.out.printf("Key: %d, value: %d \n",start + x, table.get(start + x).value);
          }
          return null;
        }

        start = current;
      }

      if (current >= maxSize) current = 0;

    }

    // TODO:
//    if (current.equals(start)) {
//      System.out.printf("START & CURRENT ARE THE SAME! Start: %d, current: %d\n.", start, current);
//    }

    // Set the neighborhood info
    // flag1 means setting up a bitmap for a value in a different bucket (put start into current)
    if (flag1) {
      if (table.get(current) == null) {
        if (table.get(start) != null && table.get(start).value != null) {
          table.put(current, new Node(table.get(start).value));
          table.get(current).resetBits();
        }
        // TODO: clean up
        if (table.get(start) == null) {
          System.out.printf("Something's wrong with your logic! Start shouldn't be null!!\n\n");
        }
      } else {
        if (table.get(start).value != null) {
          table.get(current).setVal(new Integer(table.get(start).value));
          table.get(current).resetBits();
        }
      }

      // TODO: better logic
      if (table.get(start) == null) {
        neighborArr = new char[H + 2];
      } else {
        neighborArr = new char[table.get(start).bits.length];//[table.get(current).origH];
      }
      table.get(current).resetBits();//.setBitArr(neighborArr);

      // start by copying original neighbor array
      // Start with the bitmap corresponding to this bucket
      if (table.get(start) != null && table.get(start).value != null && table.get(hash(table.get(start).value)) != null) {
        System.arraycopy(table.get(hash(table.get(start).value)).bits, 0, neighborArr, 0, neighborArr.length);
      }

      // Set bit stating that this is occupied
      table.get(current).setBit(0, '1');

      // If location of the bucket being set wraps around to the front of the array
      if (table.get(start) != null && table.get(start).value != null) {
        if (current - hash(table.get(start).value) < 0) {
          // Set this position as occupied
          table.get(current).setBit((maxSize - hash(table.get(start).value) + current + 2), '1');
        } else {
          table.get(current).setBit((current - hash(table.get(start).value) + 2), '1');
        }
      }

      // Update the bitmap for this bucket
      table.get(hash(table.get(start).value)).setBitArr(table.get(current).bits);

      // Clear the bucket that this was moved from
      if (table.get(hash(table.get(start).value)).getBit(start - hash(table.get(start).value) + 2) == '1') {
        table.get(hash(table.get(start).value)).setBit(start - hash(table.get(start).value) + 2, '0');
        //table.get(hash(table.get(start).value)).setBit(0, '0');
        //table.get(hash(table.get(start).value)).resetBits();  // reset bits
//        table.remove(hash(table.get(start).value));
      }

      Integer newVal = new Integer(val);
      if (table.get(start) == null) {
        //TODO
        System.out.printf("This value for start is invalid? Maybe?\n");
        table.put(start, new Node(newVal));
      } else {
        table.get(start).setVal(newVal);
      }
      table.get(start).setBit(0,'1');

      if (table.get(hash(newVal)) != null) {
        table.get(start).setBitArr(table.get(hash(newVal)).bits);
      }
//      } else {
//        table.get(start).resetBits();
//      }

      if (start - hash(newVal) < 0) {
        table.get(start).setBit(maxSize - hash(newVal) + start + 2, '1');
      } else {
        table.get(start).setBit(start - hash(newVal) + 2, '1');
      }

      current = start;
    } else {
      if (start != current) {
        if (table.get(current) == null) {
          table.put(current, new Node(val));
        } else {
          table.get(current).setVal(val);
        }
        table.get(current).resetBits();

        if (table.get(start) != null) {
          table.get(current).setBitArr(table.get(start).bits);
        }

        if (current - start < 0) {
          table.get(current).setBit(maxSize - start + current + 2, '1');
        } else {
          table.get(current).setBit(current - start + 2, '1');
        }
        table.get(current).setBit(0, '1'); // Mark this bit as valid
        //}

        if (table.get(start) != null) {
          table.get(start).setBitArr(table.get(current).bits);
        }
        current = start;
      }
    }
    return current;
  }

  private Integer jumpPos(int start) {
//    System.out.printf("\n\n\n\n\n\n");
    int pos = start + 1;
    Integer c = checkBits(pos);

    while (c == null) {
//      System.out.printf("start: %d\tpos: %d", start, pos);
      if (pos >= maxSize) {
        pos = 0;
        c = checkBits(pos);
      }

      if (pos == start) {
        return null;
      }

      c = checkBits(pos++);
    }

    return c;
  }

  private Integer checkBits(int pos) {
    if (table.get(pos) != null && table.get(pos).value != null && table.get(pos).getBit(0) == '1') {
      for (int i = 2; i < table.get(pos).bits.length; i++) {
        if (table.get(pos).getBit(i) == '1') {
          return new Integer(pos + i);
        }
      }
    }
    return null;
  }


  @Override
  public void remove(int value) {
    int idx;
    Integer val = new Integer(value);
    idx = hash(val);

    // Check all values in the neighborhood
    for (int i = 0; i < H; i++) {
      if ((idx + i) >= maxSize) break;
      if (table.get(idx + i) == null || table.get(idx + i).value == null) continue;
      if (table.get(idx + i).equals(val)) {
        table.remove(idx + i);
        decrementSize();
        return;
      }
    }
    //System.out.printf("Value %d not found. No value removed.\n", value);
    return;

  }

  public Integer find(int value) {
    Integer val = new Integer(value);
    int idx = hash(val);
    Integer realkey;
    for (int i = 0; i < H; i++) {
      if ((idx + i) >= maxSize) break;
      if (table.get(idx + i) == null || table.get(idx + i).value == null) continue;
      if (table.get(idx + i).equals(val)) {
        realkey = new Integer(idx + i);
        //table.get(idx + i).value = null;
        //table.get(idx + i).resetBits();
        return realkey;
      }
    }
    return null;
  }

  public int size() {
    return size;
  }

  private void resize() {
    System.out.printf("Resizing\n");
    return;
  }

  int hash(int key) {
    return key % maxSize;
  }

  private int getLog2(int x) {
    double temp = Math.log((double)x) / Math.log((2.0));
    temp = Math.ceil(temp);
    //System.out.printf("Log is %d\n", (int)temp);
    return (int)temp;
  }


  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    for (int i = 0; i < maxSize; i++) {
      if (table.get(i) != null && table.get(i).value != null) {
        str.append("" + table.get(i).value + "\t");
      } else {
        str.append("\t");
      }
    }
    str.append("\n");  // indicate break between 2 tables
    return str.toString();
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

  private class Node {
    Integer value;
    int origH = H;
    char bits[] = new char[H + 2];

    public Node(Integer val) {
      value = val;
      fill(bits, '0');
    }

    public void resetBits() {
      fill(bits, '0');
    }

    public void resizeBits(int newH) {
      char newBits[] = new char[newH];
      int charsToCopy = (origH < newH) ? origH + 2 : newH + 2;
      System.arraycopy(bits, 0, newBits, 0, charsToCopy);
      bits = newBits;
    }

    public void setBitArr(char[] newBits) {
      System.arraycopy(newBits, 0, bits, 0, bits.length);
    }


    public void setBit(int loc, char val) {
      int actualLoc = bits.length - 1 - loc;
      if (loc >= bits.length) {
        System.err.printf("Desired location larger than array length\n");
        return;
      }

      bits[actualLoc] = val;
    }

    public char getBit(int loc) {
      int actualLoc = bits.length - 1 - loc;
      if (loc >= bits.length) {
        System.err.printf("Desired location larger than array length\n");
        return '\0';
      }

      return bits[actualLoc];
    }

    public void setVal(Integer newVal) {
      value = newVal;
    }

  }
}
