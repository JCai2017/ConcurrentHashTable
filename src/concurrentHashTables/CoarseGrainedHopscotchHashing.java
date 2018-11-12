package concurrentHashTables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Arrays.fill;

public class CoarseGrainedHopscotchHashing implements TableType {
  //HashMap<Integer, Integer> hashMap = new HashMap<Integer, Integer>();
  //private ArrayList<Integer> hashMap;
  //private List<List<Integer>> tables;
  private List<Node> table;
  private int size = 0;
  private int maxSize = 5000;
  private int H = getLog2(maxSize);
  private int buckets = (int)Math.ceil((double)maxSize / (double)H);
  private Random randy = new Random();
  private int a;
  private int b;

  private static final ReentrantLock lock = new ReentrantLock();

  // Initialize the hash table
  public CoarseGrainedHopscotchHashing() {
    table = new ArrayList<>();

    for (int i = 0; i < maxSize; i++) {
      table.add(new Node(null));
    }
  }

  @Override
  public void put(int value) {
    lock.lock();

    try {
      Integer val = new Integer(value);

      Integer curr = findNextPos(val);

      if (curr == null) {
        resize();
      }

      table.get(curr).setVal(val);
      table.get(curr).setBit(0,'1');
      size++;

    } finally {
      lock.unlock();
    }
  }


  private Integer findNextPos(Integer val) {
    int start = hash(val);
    Integer current = new Integer(start);

    boolean flag0 = false;
    boolean flag1 = false;

    char[] neighborArr;


    // While the buckets we land on are full
    while (table.get(current).value != null) {
      ++current;

      // Check if discovered bucket is outside of our neighborhood
      if (current - start >= H || ((current < start) && (table.size() - start + current) >= H)) {
        flag0 = true;
      }

      // Find bucket in another neighborhood to begin swapping
      if (flag0 && ((hash(table.get(start).value) - current) >= H ||
                    (current - hash(table.get(start).value)) >= H ||
                    ((current - hash(table.get(start).value) < 0) && (table.size() - hash(table.get(start).value) + current) >= H))) {
        flag1 = true;
        current = jumpPos(start);
        if (current == null) {
          return null;
        }

        start = current;
      }

      if (current >= table.size()) current = 0;

    }

    // Set the neighborhood info
    // flag1 means setting up a bitmap for a value in a different bucket
    if (flag1) {
      table.get(current).setVal(new Integer(table.get(start).value));
      table.get(current).resetBits();

      neighborArr = new char[table.get(current).origH];
      table.get(current).setBitArr(neighborArr);

      // start by copying original neighbor array
      // Start with the bitmap corresponding to this bucket
      System.arraycopy(table.get(hash(table.get(start).value)).bits, 0, neighborArr, 0, neighborArr.length);

      // Set bit stating that this is occupied
      table.get(current).setBit(0, '1');

      // If location of the bucket being set wraps around to the front of the array
      if (current - hash(table.get(start).value) < 0) {
        // Set this position as occupied
        table.get(current).setBit((table.size() - hash(table.get(start).value) + current + 2), '1');
      } else {
        table.get(current).setBit((current - hash(table.get(start).value) + 2), '1');
      }

      table.get(hash(table.get(start).value)).setBitArr(table.get(current).bits);

      // Clear the bucket that this was moved from
      if (table.get(hash(table.get(start).value)).getBit(start - hash(table.get(start).value) + 2) == '1') {
        table.get(hash(table.get(start).value)).setBit(start - hash(table.get(start).value) + 2, '0');
        table.get(hash(table.get(start).value)).setBit(0, '0');
        table.get(hash(table.get(start).value)).resetBits();  // reset bits
      }

      Integer newVal = new Integer(val);
      table.get(start).setVal(newVal);
      //table.get(start).setBit(0, '1');

      table.get(start).setBitArr(table.get(hash(newVal)).bits);

      if (start - hash(newVal) < 0) {
        table.get(start).setBit(table.size() - hash(newVal) + start + 2, '1');
      } else {
        table.get(start).setBit(start - hash(newVal) + 2, '1');
      }

      current = start;

    } else {
      if (start != current) {
        table.get(current).setVal(val);
        table.get(current).resetBits();

        table.get(current).setBitArr(table.get(start).bits);

        if (current - start < 0) {
          table.get(current).setBit(table.size() - start + current + 2, '1');
        } else {
          table.get(current).setBit(current - start + 2, '1');
        }
        table.get(current).setBit(0, '1'); // Mark this bit as valid
      }

      table.get(start).setBitArr(table.get(current).bits);
      current = start;
    }
    return current;
  }

  private Integer jumpPos(int start) {
    int pos = start + 1;
    Integer c = checkBits(pos);

    while (c == null) {
      if (pos >= table.size()) {
        pos = 0;
        c = checkBits(pos);
      }

      if (pos == start) {
        return null;
      }

      c = checkBits(++pos);
    }

    return c;
  }

  private Integer checkBits(int pos) {
    if (table.get(pos).value != null && table.get(pos).getBit(0) == '1') {
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
    lock.lock();
    int idx;
    try {
      Integer val = new Integer(value);
      idx = hash(val);
      // Fill positions
      for (int i = 0; i <= H; i++) {
        if (table.get(idx + i) == null) continue;
        if (table.get(idx + i).equals(val)) {
          table.get(idx + i).value = null;
          table.get(idx + i).resetBits();
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


  public int size() {
    return size;
  }

  private void resize() {
    return;
  }

  int hash(int key) {
    return key % maxSize % buckets;
  }

  private int getLog2(int x) {
    double temp = Math.log((double)x) / Math.log((2.0));
    temp = Math.ceil(temp);
    //System.out.printf("Log is %d\n", (int)temp);
    return (int)temp;
  }


  @Override
  public String toString() {
    lock.lock();
    try {
      //System.out.printf("Inside toString\n");
      StringBuilder str = new StringBuilder();
      for (int i = 0; i < maxSize; i++) {
        if (table.get(i).value != null) {
          str.append("" + table.get(i).value + "\t");
        } else {
          str.append("\t");
        }
      }
      str.append("\n");  // indicate break between 2 tables
      return str.toString();
    } finally {
      lock.unlock();
    }
  }

  private class Node {
    Integer value;
    int origH = H;
    char bits[] = new char[H + 2];

    public Node(Integer val) {
      //bits = Collections.nCopies(origH, '0');
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
      //bits = newBits;

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
      //bits[actualLoc] = val;
    }

    public void setVal(Integer newVal) {
      value = newVal;
    }


  }
}
