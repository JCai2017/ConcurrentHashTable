package concurrentHashTables;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Arrays.fill;

public class FineGrainedHopscotchHashing implements TableType {
  private ConcurrentHashMap<Integer, Node> table = new ConcurrentHashMap<>();
  private final ReentrantLock slock = new ReentrantLock();
  private int size = 0;

  private final ReentrantLock rlock = new ReentrantLock();
  private int maxSize = 5000;
  private final int neighborhoodOffset = 2;
  private int H = 2 * getLog2(maxSize);
  private int buckets = (int)Math.ceil((double)maxSize / (double)H);

  // Initialize the hash table
  public FineGrainedHopscotchHashing() {
  }

  @Override
  public void put(int value) {
    Integer val;
    //Integer key;
    Integer newPos;
    Integer bucketIdx;
    int distanceToNeighborhood;
    Node insertBucket;
    Node neighborhoodBucket;

    if (getIdx(value) != null) {
      System.out.printf("Value %d already in hash table.\n", value);
      return;
    }

    val = new Integer(value);

    newPos = getEmptyBucket(val);
    bucketIdx = hash(val);

    while (newPos == null) {
      resize();
      newPos = getEmptyBucket(val);
      bucketIdx = hash(val);
    }

    distanceToNeighborhood = getDist(bucketIdx, newPos);

    insertBucket = table.get(newPos);

    if (insertBucket == null) {
      table.put(newPos, new Node(val));
      insertBucket = table.get(newPos);
    } else {
      //System.err.printf("Nodes may not be getting deleted properly\n");
      // Just kidding; usually don't want to delete nodes bc they contain neighborhood information
      insertBucket.setVal(val);
    }
    neighborhoodBucket = table.get(bucketIdx);

    insertBucket.setBit(0,'1');
    neighborhoodBucket.setNeighborBit(distanceToNeighborhood, '1');

    incrementSize();

    //TODO
//    if (getSize() != table.size()) {
//      System.err.printf("SIZES DON'T MATCH UP??? May not have successfully added %d\n", val);
//    }

    //System.out.printf("Successfully placed %d at key %d\n", value, key);
  }

  private Integer getEmptyBucket(Integer val) {
    int i = hash(val);
    Integer j = new Integer(i);

    int maxPlacementDist;
    int placementDist;

    Node emptyBucket = table.get(j);
    Node neighborhoodBucket;

    // While buckets are occupied
    while (emptyBucket != null && emptyBucket.value != null) {
      j = nextBucket(j);
      emptyBucket = table.get(j);
    }

    maxPlacementDist = getDist(i, j);

    while (true) {
      placementDist = getDist(i, j);

      // Search out of bounds
      if (placementDist > maxPlacementDist) {
        return null;
      }

      // This empty index is in the neighborhood of hash(val)
      if (isNeighbor(i, j)) {
        return j;
      }

      j = getCloserSwapBucket(j);

      if (j == null) {
        System.out.printf("No bucket closer to %d could be found to place %d\n", i, val);
        return null;
      }
    }
  }

  private Integer getCloserSwapBucket(int emptyBucketIdx) {
    int swapNeighborIdx = (emptyBucketIdx - H + 1 < 0) ? emptyBucketIdx + maxSize - H + 1 : emptyBucketIdx - H + 1;
    int swapIdx;

    Node emptyBucket = table.get(emptyBucketIdx);
    Node neighborhoodBucket;
    Node swapCandidateBucket;
    int emptyDistanceFromMainBucket;

    char[] emptyNeighborhood = new char[H];
    Arrays.fill(emptyNeighborhood, '0');

    for (; isNeighbor(swapNeighborIdx, emptyBucketIdx); swapNeighborIdx = nextBucket(swapNeighborIdx)) {
      neighborhoodBucket = table.get(swapNeighborIdx);
      emptyDistanceFromMainBucket = getDist(swapNeighborIdx, emptyBucketIdx);

      if (neighborhoodBucket == null || neighborhoodBucket.bits == null) {
        System.err.printf("Neighborhood bucket node is unexpectedly empty\n");
        continue;
      }

      if (neighborhoodBucket.bits.equals(emptyNeighborhood)) {
        continue;
      }

      swapIdx = swapNeighborIdx;

      for (int i = 0; i < H; ++i) {
        swapCandidateBucket = table.get(swapIdx);
        if (neighborhoodBucket.getNeighborBit(i) == '1') {
          if (swapCandidateBucket == null || swapCandidateBucket.value == null) {
            System.err.printf("\n\n\nThe value you're trying to swap with is null!!\n\n\n");
          }

          if (emptyBucket == null) {
            table.put(emptyBucketIdx, new Node(new Integer(swapCandidateBucket.value)));
            emptyBucket = table.get(emptyBucketIdx);
          } else {
            emptyBucket.setVal(new Integer(swapCandidateBucket.value));
          }
          emptyBucket.setBit(0, '1');

          neighborhoodBucket.setNeighborBit(emptyDistanceFromMainBucket, '1');

          neighborhoodBucket.setNeighborBit(i, '0');  // race condition is probably happening here
          swapCandidateBucket.setVal(null);
          swapCandidateBucket.setBit(0, '0');

          return swapIdx;
        }
        swapIdx = nextBucket(swapIdx);
        if (!isNeighbor(swapIdx, emptyBucketIdx)) break;
      }

    }
    return null;
  }

  @Override
  public void remove(int value) {
    Integer idx;
    Integer neighborhoodBucketIdx;
    int distanceToNeighborhood;
    Node neighborhoodBucket;
    Node foundBucket;
    //Integer val = new Integer(value);

    idx = getIdx(value);

    if (idx == null) {
//      System.out.printf("Fine-grained: Value %d not found in expected bucket %d. No value removed.\n", value, hash(value));
      return;
    }

    foundBucket = table.get(idx);

    neighborhoodBucketIdx = hash(value);
    neighborhoodBucket = table.get(neighborhoodBucketIdx);

    distanceToNeighborhood = getDist(neighborhoodBucketIdx, idx);

//    // Check all values in the neighborhood
//    for (int i = 0; i < H; i++) {
//      if ((idx + i) >= maxSize) break;
//      if (table.get(idx + i) == null || table.get(idx + i).value == null) continue;
//      if (table.get(idx + i).equals(val)) {
//        table.remove(idx + i);
//        decrementSize();
//        return;
//      }
//    }
    //System.out.printf("Value %d not found. No value removed.\n", value);

    neighborhoodBucket.setNeighborBit(distanceToNeighborhood, '0');
    foundBucket.setVal(null);
    foundBucket.setBit(0, '0');
    decrementSize();
    return;
  }

  public Integer getIdx(int value) {
    Integer val = new Integer(value);
    int bucket = hash(val);
    int idx = bucket;
    Node neighborhoodBucket;
    Node occupiedBucket;
    //Integer realkey;

    neighborhoodBucket = table.get(bucket);
    if (neighborhoodBucket == null) {
      return null;
    }

    for (int i = 0; i < H; ++i) {
      occupiedBucket = table.get(idx);
      if (neighborhoodBucket.getNeighborBit(i) == '1') {
        if (occupiedBucket == null || occupiedBucket.value == null) {
          System.err.printf("Unexpected null value in getIdx\n");
          idx = nextBucket(idx);
          continue;
        }

        if (occupiedBucket.value.equals(value)) {
          return idx;
        }
      }
      idx = nextBucket(idx);
    }

    return null;
  }

  public boolean get(int value) {
    return ((getIdx(value) == null) ? false : true);
  }

  public int size() {
    return size;
  }

  private void resize() {
    System.out.printf("Resizing\n");
    return;
  }

  int hash(int key) {
    return (key % 1500 % buckets) * H;
  }

  private int getLog2(int x) {
    double temp = Math.log((double)x) / Math.log((2.0));
    temp = Math.ceil(temp);
    //System.out.printf("Log is %d\n", (int)temp);
    return (int)temp;
  }

  // Get distance from i to j in circular array
  private int getDist(int i, int j) {
    // if j is less than i, assume i wraps around
    return (j < i ? (j + maxSize - i) : (j - i));
  }

  // Deal with circular array
  private boolean isNeighbor(int i, int j) {
    // check if j is within neighborhood of i using circular array
    return (getDist(i,j) < H);//(j < i ? (j + maxSize - i < H) : (j - i < H));
  }

  private int nextBucket(int x) {
    int next = (x + 1 >= maxSize) ? x + 1 - maxSize : x + 1;
    if (next > 4990) {
      //System.out.printf("Something fishy.\n");
    }
    return ((x + 1 >= maxSize) ? x + 1 - maxSize : x + 1);
  }

  private int prevBucket(int x) {
    return (x <= 0 ? x + maxSize - 1 : x - 1);
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
    volatile Integer value;
    int origH = H;
    volatile char bits[] = new char[H + neighborhoodOffset];

    public Node(Integer val) {
      value = val;
      fill(bits, '0');
    }

    public void resetBits() {
      fill(bits, '0');
    }

    public void resizeBits(int newH) {
      char newBits[] = new char[newH];
      int charsToCopy = (origH < newH) ? origH + neighborhoodOffset : newH + neighborhoodOffset;
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

    public void setNeighborBit(int neighborDist, char val) {
      int actualLoc = bits.length - 3 - neighborDist;
      if (neighborDist >= H) {
        System.err.printf("Desired location to get bit larger than neighborhood length\n");
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

    public char getNeighborBit(int loc) {
      int actualLoc = bits.length - 3 - loc;
      if (loc >= bits.length) {
        System.err.printf("Desired location to get bit is larger than neighborhood length\n");
        return '\0';
      }
      return bits[actualLoc];
    }

    public void setVal(Integer newVal) {
      value = newVal;
    }

  }
}
