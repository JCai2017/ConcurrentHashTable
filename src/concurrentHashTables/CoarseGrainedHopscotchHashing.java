package concurrentHashTables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Arrays.fill;

public class CoarseGrainedHopscotchHashing implements TableType {
  private List<Node> table;
  private int size = 0;
  private int maxSize = 5000;
  private int H = getLog2(maxSize);

  private final int neighborhoodOffset = 2;
  private int buckets = (int) Math.ceil((double) maxSize / (double) H);

  private final ReentrantLock lock = new ReentrantLock();

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
    Integer val;
    Integer newPos;
    Integer bucket;
    int distanceToNeighborhood;
    Node insertBucket;
    Node neighborhoodBucket;

    try {
      // Hash table already contains value. Return.
      if (get(value)) {
        System.out.printf("%d already in hash table at index %d\n", value, getIdx(value));
        return;
      }

      val = new Integer(value);

      do {
        newPos = getEmptyBucket(val);
        bucket = hash(val);

        if (newPos == null) {
          System.err.printf("Coarse hopscotch -- put: Wasn't able to place %d\n", value);
          resize();
        }
      } while (newPos == null);

      distanceToNeighborhood = getDist(bucket, newPos);

      insertBucket = table.get(newPos);
      neighborhoodBucket = table.get(bucket);

      insertBucket.setVal(val);
      insertBucket.setBit(0, '1');
      // Mark this entry as full in the neighborhood
      neighborhoodBucket.setNeighborBit(distanceToNeighborhood, '1');
      size++;

    } finally {
      lock.unlock();
    }
  }

  private Integer getEmptyBucket(Integer val) {
    int i = hash(val);  // starting bucket
    Integer j = new Integer(i);

    int maxPlacementDist;
    int placementDist;

    Node emptyBucket = table.get(j);

    // While the buckets we land on are full
    while (emptyBucket.value != null) {
      j = nextBucket(j);
      emptyBucket = table.get(j);
    }

    maxPlacementDist = getDist(i, j);

    while (true) {
      placementDist = getDist(i, j);

      if (placementDist > maxPlacementDist) {  // Unable to place val in bucket neighborhood
        return null;
      }

      if (isNeighbor(i, j)) {
        // Can put value at j
        return j;
      }

      j = getCloserSwapBucket(j);

      // No buckets could be swapped closer to i
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
    Node neighborhoodBucket;// = table.get(swapNeighborIdx);
    Node swapCandidateBucket;
    int emptyDistanceFromMainBucket;

    char[] emptyNeighborhood = new char[H];
    Arrays.fill(emptyNeighborhood, '0');

    //while (isNeighbor(swapIdx, j)) {
    for (; isNeighbor(swapNeighborIdx, emptyBucketIdx); swapNeighborIdx = nextBucket(swapNeighborIdx)) {
      // Find a value that can go into this bucket instead
      // Get the neighborhood information
      neighborhoodBucket = table.get(swapNeighborIdx);
      emptyDistanceFromMainBucket = getDist(swapNeighborIdx, emptyBucketIdx);

      if (neighborhoodBucket.bits == null) {
        System.err.printf("Unexpected neighbor array\n");
        continue;
      }

      // Skip empty arrays
      if (neighborhoodBucket.bits.equals(emptyNeighborhood)) {
        continue;
      }

//      // Make sure that the character arrays are properly maintaining neighborhood info
//      if (neighborhoodBucket.getBit(emptyDistanceFromMainBucket) != '0') {
//        //System.err.printf("getSwapBucket: Character array not being properly maintained\n");
//      }

      swapIdx = swapNeighborIdx;
      // Check for the earliest occurrence of a value within this neighborhood
      for (int i = 0; i < H; ++i) {
        swapCandidateBucket = table.get(swapIdx);
        if (neighborhoodBucket.getNeighborBit(i) == '1') {
          if (swapCandidateBucket.value == null) {
            System.err.printf("Value you're trying to move into empty bucket is null!!!\n");
          }
          // Move this value into the empty bucket
          emptyBucket.setVal(new Integer(swapCandidateBucket.value));
          emptyBucket.setBit(0,'1');

          // Set this value as occupied
          neighborhoodBucket.setNeighborBit(emptyDistanceFromMainBucket, '1');

          // Reset the value in this entry
          neighborhoodBucket.setNeighborBit(i, '0');
          swapCandidateBucket.setVal(null);
          swapCandidateBucket.setBit(0, '0');
          return swapIdx;
        }

        swapIdx = nextBucket(swapIdx);  // Set swapIdx to the next
        if (!isNeighbor(swapIdx, emptyBucketIdx)) break;
      }
    }
    return null;
  }

  @Override
  public boolean get(int value) {
    return ((getIdx(value) == null) ? false : true);
  }

  public Integer getIdx(int value) {
    lock.lock();
    int bucket = hash(value);
    int idx = bucket;
    Integer other;
    Node neighborhoodBucket;
    Node occupiedBucket;

    try {
      neighborhoodBucket = table.get(bucket);
      // Check each bit in the neighborhood
      for (int i = 0; i < H; ++i) {
        occupiedBucket = table.get(idx);
        if (neighborhoodBucket.getNeighborBit(i) == '1') {
          if (occupiedBucket.value == null) {
            System.err.printf("Coarse grained: unexpected null value in getIdx\n");
            idx = nextBucket(idx);
            continue;
          }

          //occupiedBucket = table.get(idx);
          if (occupiedBucket.value.equals(value)) {
            return idx;
          }
        }
        idx = nextBucket(idx);
      }
      return null;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void remove(int value) {
    lock.lock();
    Integer idx;
    Integer neighborhoodBucketIdx;
    int distanceToNeighborhood;
    Node neighborhoodBucket;
    Node foundBucket;
    try {
      idx = getIdx(value);

      if (idx == null) {
        System.out.printf("Value %d not found. No value removed.\n", value);
        return;
      }

      foundBucket = table.get(idx);

      neighborhoodBucketIdx = hash(value);
      neighborhoodBucket = table.get(neighborhoodBucketIdx);

      distanceToNeighborhood = getDist(neighborhoodBucketIdx, idx);

      neighborhoodBucket.setNeighborBit(distanceToNeighborhood, '0');
      foundBucket.setVal(null);
      foundBucket.setBit(0, '0');

      size--;
      return;
    } finally {
      lock.unlock();
    }
  }


  public int size() {
    return size;
  }

  private void resize() {
    System.out.printf("Time to resize\n");
    return;
  }

  int hash(int key) {
    key = ((key >>> 16) ^ key) * 0x45d9f3b;
    key = ((key >>> 16) ^ key) * 0x45d9f3b;
    key = (key >>> 16) ^ key;
    return Math.abs(key) % maxSize;
  }


  private int getLog2(int x) {
    double temp = Math.log((double) x) / Math.log((2.0));
    temp = Math.ceil(temp);
    return (int) temp;
  }

  // Get distance from i to j in circular array
  private int getDist(int i, int j) {
    // if j is less than i, assume i wraps around
    return (j < i ? (j + maxSize - i) : (j - i));
  }

  // Deal with circular array
  private boolean isNeighbor(int i, int j) {
    // check if j is within neighborhood of i using circular array
    return (getDist(i,j) < H);
  }

  private int nextBucket(int x) {
    //int next = (x + 1 >= maxSize) ? x + 1 - maxSize : x + 1;
    return ((x + 1 >= maxSize) ? x + 1 - maxSize : x + 1);
  }

  private int prevBucket(int x) {
    return (x <= 0 ? x + maxSize - 1 : x - 1);
  }




  @Override
  public String toString() {
    lock.lock();
    try {
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
    char bits[] = new char[H + neighborhoodOffset];

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
        System.err.printf("Desired location to set bit is larger than array length\n");
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
        System.err.printf("Desired location to get bit larger than array length\n");
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
      if (value != null && newVal != null) {
        System.out.printf("Trying to set %d to %d\n", value, newVal);
      }
      value = newVal;
    }
  }
}
