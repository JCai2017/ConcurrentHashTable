package concurrentHashTables;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/* Modified from Nguyen and Tsigas, "Lock-Free Cuckoo Hashing" (2014) */
public class LockFreeCuckooHashing implements TableType {
  private static final int nests = 2;
  private List<List<AtomicStampedReference<Node>>> tables;

  // Protect size
  private final ReentrantLock slock = new ReentrantLock();
  private AtomicInteger size = new AtomicInteger();


  // Global lock for resizing table
  private final ReentrantLock rlock = new ReentrantLock();
  //private final Condition resized = rlock.newCondition();
  //private final Condition doneAltering = rlock.newCondition();
  //private boolean resizing = false;
  //private int rehashDepth = 0;
  //private Thread resizingThread = null;
  //private int numAltering = 0;

  private int maxSize = 5000;
  private final int maxCycle = 200;
  private Random randy = new Random();
  private int a;
  private int b;


  // Initialize the hash tables
  public LockFreeCuckooHashing () {
    tables = new ArrayList<>();

    for (int i = 0; i < nests; i++) {
      List<AtomicStampedReference<Node>> table = new ArrayList<>();

      for (int j = 0; j < maxSize; j++) {
        //table.add(new Node(null));
        table.add(new AtomicStampedReference<>(null,0));
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
    int idx0 = hash(0, val);
    int idx1 = hash(1, val);

    if (get(value)) {
      System.out.printf("%d already in hash table.\n", value);
      return;
    }

    Node node = new Node(val);

    AtomicStampedReference<Node> n0 = new AtomicStampedReference<>(null,0);
    AtomicStampedReference<Node> n1 = new AtomicStampedReference<>(null,0);

    Integer tab;
    boolean reloc = false;

    while (true) {
      tab = find(val, n0, n1);

      if (tab != null) {
        // Update node for val with new "value"
        // (our value is the Integer, so that doesn't change)
//        System.out.printf("Lock-free cuckoo state:\n%s\n", this);
        return;
      }

      // If the desired slot is empty
      if (n0.getReference() == null) {
        if (!tables.get(0).get(idx0).compareAndSet(n0.getReference(), node, n0.getStamp(), n0.getStamp())) {
          // try again
          continue;
        }
        size.getAndIncrement();
//        System.out.printf("Lock-free cuckoo state:\n%s\n", this);
        return;
      }

      if (n1.getReference() == null) {
        if (!tables.get(1).get(idx1).compareAndSet(n1.getReference(), node, n1.getStamp(), n1.getStamp())) {
          continue;
        }
        size.getAndIncrement();
//        System.out.printf("Lock-free cuckoo state:\n%s\n", this);
        return;
      }

      // If we have to relocate

      reloc = relocate(0, idx0);

      if (reloc) {
        continue;
      } else {
        rehash();
      }

    }
  }


  @Override
  public void remove(int value) {
    Integer val = new Integer(value);
    int idx0 = hash(0, val);
    int idx1 = hash(1, val);

    AtomicStampedReference<Node> n0 = new AtomicStampedReference<>(null,0);
    AtomicStampedReference<Node> n1 = new AtomicStampedReference<>(null,0);

    Integer tab;
    while (true) {
      tab = find(val, n0, n1);

      if (tab == null) {
        System.out.printf("Value %d not in table. No values removed.\n", val);
        return;
      }

      if (tab.equals(0)) {
        if (tables.get(0).get(idx0).compareAndSet(n0.getReference(), null, n0.getStamp(), n0.getStamp())) {
          size.getAndDecrement();
          return;
        }
      } else if (tab.equals(1)) {
        if (!(tables.get(0).get(idx0).getReference().equals(n0.getReference()) && tables.get(0).get(idx0).getStamp() != n0.getStamp())) {
          continue;
        }
        if (tables.get(1).get(idx1).compareAndSet(n1.getReference(), null, n1.getStamp(), n1.getStamp())) {
          size.getAndDecrement();
          return;
        }
      } else {
        System.err.printf("Invalid node found!\n");
        continue;
      }
    }
  }



  private void rehash() {
    // TODO
  }

  int hash(int fn, int key) {
    if (fn == 0) {
      return key % maxSize;//key % 1499 % maxSize;
    } else {
      //System.out.printf("a: %d, b: %d\n", a, b);

      int hashval = ((a * key + b) & ((1 << 31 << 1) - 1));
      if (hashval < 0) hashval = hashval * -1;

      return hashval % maxSize;
    }
  }


  // Find all entries for put or remove
  public Integer find(Integer val, AtomicStampedReference<Node> t0ref, AtomicStampedReference<Node> t1ref) {

    int idx0 = hash(0, val);
    int idx1 = hash(1, val);
    Node n0a, n1a, n0b, n1b;
    int[] s0a = new int[1];
    int[] s1a = new int[1];
    int[] s0b = new int[1];
    int[] s1b = new int[1];

    Integer foundTable = null;//new Integer(null);

    while (true) {
      // First-round query
      n0a = tables.get(0).get(idx0).get(s0a);
      t0ref.set(n0a, s0a[0]);
      if (n0a != null) {
        if (n0a.relocating) {
          helpReloc(0, idx0, false);
          continue;
        }

        if (n0a.value != null && n0a.value.equals(val)){
          foundTable = new Integer(0);
        }
      }

      n1a = tables.get(1).get(idx1).get(s1a);
      t1ref.set(n1a, s1a[0]);
      if (n1a != null) {
        if (n1a.relocating) {
          helpReloc(0, idx1, false);
          continue;
        }
        if (n1a.value != null && n1a.value.equals(val)) {
          if (foundTable != null && foundTable.equals(0)) {
            delDupe(idx0, t0ref, idx1, t1ref);
          } else {
            foundTable = new Integer(1);
          }
        }
      }

      if (foundTable != null && (foundTable.equals(0) || foundTable.equals(1))) {
        return foundTable;
      }

      // Second-round query
      n0b = tables.get(0).get(idx0).get(s0b);
      t0ref.set(n0b, s0b[0]);
      if (n0b != null) {
        if (n0b.relocating) {
          helpReloc(0, idx0, false);
          continue;
        }

        if (n0b.value != null && n0b.value.equals(val)){
          foundTable = new Integer(0);
        }
      }

      n1b = tables.get(1).get(idx1).get(s1b);
      t1ref.set(n1b, s1b[0]);
      if (n1b != null) {
        if (n1b.relocating) {
          helpReloc(0, idx1, false);
          continue;
        }
        if (n1b.value != null && n1b.value.equals(val)) {
          if (foundTable.equals(0)) {
            delDupe(idx0, t0ref, idx1, t1ref);
          } else {
            foundTable = new Integer(1);
          }
        }
      }

      if (foundTable != null && (foundTable.equals(0) || foundTable.equals(1))) {
        return foundTable;
      }

      if (checkCounter(s0a, s1a, s0b, s1b)) {
        continue;
      } else {
        return null;//new Integer(null);
      }
    }
  }


  // Determine whether the key is in the tables
  public boolean get(Integer val) {
    int idx0 = hash(0, val);
    int idx1 = hash(1, val);

    Node n0a, n1a, n0b, n1b;
    int[] s0a = new int[1];
    int[] s1a = new int[1];
    int[] s0b = new int[1];
    int[] s1b = new int[1];

    while (true) {
      // First-round query
      n0a = tables.get(0).get(idx0).get(s0a);
      if (n0a != null && n0a.value != null && n0a.value.equals(val)) {
        return true;
      }

      n1a = tables.get(1).get(idx1).get(s1a);
      if (n1a != null && n1a.value != null && n1a.value.equals(val)) {
        return true;
      }

      // Second-round query
      n0b = tables.get(0).get(idx0).get(s0b);
      if (n0b != null && n0b.value != null && n0b.value.equals(val)) {
        return true;
      }

      n1b = tables.get(1).get(idx1).get(s1b);
      if (n1b != null && n1b.value != null && n1b.value.equals(val)) {
        return true;
      }
      if (checkCounter(s0a, s1a, s0b, s1b)) {
        continue;
      } else {
        return false;
      }
    }
  }

  private boolean checkCounter(int[] t0sa, int[] t1sa, int[] t0sb, int[] t1sb) {
    return ((t0sb[0] >= t0sa[0] + 2) && (t1sb[0] >= t1sa[0] + 2) && (t1sb[0] >= t0sa[0] + 3));
  }


  private void helpReloc(int tableIdx, int index, boolean initiator) {
    Node src;
    int[] s0 = new int[1];

    Node dst;
    int[] d0 = new int[1];

    int hd;

    int nCnt;
    AtomicStampedReference<Node> s, d;
    while (true) {
      src = tables.get(tableIdx).get(index).get(s0);
      while (src == null || (initiator && !src.relocating)) {
        if (src == null) return;

        tables.get(tableIdx).get(index).compareAndSet(src, new Node(src, true), s0[0], s0[0]);
        src = tables.get(tableIdx).get(index).get(s0);
      }

      if (src == null || !src.relocating) return;

      hd = hash(1 - tableIdx, src.value);

      dst = tables.get(1 - tableIdx).get(hd).get(d0);

      if (dst == null) {
        nCnt = (s0[0] > d0[0]) ? (s0[0] + 1) : (d0[0] + 1);
        s = new AtomicStampedReference<>(src, s0[0]);
        d = new AtomicStampedReference<>(dst, d0[0]);
        if (!(tables.get(tableIdx).get(index).getReference().equals(s.getReference()) && tables.get(tableIdx).get(index).getStamp() != s.getStamp())) {
          continue;
        }

        if (tables.get(1 - tableIdx).get(hd).compareAndSet(dst, src, d0[0], nCnt)) {
          tables.get(tableIdx).get(index).compareAndSet(src, null, s0[0], s0[0] + 1);
          return;
        }
      }

      if (src.equals(dst)) {
        tables.get(tableIdx).get(index).compareAndSet(src, null, s0[0], s0[0] + 1);
        return;
      }

      tables.get(tableIdx).get(index).compareAndSet(src, new Node(src, false), s0[0], s0[0] + 1);
    }
  }

  private void delDupe(int idx0, AtomicStampedReference<Node> t0ref, int idx1, AtomicStampedReference<Node> t1ref) {
    // Delete duplicate entries
    if (tables.get(0).get(idx0).getReference() == null || tables.get(1).get(idx1).getReference() == null) {
      return;
    }

    // If the entries in the tables are not the same as the passed in references, continue
    if (!(tables.get(0).get(idx0).getReference().equals(t0ref.getReference()) && tables.get(0).get(idx0).getStamp() != t0ref.getStamp()) &&
    !(tables.get(1).get(idx1).getReference().equals(t1ref.getReference()) && tables.get(1).get(idx1).getStamp() != t1ref.getStamp())) {
      return;
    }

    // If the two values are different, return
    if (t0ref.getReference() != null && t0ref.getReference() != null && !(t0ref.getReference().value.equals(t1ref.getReference().value))) {
      return;
    }

    tables.get(1).get(idx1).compareAndSet(t1ref.getReference(), null, t1ref.getStamp(), t1ref.getStamp());
  }

  private boolean relocate(int tableIdx, int index) {
    int threshold = (size.get() > maxCycle) ? maxCycle : size.get();
    int[] route = new int[threshold];
    int startLevel = 0;
    int tbl = tableIdx;
    int idx = index;

    boolean found;
    int depth;

    Node n0, n1;
    int[] s0 = new int[1];
    int[] s1 = new int[1];

    Node pre = null;
    int[] sp = new int[1];
    int preIdx = -1;  // want to fail here

    AtomicStampedReference<Node> n;
    AtomicStampedReference<Node> p;

    Integer key;

    int destIdx;

    boolean backToPath = false;

    while (true) {
      found = false;
      depth = startLevel;

      do {
        n0 = tables.get(tbl).get(idx).get(s0);

        while (n0 != null && n0.relocating) {
          helpReloc(tbl, idx, false);
          n0 = tables.get(tbl).get(idx).get(s0);
        }

        n = new AtomicStampedReference<>(n0, s0[0]);
        p = new AtomicStampedReference<>(pre, sp[0]);
        //if (((pre != null) && (n0 != null)) && ((pre.equals(n0) && sp[0] == (s0[0])) || pre.value.equals(n0.value))) {
        if (p.getReference() != null && n.getReference() != null && ((p.getReference().equals(n.getReference()) && p.getStamp() == n.getStamp()) || p.getReference().value.equals(n.getReference().value))) {
          if (tbl == 0) {
            delDupe(idx, n, preIdx, p);
          } else {
            delDupe(preIdx, p, idx, n);
          }
        }

        if (n0 != null) {
          route[depth] = idx;
          key = n0.value;
          pre = n0;
          sp[0] = s0[0];
          preIdx = idx;
          tbl = 1 - tbl;
          idx = (tbl == 0) ? hash(0, key) : hash(1, key);
        } else {
          // reached the end of the path
          found = true;
        }
      //System.out.printf("Depth: %d", depth);
      } while (!found && ++depth < threshold);

      if (found) {
        backToPath = false;
        tbl = 1 - tbl;
        for (int i = depth - 1; i >= 0; --i, tbl = 1 - tbl) {
          idx = route[i];
          n0 = tables.get(tbl).get(idx).get(s0);
          n = new AtomicStampedReference<>(n0, s0[0]);

          if (n.getReference() != null && n.getReference().relocating) {
            helpReloc(tbl, idx, false);
            n0 = tables.get(tbl).get(idx).get(s0);
            n = new AtomicStampedReference<>(n0, s0[0]);
          }

          if (n.getReference() == null) continue;

          destIdx = (tbl == 0) ? hash(1, n0.value) : hash(0, n0.value);
          n1 = tables.get(1 - tbl).get(destIdx).get(s1);

          if (n1 != null) {
            startLevel = i + 1;
            idx = destIdx;
            tbl = 1 - tbl;
            backToPath = true;
            break;
          }
          helpReloc(tbl, idx, false);
        }
        if (!backToPath) break;
      }
    }
    return found;
  }

  public int size() {
    return size.get();
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    for (int i = 0; i < nests; i++) {
      // print both columns of values
      for (int j = 0; j < maxSize; j++) {
        if (tables.get(i).get(j).getReference() != null) {
          str.append("" + tables.get(i).get(j).getReference().value + "\t");
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
    int relocations = 0;
    boolean relocating = false;
    boolean newTable = false;

    public Node(Integer value) {
      this.value = value;
    }

    public Node(Node other, boolean relo) {
      value = new Integer(other.value);
      relocating = relo;
    }

    public void setVal(Integer value) {
      this.value = value;
    }

    public void setRelo() {
      relocating = true;
    }

    public void clearRelo() {
      relocating = false;
    }

    public void setNewTable() {
      newTable = true;
    }
    public void clearNewTable() {
      newTable = false;
    }
  }
}

