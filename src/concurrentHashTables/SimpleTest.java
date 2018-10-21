package concurrentHashTables;

import org.junit.Assert;
import org.junit.Test;

public class SimpleTest {
	@Test
	public void testJavaHashMap() {
		JavaHashMap testHashMap = new JavaHashMap();
		HashTableTiming.addElements(testHashMap);
		Assert.assertEquals(3000, testHashMap.javaHashMap.size());
		HashTableTiming.removeElements(testHashMap);
		Assert.assertEquals(0, testHashMap.javaHashMap.size());
	}
	
	@Test
	public void testCoarseChainHashMap() {
		CoarseGrainedChainHashing testHashMap = new CoarseGrainedChainHashing();
		HashTableTiming.addElements(testHashMap);
		HashTableTiming.removeElements(testHashMap);
	}

}
