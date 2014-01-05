package com.labs.watermelon.gulp;

import org.junit.Test;

public class ParallelHbaseLoaderTest {

	@Test
	public void test() {
		String file = "/Users/admin/Documents/workspace/HelloMac/log1.txt";
		String keyPattern = "MM/dd/yyyy hh:mm:ss.SSS";
		String fileId = "example";
		String tableName = "testtable";
		String colFamily = "testfam";
		String colName = "testcol";
		int keyStartIndex = 0;
		int keyEndIndex = 23;
		boolean noRegex = true;

		// ParallelHbaseLoader hl = new ParallelHbaseLoader(file, keyPattern,
		// fileId, tableName, colFamily, colName);
		ParallelHbaseLoader hl = new ParallelHbaseLoader(file, keyPattern,
				keyStartIndex, keyEndIndex, noRegex, fileId, tableName,
				colFamily, colName);
		hl.load();
	}
}
