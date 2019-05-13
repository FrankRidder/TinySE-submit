package edu.hanyang.submit;

import edu.hanyang.indexer.BPlusTree;

public class TinySEBPlusTree implements BPlusTree{

	private int nblocks;

	private Leaf root;       // root of the B-tree
	private int height;      // height of the B-tree

	// helper B-tree node data type
	private static final class Leaf {
		private int size;           // number of key-value pairs in this leaf
		private String[] keys;
		private String[] children;


		private Leaf(int nblock){
			keys = new String[nblock];
			children = new String[nblock];
		}
	}




		@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insert(int key, int val) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void open(String metaPath, String savePath, int blocksize, int nblocks) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int search(int arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

}
