package edu.hanyang.submit;

import java.io.*;

import edu.hanyang.indexer.BPlusTree;

public class TinySEBPlusTree implements BPlusTree {
    static Node root;
    private static int root_index;
    static Integer maxKeys;
    static Integer blocksize;
    static RandomAccessFile file;
    static Integer amountOfNodes;
    private String metapath;

    public TinySEBPlusTree() {
    }


    public void open(String metapath, String savepath, int blocksize, int nblocks) throws IOException {
        this.metapath = metapath;
        TinySEBPlusTree.blocksize = blocksize;
        maxKeys = blocksize / Integer.SIZE;
        root_index = 0;

        File meta = new File(metapath);
        if (meta.exists() && meta.length() > 0) {
            DataInputStream is;

            is = new DataInputStream(new BufferedInputStream(new FileInputStream(metapath), blocksize));

            root_index = is.readInt();
            maxKeys = is.readInt();
            TinySEBPlusTree.blocksize = is.readInt();

        } else {
            meta.createNewFile();
        }

        File save = new File(savepath);
        if (!save.exists()) {
            save.createNewFile();

        }

        file = new RandomAccessFile(savepath, "rw");

        TinySEBPlusTree.amountOfNodes = 0;

        if (file.length() > 0) {
            TinySEBPlusTree.root = loadNode(root_index);
        } else {
            TinySEBPlusTree.root = new LeafNode();
        }

    }

    public void insert(int key, int val) throws IOException {

        root.insertValue(key, val);

    }

    public int search(int toFind) throws IOException {
        return root.getValue(toFind);
    }

    static Node loadNode(int key) throws IOException {
        file.seek(key);
        long type = file.readLong();
        if (type == 1) {
            return new LeafNode(key);
        } else {
            return new InternalNode(key);
        }
    }

    public void close() throws IOException {
        root_index = root.position;
        File meta = new File(metapath);
        if (!meta.exists()) {

            meta.createNewFile();

        } else {

            meta.delete();
            meta.createNewFile();
        }
        DataOutputStream os;

        os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metapath, true), blocksize));

        os.writeLong(root_index);
        os.writeInt(maxKeys);
        os.writeInt(blocksize);
        os.close();
        root.save(root_index);
    }

}
