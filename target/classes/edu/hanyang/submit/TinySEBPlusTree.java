package edu.hanyang.submit;

import edu.hanyang.indexer.BPlusTree;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

public class TinySEBPlusTree implements BPlusTree {

    private Block root;

    private int maxKeys;
    private RandomAccessFile save;

    private Queue<Block> nodes = new LinkedList<>();

    private class Block {
        int n_keys;
        int type;
        int child_pointer;
        ArrayList<Integer> keys;
        ArrayList<Integer> values;
        Block parent;
        ArrayList<Block> children;


        private Block(int n_keys, int type, ArrayList<Integer> keys, ArrayList<Integer> values, int child_pointer, Block parent, ArrayList<Block> children) {
            this.n_keys = n_keys;
            this.type = type; // 0 = nonleaf, 1 = leaf 2 = root
            this.child_pointer = child_pointer;
            if (keys == null) {
                this.keys = new ArrayList<>();
            } else {
                this.keys = keys;
            }
            if (values == null) {
                this.values = new ArrayList<>();
            } else {
                this.values = values;
            }
            this.parent = parent;
            if (children == null) {
                this.children = new ArrayList<>();
            } else {
                this.children = children;
            }

        }
    }

    @Override
    public void close() throws IOException {
        nodes.add(root);
        while (!nodes.isEmpty()) {
            toFile(nodes.poll());
        }
        save.close();

    }

    @Override
    public void insert(int key, int val) throws IOException {
        Block currentBlock = root;
        if (currentBlock.type == 1) {
            if(currentBlock.n_keys == 0){
                currentBlock.keys.add(0, key);
                currentBlock.values.add(0, val);
                currentBlock.n_keys++;
            }
            if (key < currentBlock.keys.get(0)) {
                currentBlock.keys.add(0, key);
                currentBlock.values.add(0, val);
                currentBlock.n_keys++;
                if (currentBlock.n_keys > maxKeys) {
                    split(currentBlock);
                }
            } else if (key > currentBlock.keys.get(currentBlock.n_keys - 1)) {
                currentBlock.keys.add(currentBlock.n_keys - 1, key);
                currentBlock.values.add(currentBlock.n_keys - 1, val);
                currentBlock.n_keys++;
                if (currentBlock.n_keys > maxKeys) {
                    split(currentBlock);
                }
            } else {
                for (int i = 0; i < currentBlock.n_keys - 1; i++) {
                    if (key > currentBlock.keys.get(i) && key < currentBlock.keys.get(i + 1)) {
                        currentBlock.keys.add(i + 1, key);
                        currentBlock.values.add(i + 1, val);
                        currentBlock.n_keys++;
                        break;
                    }
                }
                if (currentBlock.n_keys > maxKeys) {
                    split(currentBlock);
                }
            }
        } else {
            while (true) {
                if (currentBlock.type == 0) {
                    if (key < currentBlock.keys.get(0)) {
                        currentBlock = currentBlock.children.get(0);
                    } else if (key > currentBlock.keys.get(currentBlock.n_keys - 1)) {
                        currentBlock = currentBlock.children.get(currentBlock.n_keys - 1);
                    } else {
                        for (int i = 0; i < currentBlock.n_keys - 1; i++) {
                            if (key > currentBlock.keys.get(i) && key < currentBlock.keys.get(i + 1)) {
                                currentBlock = currentBlock.children.get(i + 1);
                            }
                        }
                    }
                }
                if (currentBlock.type == 1) {
                    if (key < currentBlock.keys.get(0)) {
                        currentBlock.keys.add(0, key);
                        currentBlock.values.add(0, val);
                        currentBlock.n_keys++;
                        if (currentBlock.n_keys > maxKeys) {
                            split(currentBlock);
                        }
                    } else if (key > currentBlock.keys.get(currentBlock.n_keys - 1)) {
                        currentBlock.keys.add(currentBlock.n_keys, key);
                        currentBlock.values.add(currentBlock.n_keys, val);
                        currentBlock.n_keys++;
                        if (currentBlock.n_keys > maxKeys) {
                            split(currentBlock);
                        }
                    } else {
                        for (int i = 0; i < currentBlock.n_keys - 1; i++) {
                            if (key > currentBlock.keys.get(i) && key < currentBlock.keys.get(i + 1)) {
                                currentBlock.keys.add(i + 1, key);
                                currentBlock.values.add(i + 1, val);
                                currentBlock.n_keys++;
                                break;
                            }
                        }
                        if (currentBlock.n_keys > maxKeys) {
                            split(currentBlock);
                        }
                    }
                    break;
                }
            }
        }

    }

    @Override
    public void open(String metaFilePath, String saveFilePath, int blocksize, int nblocks) throws IOException {

        Queue<Integer> child_nums = new LinkedList<>();

        int numberOfPointers, currentKey, currentValue, numberOfChildren;

        Block parent_node, new_node;

        byte[] buf = new byte[blocksize];
        ByteBuffer buffer = ByteBuffer.wrap(buf);
        maxKeys = blocksize / 4;

        save = new RandomAccessFile(saveFilePath, "rw");
        RandomAccessFile meta = new RandomAccessFile(metaFilePath, "rw");


        root = new Block(0, 1, null, null, 0, null, null);

        buffer.put(Integer.toString(0).getBytes());

        writeToFile(meta, buffer, 0);

        buffer.put(Integer.toString(nblocks).getBytes());

        writeToFile(meta, buffer, (Integer.SIZE));

        buffer.put(Integer.toString(maxKeys).getBytes());

        writeToFile(meta, buffer, (Integer.SIZE * 2));

        if (save.length() != 0) {
            if (save.readInt() == 0) {
                root.type = 0;
            }
            numberOfPointers = save.readInt();
            if (root.type == 0) {
                numberOfPointers--;
            }
            for (int i = 0; i < numberOfPointers; i++) {
                currentKey = save.readInt();
                currentValue = save.readInt();
                if (currentKey > 0 && currentValue > 0) {
                    root.keys.add(i, currentKey);
                    root.values.add(i, currentValue);
                    root.n_keys++;
                } else if (currentKey > 0 && currentValue == 0) {
                    root.keys.add(i, currentKey);
                    root.n_keys++;
                }
            }

            if (root.type == 0) {
                numberOfPointers++;
                nodes.offer(root);
                child_nums.offer(numberOfPointers);
            }

            while (!child_nums.isEmpty()) {
                numberOfChildren = child_nums.poll();
                parent_node = nodes.poll();
                for (int i = 0; i < numberOfChildren; i++) {

                    new_node = new Block(0, 0, null, null, 0, null, null);

                    if (save.readInt() == 0) {
                        new_node.type = 0;
                    }
                    numberOfPointers = save.readInt();

                    if (new_node.type == 0) {
                        numberOfPointers--;
                    }
                    for (int j = 0; j < numberOfPointers; j++) {
                        currentKey = save.readInt();
                        currentValue = save.readInt();
                        if (currentKey > 0 && currentValue > 0) {
                            new_node.keys.add(i, currentKey);
                            new_node.values.add(i, currentValue);
                            new_node.n_keys++;
                        } else if (currentKey > 0 && currentValue == 0) {
                            new_node.keys.add(i, currentKey);
                            new_node.n_keys++;
                        }
                    }
                    new_node.child_pointer = i;
                    new_node.parent = parent_node;
                    if (parent_node != null) {
                        parent_node.children.add(new_node);
                    }
                    if (new_node.type == 0) {
                        numberOfPointers++;
                        nodes.offer(new_node);
                        child_nums.offer(numberOfPointers);
                    }
                }
            }
        }
    }


    private int search_(int keyToFind, Block toSearch) {

        if (toSearch.type == 1) {
            for (int i = 0; i < toSearch.n_keys - 1; i++) {
                if (toSearch.keys.get(i) == keyToFind) {
                    return toSearch.values.get(i);
                }
            }
            return -1;
        } else {
            for (int i = 0; i < toSearch.n_keys - 1; i++) {
                if (keyToFind < toSearch.keys.get(i)) {
                    return search_(keyToFind, toSearch.children.get(i));
                }
            }
            return search_(keyToFind, toSearch.children.get(toSearch.n_keys));
        }
    }

    @Override
    public int search(int key) {
        return search_(key, root);
    }

    private void writeToFile(RandomAccessFile file, ByteBuffer data, int position)
            throws IOException {

        file.seek(position);
        file.write(data.get());
        data.clear();

    }

    private void toFile(Block toSave) throws IOException {

        if (toSave.type == 1) {
            save.writeInt(1);
            save.writeInt(toSave.n_keys);
            for (int i = 0; i < toSave.n_keys - 1; i++) {
                save.writeInt(toSave.keys.get(i));
                save.writeInt(toSave.values.get(i));
            }
        } else {
            save.writeInt(0);
            save.writeInt(toSave.n_keys + 1);
            for (int i = 0; i < toSave.n_keys; i++) {
                save.writeInt(toSave.keys.get(i));
                save.writeInt(0);
            }

            for (int i = 0; i < toSave.n_keys + 1; i++) {
                nodes.offer(toSave.children.get(i));
            }
        }

    }

    private void split(Block node) {
        Block parent;

        Block left = new Block(0, 1, null, null, 0, null, null);
        Block right = new Block(0, 1, null, null, 0, null, null);
        int new_n_keys;

        if (node.parent == null) {
            parent = new Block(0, 0, null, null, 0, null, null);
            root = parent;
        } else {
            parent = node.parent;
        }

        if (node.type == 1) {
            if (node.n_keys % 2 == 0) {
                new_n_keys = node.n_keys / 2;
            } else {
                new_n_keys = node.n_keys / 2 + 1;
            }

            for (int i = 0; i < new_n_keys; i++) {
                left.keys.add(node.keys.get(i));
                left.values.add(node.values.get(i));
                left.n_keys++;
            }

            left.child_pointer = node.child_pointer;
            left.parent = parent;

            for (int i = new_n_keys; i < node.n_keys; i++) {
                right.keys.add(node.keys.get(i));
                right.values.add(node.values.get(i));
                right.n_keys++;
            }

            right.child_pointer = node.child_pointer + 1;
            right.parent = parent;
            parent.keys.add(node.child_pointer, node.keys.get(new_n_keys));
            parent.n_keys++;

            try {
                parent.children.set(node.child_pointer, left);
            } catch (Exception e) {
                parent.children.add(node.child_pointer, left);
            }
            parent.children.add(node.child_pointer + 1, right);

            if (left.child_pointer < parent.n_keys) {
                for (int i = right.child_pointer + 1; i <= parent.n_keys; i++) {
                    parent.children.get(i).child_pointer++;
                }
            }

            if (parent.n_keys > maxKeys) {
                split(parent);
            }

        } else {
            left.type = 0;
            right.type = 0;
            new_n_keys = node.n_keys / 2;
            int position_index = 0;

            for (int i = 0; i < new_n_keys; i++) {
                left.keys.add(node.keys.get(i));
                left.n_keys++;
                left.children.add(node.children.get(i));
                left.children.get(position_index).child_pointer = position_index;
                left.children.get(position_index).parent = left;
                position_index++;
            }

            left.children.add(node.children.get(new_n_keys));
            left.children.get(position_index).child_pointer = position_index;
            left.children.get(position_index).parent = left;
            left.child_pointer = node.child_pointer;
            left.parent = parent;

            position_index = 0;

            for (int i = new_n_keys + 1; i < node.n_keys; i++) {
                right.keys.add(node.keys.get(i));
                right.n_keys++;
                right.children.add(node.children.get(i));
                right.children.get(position_index).child_pointer = position_index;
                right.children.get(position_index).parent = right;
                position_index++;
            }

            right.children.add(node.children.get(node.n_keys));
            right.children.get(position_index).child_pointer = position_index;
            right.children.get(position_index).parent = right;
            right.child_pointer = node.child_pointer + 1;
            right.parent = parent;

            parent.keys.add(node.child_pointer, node.keys.get(new_n_keys));
            parent.n_keys++;

            try {
                parent.children.set(node.child_pointer, left);
            } catch (Exception e) {
                parent.children.add(node.child_pointer, left);
            }
            parent.children.add(node.child_pointer + 1, right);

            if (left.child_pointer < parent.n_keys) {
                for (int i = right.child_pointer + 1; i <= parent.n_keys; i++) {
                    parent.children.get(i).child_pointer++;
                }
            }
            if (parent.n_keys > maxKeys) {
                split(parent);
            }
        }

    }

    private void show(Block root) {
        if (root.type == 1) {
            for (int i = 0; i < root.n_keys; i++) {
                System.out.println(root.keys.get(i));
            }
            System.out.println("@" + root.n_keys + " " + root.child_pointer);
        } else {
            for (int i = 0; i < root.children.size(); i++) {
                show(root.children.get(i));
            }
        }
    }

    public static void main(String[] args) throws IOException {
        String metapath = "./tmp/bplustree.meta";
        String savepath = "./tmp/bplustree.tree";
        int blocksize = 52;
        int nblocks = 10;

        File treefile = new File(savepath);
        if (treefile.exists()) {
            if (!treefile.delete()) {
                System.err.println("error: cannot remove files");
                System.exit(1);
            }
        }

        TinySEBPlusTree tree = new TinySEBPlusTree();
        tree.open(metapath, savepath, blocksize, nblocks);

        tree.insert(5, 10);
        tree.insert(6, 15);
        tree.insert(4, 20);
        tree.insert(7, 1);
        tree.insert(8, 5);
        tree.insert(17, 7);
        tree.insert(30, 8);
        tree.insert(1, 8);
        tree.insert(58, 1);
        tree.insert(25, 8);
        tree.insert(96, 32);
        tree.insert(21, 8);
        tree.insert(9, 98);
        tree.insert(57, 54);
        tree.insert(157, 54);
        tree.insert(247, 54);
        tree.insert(357, 254);
        tree.insert(557, 54);
        tree.show(tree.root);
        tree.close();
    }
}
