package edu.hanyang.submit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class LeafNode extends Node {
    private List<Integer> values;
    private LeafNode next;

    // create a new Node at the end of the file
    LeafNode() {
        keys = new ArrayList<>();
        values = new ArrayList<>();
        position = TinySEBPlusTree.amountOfNodes * TinySEBPlusTree.blocksize;
    }

    // load the Node from the file at the offset
    LeafNode(Integer offset) throws IOException {
        keys = new ArrayList<>();
        values = new ArrayList<>();
        position = offset;
        TinySEBPlusTree.file.seek(offset);
        TinySEBPlusTree.file.readLong();
        int num_keys = TinySEBPlusTree.file.readInt();
        for (int i = 0; i < num_keys; i++) {
            keys.add(TinySEBPlusTree.file.readInt());
            values.add(TinySEBPlusTree.file.readInt());
        }
    }

    @Override
    void save(int offset) throws IOException {
        TinySEBPlusTree.file.seek(offset);
        TinySEBPlusTree.file.writeLong(1);
        TinySEBPlusTree.file.writeInt(keyNumber());
        for (int i = 0; i < keyNumber(); i++) {
            TinySEBPlusTree.file.writeInt(keys.get(i));
            TinySEBPlusTree.file.writeInt(values.get(i));
        }
    }

    @Override
    Integer getValue(Integer key) {
        int loc = Collections.binarySearch(keys, key);
        return loc >= 0 ? values.get(loc) : -1;
    }

    @Override
    void insertValue(Integer key, Integer value) throws IOException {
        int loc = Collections.binarySearch(keys, key);
        int valueIndex = loc >= 0 ? loc : -loc - 1;
        if (loc >= 0) {
            values.set(valueIndex, value);
        } else {
            keys.add(valueIndex, key);
            values.add(valueIndex, value);
        }
        if (TinySEBPlusTree.root.isOverflow()) {
            Node sibling = split();
            TinySEBPlusTree.amountOfNodes += 1;
            InternalNode newRoot = new InternalNode();
            newRoot.keys.add(sibling.getFirstLeafKey());
            newRoot.children.add(position);
            newRoot.children.add(sibling.position);
            newRoot.save(newRoot.position);
            TinySEBPlusTree.root = newRoot;
            TinySEBPlusTree.amountOfNodes += 1;
        }
    }

    @Override
    Integer getFirstLeafKey() {
        return keys.get(0);
    }

    @Override
    Node split() throws IOException {
        LeafNode sibling = new LeafNode();
        int from = keyNumber() / 2, to = keyNumber();
        sibling.keys.addAll(keys.subList(from, to));
        sibling.values.addAll(values.subList(from, to));
        keys.subList(from, to).clear();
        values.subList(from, to).clear();
        sibling.next = next;
        next = sibling;
        save(position);
        TinySEBPlusTree.amountOfNodes += 1;
        sibling.position = TinySEBPlusTree.amountOfNodes * TinySEBPlusTree.blocksize;
        sibling.writeToFileEnd();
        sibling.save(sibling.position);
        return sibling;
    }

    @Override
    boolean isOverflow() {
        return values.size() > TinySEBPlusTree.maxKeys - 1;
    }

    private void writeToFileEnd() throws IOException {
        save(TinySEBPlusTree.amountOfNodes * TinySEBPlusTree.blocksize);
    }

}