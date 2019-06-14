package edu.hanyang.submit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InternalNode extends Node {
    List<Integer> children;

    InternalNode() {
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
        position = (TinySEBPlusTree.amountOfNodes * TinySEBPlusTree.blocksize);
    }

    InternalNode(int offset) throws IOException {
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
        position = offset;
        TinySEBPlusTree.file.seek(offset);
        TinySEBPlusTree.file.readLong();
        int num_keys = TinySEBPlusTree.file.readInt();
        for (int i = 0; i < num_keys; i++) {
            children.add(TinySEBPlusTree.file.readInt());
            keys.add(TinySEBPlusTree.file.readInt());
        }
        if (children.size() > 0) {
            children.add(TinySEBPlusTree.file.readInt());
        }
    }

    @Override
    void save(int offset) throws IOException {
        TinySEBPlusTree.file.seek(offset);
        TinySEBPlusTree.file.writeLong(0);
        TinySEBPlusTree.file.writeInt(keyNumber());
        int i;
        for (i = 0; i < keyNumber(); i++) {
            TinySEBPlusTree.file.writeLong(children.get(i));
            TinySEBPlusTree.file.writeInt(keys.get(i));
        }
        if (children.size() > 0) {
            TinySEBPlusTree.file.writeLong(children.get(i));
        }

    }

    private Node getChild(Integer key) throws IOException {
        int loc = Collections.binarySearch(keys, key);
        int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
        int offset = children.get(childIndex);
        return TinySEBPlusTree.loadNode(offset);
    }

    private void insertChild(Integer key, Node child) {
        int loc = Collections.binarySearch(keys, key);
        int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
        if (loc >= 0) {
            children.set(childIndex, child.position);
        } else {
            keys.add(childIndex, key);
            children.add(childIndex + 1, child.position);
        }
    }

    @Override
    Integer getValue(Integer key) throws IOException {
        return getChild(key).getValue(key);
    }

    @Override
    void insertValue(Integer key, Integer value) throws IOException {
        Node child = getChild(key);

        child.insertValue(key, value);
        if (child.isOverflow()) {
            Node sibling = child.split();
            insertChild(sibling.getFirstLeafKey(), sibling);
        }
        if (TinySEBPlusTree.root.isOverflow()) {
            Node sibling = split();
            TinySEBPlusTree.amountOfNodes += 1;
            InternalNode newRoot = new InternalNode();
            newRoot.keys.add(sibling.getFirstLeafKey());
            sibling.keys.subList(0, 1).clear();
            sibling.save((TinySEBPlusTree.amountOfNodes - 1) * TinySEBPlusTree.blocksize);
            newRoot.children.add(position);
            newRoot.children.add(sibling.position);
            newRoot.position = TinySEBPlusTree.amountOfNodes * TinySEBPlusTree.blocksize;
            TinySEBPlusTree.root = newRoot;
        } else {
            child.save(child.position);
            save(position);
        }
    }

    @Override
    Node split() throws IOException {
        int from = keyNumber() / 2, to = keyNumber();
        InternalNode sibling = new InternalNode();
        TinySEBPlusTree.amountOfNodes += 1;


        sibling.keys.addAll(keys.subList(from + 1, to));
        sibling.children.addAll(children.subList(from + 1, to + 1));
        sibling.position = TinySEBPlusTree.amountOfNodes * TinySEBPlusTree.blocksize;
        sibling.writeToFileEnd();
        sibling.keys.clear();
        sibling.children.clear();
        sibling.keys.addAll(keys.subList(from, to));
        sibling.children.addAll(children.subList(from + 1, to + 1));

        keys.subList(from, to).clear();
        children.subList(from + 1, to + 1).clear();
        save(position);
        TinySEBPlusTree.amountOfNodes += 1;

        return sibling;
    }

    private void writeToFileEnd() throws IOException {
        save(TinySEBPlusTree.amountOfNodes * TinySEBPlusTree.blocksize);
    }

    @Override
    Integer getFirstLeafKey() throws IOException {
        return getChild(0).getFirstLeafKey();
    }

    @Override
    boolean isOverflow() {
        return children.size() > TinySEBPlusTree.maxKeys;
    }

}