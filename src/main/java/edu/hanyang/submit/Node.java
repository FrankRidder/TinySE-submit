package edu.hanyang.submit;

import java.io.IOException;
import java.util.List;

public abstract class Node {
    List<Integer> keys;
    Integer position;

    int keyNumber() {
        return keys.size();
    }

    abstract Integer getValue(Integer key) throws IOException;

    abstract void insertValue(Integer key, Integer value) throws IOException;

    abstract Integer getFirstLeafKey() throws IOException;

    abstract Node split() throws IOException;

    abstract boolean isOverflow();

    abstract void save(int offset) throws IOException;

    public String toString() {
        return keys.toString();
    }
}