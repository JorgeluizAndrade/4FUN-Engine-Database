package com.engine.fundatabase.storage;

import java.util.ArrayList;
import java.util.List;

/**
 * B-Tree index with support for duplicate keys.
 */
public class BTreeIndex<V> implements java.io.Serializable {

    private final int minimumDegree;
    private Node<V> root;

    public BTreeIndex(int minimumDegree) {
        if (minimumDegree < 2) {
            throw new IllegalArgumentException("minimumDegree must be >= 2");
        }
        this.minimumDegree = minimumDegree;
        this.root = new Node<>(true);
    }

    public void insert(Object key, V value) {
        Comparable<Object> comparableKey = asComparable(key);

        if (root.isFull(minimumDegree)) {
            Node<V> newRoot = new Node<>(false);
            newRoot.children.add(root);
            splitChild(newRoot, 0);
            root = newRoot;
        }
        insertNonFull(root, comparableKey, value);
    }

    public List<V> search(Object key) {
        Comparable<Object> comparableKey = asComparable(key);
        return search(root, comparableKey);
    }

    public List<V> searchGreaterThan(Object key, boolean includeEquals) {
        Comparable<Object> lowerBound = asComparable(key);
        List<V> result = new ArrayList<>();
        collectRange(root, lowerBound, includeEquals, null, true, result);
        return result;
    }

    public List<V> searchLessThan(Object key, boolean includeEquals) {
        Comparable<Object> upperBound = asComparable(key);
        List<V> result = new ArrayList<>();
        collectRange(root, null, true, upperBound, includeEquals, result);
        return result;
    }

    public List<V> searchNotEqual(Object key) {
        Comparable<Object> excluded = asComparable(key);
        List<V> all = new ArrayList<>();
        collectAll(root, all);

        List<V> equalMatches = search(key);
        if (equalMatches.isEmpty()) {
            return all;
        }

        List<V> result = new ArrayList<>(all);
        for (V value : equalMatches) {
            result.remove(value);
        }
        return result;
    }

    private void collectAll(Node<V> node, List<V> out) {
        for (int i = 0; i < node.keys.size(); i++) {
            if (!node.leaf) {
                collectAll(node.children.get(i), out);
            }
            out.addAll(node.values.get(i));
        }
        if (!node.leaf) {
            collectAll(node.children.get(node.children.size() - 1), out);
        }
    }

    private void collectRange(Node<V> node,
                              Comparable<Object> lower,
                              boolean includeLower,
                              Comparable<Object> upper,
                              boolean includeUpper,
                              List<V> out) {
        for (int i = 0; i < node.keys.size(); i++) {
            if (!node.leaf) {
                collectRange(node.children.get(i), lower, includeLower, upper, includeUpper, out);
            }

            Comparable<Object> currentKey = node.keys.get(i);
            boolean lowerOk = lower == null || currentKey.compareTo(lower) > 0
                    || (includeLower && currentKey.compareTo(lower) == 0);
            boolean upperOk = upper == null || currentKey.compareTo(upper) < 0
                    || (includeUpper && currentKey.compareTo(upper) == 0);

            if (lowerOk && upperOk) {
                out.addAll(node.values.get(i));
            }
        }

        if (!node.leaf) {
            collectRange(node.children.get(node.children.size() - 1), lower, includeLower, upper, includeUpper, out);
        }
    }

    private List<V> search(Node<V> node, Comparable<Object> key) {
        int idx = findIndex(node, key);

        if (idx < node.keys.size() && node.keys.get(idx).compareTo(key) == 0) {
            return new ArrayList<>(node.values.get(idx));
        }

        if (node.leaf) {
            return new ArrayList<>();
        }

        return search(node.children.get(idx), key);
    }

    private void insertNonFull(Node<V> node, Comparable<Object> key, V value) {
        int idx = findIndex(node, key);

        if (idx < node.keys.size() && node.keys.get(idx).compareTo(key) == 0) {
            node.values.get(idx).add(value);
            return;
        }

        if (node.leaf) {
            node.keys.add(idx, key);
            ArrayList<V> bucket = new ArrayList<>();
            bucket.add(value);
            node.values.add(idx, bucket);
            return;
        }

        Node<V> child = node.children.get(idx);
        if (child.isFull(minimumDegree)) {
            splitChild(node, idx);
            int cmp = key.compareTo(node.keys.get(idx));
            if (cmp == 0) {
                node.values.get(idx).add(value);
                return;
            }
            if (cmp > 0) {
                idx++;
            }
        }
        insertNonFull(node.children.get(idx), key, value);
    }

    private void splitChild(Node<V> parent, int childIndex) {
        Node<V> fullChild = parent.children.get(childIndex);
        Node<V> rightNode = new Node<>(fullChild.leaf);

        int medianIndex = minimumDegree - 1;

        Comparable<Object> medianKey = fullChild.keys.get(medianIndex);
        ArrayList<V> medianValues = fullChild.values.get(medianIndex);

        for (int i = medianIndex + 1; i < fullChild.keys.size(); i++) {
            rightNode.keys.add(fullChild.keys.get(i));
            rightNode.values.add(fullChild.values.get(i));
        }

        if (!fullChild.leaf) {
            for (int i = medianIndex + 1; i < fullChild.children.size(); i++) {
                rightNode.children.add(fullChild.children.get(i));
            }
        }

        trimNodeAfterSplit(fullChild, medianIndex);

        parent.keys.add(childIndex, medianKey);
        parent.values.add(childIndex, medianValues);
        parent.children.add(childIndex + 1, rightNode);
    }

    private void trimNodeAfterSplit(Node<V> node, int medianIndex) {
        while (node.keys.size() > medianIndex) {
            node.keys.remove(node.keys.size() - 1);
            node.values.remove(node.values.size() - 1);
        }

        if (!node.leaf) {
            while (node.children.size() > medianIndex + 1) {
                node.children.remove(node.children.size() - 1);
            }
        }
    }

    private int findIndex(Node<V> node, Comparable<Object> key) {
        int idx = 0;
        while (idx < node.keys.size() && node.keys.get(idx).compareTo(key) < 0) {
            idx++;
        }
        return idx;
    }

    @SuppressWarnings("unchecked")
    private Comparable<Object> asComparable(Object key) {
        if (!(key instanceof Comparable<?>)) {
            throw new IllegalArgumentException("Index key must implement Comparable");
        }
        return (Comparable<Object>) key;
    }

    private static class Node<V> implements java.io.Serializable {
        private final boolean leaf;
        private final ArrayList<Comparable<Object>> keys;
        private final ArrayList<ArrayList<V>> values;
        private final ArrayList<Node<V>> children;

        private Node(boolean leaf) {
            this.leaf = leaf;
            this.keys = new ArrayList<>();
            this.values = new ArrayList<>();
            this.children = new ArrayList<>();
        }

        private boolean isFull(int t) {
            return keys.size() == (2 * t) - 1;
        }
    }
}
