package sics.seiois.mlsserver.biz.der.mining.utils;

import sics.seiois.mlsserver.biz.der.metanome.denialconstraints.DenialConstraint;

import java.util.*;
import java.io.Serializable;

/**
 * A priority queue based on a binary heap.  Note that this implementation does
 * not efficiently support containment, removal, or element promotion
 * (decreaseKey) -- these methods are therefore not yet implemented.
 *
 * @author Dan Klein
 */
public class PriorityQueue_self_definded<E> implements Iterator<E>, Serializable, Cloneable {
    int size;
    int capacity;
    List<E> elements;
    double[] priorities;

    protected void grow(int newCapacity) {
        List<E> newElements = new ArrayList<E>(newCapacity);
        double[] newPriorities = new double[newCapacity];
        if (size > 0) {
            newElements.addAll(elements);
            System.arraycopy(priorities, 0, newPriorities, 0, priorities.length);
        }
        elements = newElements;
        priorities = newPriorities;
        capacity = newCapacity;
    }

    protected int parent(int loc) {
        return (loc - 1) / 2;
    }

    protected int leftChild(int loc) {
        return 2 * loc + 1;
    }

    protected int rightChild(int loc) {
        return 2 * loc + 2;
    }

    protected void heapifyUp(int loc) {
        if (loc == 0) return;
        int parent = parent(loc);
        if (priorities[loc] == priorities[parent]) {
            System.out.println("loc:"+loc+", parent:"+parent);
            return;
        }
        if (priorities[loc] > priorities[parent]) {
            swap(loc, parent);
            heapifyUp(parent);
        }
    }

    protected void heapifyDown(int loc) {
        int max = loc;
        int leftChild = leftChild(loc);
        if (leftChild < size()) {
            double priority = priorities[loc];
            double leftChildPriority = priorities[leftChild];
            if (leftChildPriority > priority)
                max = leftChild;
            int rightChild = rightChild(loc);
            if (rightChild < size()) {
                double rightChildPriority = priorities[rightChild(loc)];
                if (rightChildPriority > priority && rightChildPriority > leftChildPriority)
                    max = rightChild;
            }
        }
        if (max == loc)
            return;
        swap(loc, max);
        heapifyDown(max);
    }

    protected void swap(int loc1, int loc2) {
        double tempPriority = priorities[loc1];
        E tempElement = elements.get(loc1);
        priorities[loc1] = priorities[loc2];
        elements.set(loc1, elements.get(loc2));
        priorities[loc2] = tempPriority;
        elements.set(loc2, tempElement);
    }

    public void removeFirst() {
        if (size < 1) return;
        swap(0, size - 1);
        size--;
        elements.remove(size);
        heapifyDown(0);
    }

    /**
     * Returns true if the priority queue is non-empty
     */
    public boolean hasNext() {
        return ! isEmpty();
    }

    /**
     * Returns the element in the queue with highest priority, and pops it from
     * the queue.
     */
    public E next() {
        E first = peek();
        removeFirst();
        return first;
    }

    /**
     * Not supported -- next() already removes the head of the queue.
     */
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the highest-priority element in the queue, but does not pop it.
     */
    public E peek() {
        if (size() > 0)
            return elements.get(0);
        throw new NoSuchElementException();
    }

    /**
     * Gets the priority of the highest-priority element of the queue.
     */
    public double getPriority() {
        if (size() > 0)
            return priorities[0];
        throw new NoSuchElementException();
    }

    /**
     * Number of elements in the queue.
     */
    public int size() {
        return size;
    }

    /**
     * True if the queue is empty (size == 0).
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Adds a key to the queue with the given priority.  If the key is already in
     * the queue, it will be added an additional time, NOT promoted/demoted.
     *
     * @param key
     * @param priority
     */
    public boolean add(E key, double priority) {
        if (size == capacity) {
            grow(2 * capacity + 1);
        }
        System.out.println("key:"+key+", priority"+priority+", size:"+size);
        elements.add(key);
        priorities[size] = priority;
        heapifyUp(size);
        size++;
        return true;
    }

    /**
     * Returns a representation of the queue in decreasing priority order.
     */
    public String toString() {
        return toString(size());
    }

    /**
     * Returns a representation of the queue in decreasing priority order,
     * displaying at most maxKeysToPring elements.
     *
     * @param maxKeysToPrint
     */
    public String toString(int maxKeysToPrint) {
        PriorityQueue_self_definded<E> pq = clone();
        StringBuilder sb = new StringBuilder("[");
        int numKeysPrinted = 0;
        while (numKeysPrinted < maxKeysToPrint && pq.hasNext()) {
            double priority = pq.getPriority();
            E element = pq.next();
            sb.append(element.toString());
            sb.append(" : ");
            sb.append(priority);
            if (numKeysPrinted < size() - 1)
                sb.append(", ");
            numKeysPrinted++;
        }
        if (numKeysPrinted < size())
            sb.append("...");
        sb.append("]");
        return sb.toString();
    }

    /**
     * Returns a clone of this priority queue.  Modifications to one will not
     * affect modifications to the other.
     */
    public PriorityQueue_self_definded<E> clone() {
        PriorityQueue_self_definded<E> clonePQ = new PriorityQueue_self_definded<E>(new Comparator<DenialConstraint>() {
            @Override
            public int compare(DenialConstraint o1, DenialConstraint o2) {
                if (o1.getInterestingnessScore() < o2.getInterestingnessScore()) {
                    return 1;
                } else if (o1.getInterestingnessScore() > o2.getInterestingnessScore()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });
        clonePQ.size = size;
        clonePQ.capacity = capacity;
        clonePQ.elements = new ArrayList<E>(capacity);
        clonePQ.priorities = new double[capacity];
        if (size() > 0) {
            clonePQ.elements.addAll(elements);
            System.arraycopy(priorities, 0, clonePQ.priorities, 0, size());
        }
        return clonePQ;
    }

    public PriorityQueue_self_definded(Comparator<DenialConstraint> comparator) {
        this(15);
    }

    public PriorityQueue_self_definded() {
        this(15);
    }

    public PriorityQueue_self_definded(int capacity) {
        int legalCapacity = 0;
        while (legalCapacity < capacity) {
            legalCapacity = 2 * legalCapacity + 1;
        }
        grow(legalCapacity);
    }

    public void clear() {
        elements.clear();
        for (int i = 0; i < size; i++) {
//            elements.set(i, null);
            priorities[i] = 0.0;
        }
        size = 0;
        capacity = 0;
    }

    public static void main(String[] args) {
        PriorityQueue_self_definded<String> pq = new PriorityQueue_self_definded<String>();
//        pq.add("one",1);
//        pq.add("three",2);
//        pq.add("one",1.1);
//        pq.add("two1",2);
//        pq.add("two4",2);
//        pq.add("two2",2);
//        pq.add("two3",2);
//        System.out.println(pq);
////        while (pq.hasNext()) {
////            System.out.println(pq.next());
////        }
//        while (pq.hasNext()) {
//            System.out.println(pq.peek());
//            pq.removeFirst();
//        }
//
//        System.out.println("--------------------------");
//        pq.add("one15",1);
//        System.out.println(pq);
//        System.out.println("--------------------------");
//
//        pq.clear();
//        System.out.println(pq);

        pq.add("one1",1);
        pq.add("three2",2);
        pq.add("one2",1.1);
        pq.add("two3",2);
        pq.add("two12",2);
        pq.add("two42",2);
        pq.add("two23",2);
        System.out.println(pq);
        while (pq.hasNext()) {
            System.out.println(pq.peek());
            pq.removeFirst();
        }

        System.out.println("--------------");
        pq.add("one1",1);
        pq.add("three2",2);
        pq.add("one2",1.1);
        pq.add("two3",2);
        pq.add("two12",2);
        pq.add("two42",2);
        pq.add("two23",2);
        pq.add("two103",2);
        pq.add("two012",2);
        System.out.println(pq);
        while (pq.hasNext()) {
            System.out.println(pq.peek());
            pq.removeFirst();
        }
    }

}
