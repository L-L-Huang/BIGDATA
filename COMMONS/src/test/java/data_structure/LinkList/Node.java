package data_structure.LinkList;

import java.util.LinkedList;

public class Node {
    private int data;  //数据
    private Node next;  //指针
    private LinkedList linkList;
    
    public int getData() {
        return data;
    }
    public void setData(int data) {
        this.data = data;
    }
    public Node getNext() {
        return next;
    }
    public void setNext(Node next) {
        this.next = next;
    }    
}