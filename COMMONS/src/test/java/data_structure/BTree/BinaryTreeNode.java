package data_structure.BTree;

/**
 * 二叉树节点
 */
public class BinaryTreeNode {
    
    private int data;  //数据
    private BinaryTreeNode leftChirld;  //左孩子
    private BinaryTreeNode rightChirld; //右孩子
    
    public int getData() {
        return data;
    }
    public void setData(int data) {
        this.data = data;
    }
    public BinaryTreeNode getLeftChirld() {
        return leftChirld;
    }
    public void setLeftChirld(BinaryTreeNode leftChirld) {
        this.leftChirld = leftChirld;
    }
    public BinaryTreeNode getRightChirld() {
        return rightChirld;
    }
    public void setRightChirld(BinaryTreeNode rightChirld) {
        this.rightChirld = rightChirld;
    }        
}