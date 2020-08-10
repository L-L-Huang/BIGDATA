package data_structure.test;

import java.util.concurrent.ConcurrentHashMap;

public class Test {
    public int maxArra(int[] arr){
        int length = arr.length;
        if(length == 0){
            return 0;
        }
        int[] temp = new int[length];
        temp[0] = arr[0];
        for(int i=1;i<length;i++){
            temp[i] = Math.max(arr[i],temp[i-1] + arr[i]);
        }
        int result = temp[0];
        return 0;
        //
    }

    //方法一：
    public static final int hash(Object key) {   //jdk1.8 & jdk1.7
        int h;
        // h = key.hashCode() 为第一步 取hashCode值
        // h ^ (h >>> 16)  为第二步 高位参与运算
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }
    //方法二：
    public static int indexFor(int h, int length) {  //jdk1.7的源码，jdk1.8没有这个方法，但是实现原理一样的
        return h & (length-1);  //第三步 取模运算
    }

    public static void main(String[] args) {
        int hashCode = "hello".hashCode();
        System.out.println(hashCode);
        System.out.println(hashCode >>> 16);
        System.out.println(hashCode ^ (hashCode >>> 16));
        ConcurrentHashMap concurrentHashMap = new ConcurrentHashMap();
        concurrentHashMap.put("","");
    }
}
