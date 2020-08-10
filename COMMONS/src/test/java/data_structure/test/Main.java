package data_structure.test;

import java.util.Scanner;

public class Main {
    public static void main(String[] args){
        Scanner scan = new Scanner(System.in);
        String str = scan.nextLine();
        char[] temp = str.toCharArray();
        for(int i = temp.length - 1; i >= 0; i--){
            System.out.print(temp[i]);
        }
        System.out.println();
    }
}