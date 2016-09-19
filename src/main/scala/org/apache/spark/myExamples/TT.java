package org.apache.spark.myExamples;

/**
 * Created by wjf on 16-9-14.
 */
public class TT {
    private static volatile A a =new A(1,2);
    static class A{
        private int a;
        private int b;
        public A(int a,int b){
            this.a = a;
            this.b = b;
        }
    }
    public static void main(String[] args){
        System.out.println(a);

    }
}
