package com.ibeifeng.sparkproject;

/**
 * Created by Administrator on 2017/3/27.
 */
public class SingletonTest {
     private static  SingletonTest  instance=null;
     private  SingletonTest(){}
     public  static SingletonTest  getInstance(){
         if(instance==null){
             synchronized(SingletonTest.class){
                     if(instance==null){//代码块中加入synchronized关键字的同步，可以提高多线程并发的性能
                         instance=new SingletonTest();
                     }
             }
         }
              return  instance;
     }




}
