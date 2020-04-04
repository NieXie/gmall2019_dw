package com.atguigu.gmall2019.util;

/**
 * @author lijinmeng
 * @create 2020-04-02 23:08
 */
public class RanOpt<T>{
    T value ;
    int weight;

    public RanOpt ( T value, int weight ){
        this.value=value ;
        this.weight=weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}