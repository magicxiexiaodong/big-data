package com.xxd.hive;


import org.apache.hadoop.hive.ql.exec.UDF;

public class Lower extends UDF {

    public String evaluate(String s){
        if(s == null){
            return null;
        }
        return s.toLowerCase();
    }

}
