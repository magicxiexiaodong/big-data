package com.xxd.util;

public class ETLUtil {
    public static String oriString2ETLString(String line) {

        StringBuilder sb = new StringBuilder();

        String[] splits = line.split("\t");


        if (splits.length < 10) {
            return null;
        }
        //

        // 处理分类分隔符&之间有空格的问题
        if (splits[3].contains(" ")) {
            splits[3] = splits[3].replaceAll(" ", "");
        }
        if (splits.length > 10) {
            for (int i = 0; i < splits.length; i++) {
                if (i < 9) {
                    sb.append(splits[i] + "\t");
                }
                if (i >= 9) {
                    sb.append(splits[i] + "&");
                }
                if (i == splits.length - 1) {
                    sb.append(splits[i]);
                }
            }
        } else {
            for (int i = 0; i < splits.length; i++) {
                if (i < splits.length - 1) {
                    sb.append(splits[i] + "\t");
                }
                if (i == splits.length - 1) {
                    sb.append(splits[i]);
                }
            }
        }


        return sb.toString();
    }

    public static void main(String[] args) {
        String ori = "SDNkMu8ZT68\tw00dy911\t630\tPeople & Blogs\t186\t10181\t3.49\t494\t257\trjnbgpPJUks";
        String etlString = oriString2ETLString(ori);
        System.out.println(etlString);
    }
}
