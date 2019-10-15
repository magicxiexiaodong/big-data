package com.xxd.hadoop.hdfsClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSClient {

    private FileSystem fs;

    @Before
    public void before() throws IOException, InterruptedException {
        fs = FileSystem.get(URI.create("hdfs://10.228.83.181:9000"),new Configuration(),"hadoop");
        System.out.println("Before!!!!!!!!!!");
    }

    @Test
    public void put () throws IOException {
        Configuration conf = new Configuration();
        fs.copyFromLocalFile(new Path("d:/1.txt"),new Path("/1.txt"));
    }

    @Test
    public void get() throws IOException {
        fs.copyToLocalFile(new Path("/test"), new Path("d:\\"));
    }


    @Test
    public void rename() throws IOException, InterruptedException {
        //操作
        fs.rename(new Path("/test"), new Path("/test2"));
    }

    @Test
    public void delete() throws IOException {
        boolean delete = fs.delete(new Path("/test2"), true);
        if (delete) {
            System.out.println("删除成功");
        } else {
            System.out.println("删除失败");
        }
    }


    @Test
    public void du() throws IOException {
        FSDataOutputStream append = fs.append(new Path("/test2/1.txt"), 1024);
        FileInputStream open = new FileInputStream("d:\\1.txt");
        IOUtils.copyBytes(open, append, 1024, true);
    }

    @Test
    public void ls() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("以下信息是一个文件的信息");
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getLen());
            } else {
                System.out.println("这是一个文件夹");
                System.out.println(fileStatus.getPath());
            }
        }
    }

    @After
    public void after() throws IOException {
        System.out.println("After!!!!!!!!!!");
        fs.close();
    }

}
