package com.lcy.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * Created by： luochengyue
 * date: 2019/4/30.
 * desc：hdfs 操作类
 * @version:
 */
public class HdfsOperate {
    private FileSystem fs;
    @Before
    public void beforeInit() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop1:9000");//可不配置默认操作本地的
        fs =  FileSystem.get(new URI("hdfs://hadoop1:9000"),conf,"hadoop");
    }
    //上传测试
    @Test
    public void testCopyFromLocalFile() throws IOException {
        fs.copyFromLocalFile(new Path("E:\\hadooptest\\files\\a.txt"),new Path("/"));
        fs.close();
    }
    //下载测试
    @Test
    public void testDownLoad() throws IOException {
        fs.copyToLocalFile(new Path("/a.txt"),new Path("d:/"));
        fs.close();
    }
}
