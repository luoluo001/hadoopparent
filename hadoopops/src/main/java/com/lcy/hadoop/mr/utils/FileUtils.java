package com.lcy.hadoop.mr.utils;

import java.io.File;

/**
 * Created by luo on 2019/6/2.
 */
public class FileUtils {

    public static void deleteFile(String path){
        deleteFile(new File(path));
    }

    public static void deleteFile(File file){
        if(file.isDirectory()){
            String[] files = file.list();
            for(String f : files){
                File fi = new File(file.getParent(),f);
                deleteFile(fi);
            }
        }
        file.delete();
    }
}
