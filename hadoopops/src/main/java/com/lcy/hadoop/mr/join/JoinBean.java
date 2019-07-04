package com.lcy.hadoop.mr.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by： luochengyue
 * date: 2019/5/22.
 * desc：joinbean主要包含了 student和对应的class相关属性
 */
public class JoinBean implements Writable {
    private Long sid;
    private String sName;
    private Integer sAge;
    private Long cid;
    private String cName;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(sid);
        out.writeUTF(sName);
        out.writeInt(sAge);
        out.writeLong(cid);
        out.writeUTF(cName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.sid=in.readLong();
        this.sName=in.readUTF();
        this.sAge=in.readInt();
        this.cid=in.readLong();
        this.cName=in.readUTF();
    }
}
