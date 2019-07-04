package com.lcy.hadoop.mr.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import lombok.Data;
import org.apache.hadoop.io.Writable;

/**
 * Created by luo on 2019/6/2.
 */
@Data
public class JoinBean implements Writable {
    //学员id 学员名称 年龄 所属班级id
    private int sId;
    private String sName;
    private int sAge;
    private int cId; //班级id
    private String cName;//班级名称
    private boolean isClassFlas;

    public JoinBean(int sId, String sName, int sAge, int cId,String cName,boolean isClassFlas) {
        this.sId = sId;
        this.sName = sName;
        this.sAge = sAge;
        this.cId = cId;
        this.isClassFlas = isClassFlas;
        this.cName = cName;

    }


    public JoinBean() {
    }


    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(sId);
        output.writeUTF(sName);
        output.writeInt(sAge);
        output.writeInt(cId);
        output.writeUTF(cName);
        output.writeBoolean(isClassFlas);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.sId = input.readInt();
        this.sName = input.readUTF();
        this.sAge = input.readInt();
        this.cId = input.readInt();
        this.cName = input.readUTF();
        this.isClassFlas = input.readBoolean();
    }

    @Override
    public String toString() {
        return "" + sId + '\t' +
                sName + '\t' +
                sAge + '\t' +
                cId + '\t' +
                cName;
    }
}
