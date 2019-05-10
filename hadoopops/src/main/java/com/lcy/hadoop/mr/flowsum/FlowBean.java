package com.lcy.hadoop.mr.flowsum;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by： luochengyue
 * date: 2019/5/10.
 * desc：flow bean 流量统计对象，两个作用 1是map到reduce的数据对接 2是输出到reduce结果
 * @version:
 */
@Data
public class FlowBean implements Writable {

    private Long upFlum;
    private Long downFlum;
    private Long sumFlum;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlum);
        out.writeLong(downFlum);
        out.writeLong(sumFlum);
    }

    /**
     * 与write顺序一致
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlum = in.readLong();
        downFlum = in.readLong();
        sumFlum = in.readLong();
    }
    //给反射用
    public FlowBean(){}

    public FlowBean(long upFlum,long downFlum){
        this.upFlum = upFlum;
        this.downFlum = downFlum;
        this.sumFlum = upFlum+downFlum;
    }
    @Override
    public String toString() {
        return  upFlum +"\t" + downFlum +"\t" + sumFlum;
    }
}
