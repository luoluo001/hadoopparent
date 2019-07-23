package com.dinfo.hbase;

import org.apache.curator.framework.state.ConnectionStateManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by： luochengyue
 * date: 2019/7/15.
 * desc：
 */
public class HBaseApiOps {
    Configuration conf = null;
    Table table = null;//用于做相关表操作的信息
    Connection conn =null;
    @Before
    public void before() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conn = ConnectionFactory.createConnection(conf);
        table = conn.getTable(TableName.valueOf("user"));
    }

    @Test
    public void testCreateTable() throws IOException {
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("user"); // 表名称
        HTableDescriptor desc = new HTableDescriptor(tableName);
        // 创建列族的描述类
        HColumnDescriptor family = new HColumnDescriptor("info"); // 列族
        // 将列族添加到表中
        desc.addFamily(family);
        HColumnDescriptor family2 = new HColumnDescriptor("info2"); // 列族
        // 将列族添加到表中
        desc.addFamily(family2);
        admin.createTable(desc);
    }

    @Test
    public void testDropTable() throws IOException {
        Admin admin = conn.getAdmin();
        TableName tName = TableName.valueOf("user");
        admin.disableTable(tName);
        admin.deleteTable(tName);
    }

    /**
     * 插入 前面定义我们用了hadmin 这里我们dml的时候直接用table就可了
     * @throws IOException
     */
    @Test
    public void testInsertOrUpdate() throws IOException {
        Put put = new Put(Bytes.toBytes("u_1234"));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        table.put(put);
    }

    @Test
    public void batchInsert() throws IOException {
        List<Put> puts = new ArrayList<>();
        for(int i =0;i<10;i++){
            Put put = new Put(Bytes.toBytes("u_123"+i));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan" + i));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(i+10));
            puts.add(put);
        }
        table.put(puts);
    }

    /**
     * 删除一行rowkey
     * @throws IOException
     */
    @Test
    public void testDel() throws IOException {
        //从上面可以知道 table的操作还是比较好记的 用table调用put传put对象 delte传delete对象 scan传scan对象我们试下del
        Delete delete = new Delete(Bytes.toBytes("u_1230"));
        table.delete(delete);
        table.flushCommits();
    }

    @Test
    public void testDelColumnFamily() throws IOException {
        //删除某个列
        Delete del = new Delete(Bytes.toBytes("u_1232"));
        del.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"));
        table.delete(del);
    }

    /**
     * 以下为相关查询的api
     */
    //单条查询
    @Test
    public void testGet() throws IOException {
        //这里对照命令行的get
        Get get = new Get(Bytes.toBytes("u_1233"));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for(Cell ce:cells){
            System.out.println("row:"+ new String(ce.getRowArray(),ce.getRowOffset(),ce.getRowLength()));
            System.out.println("family:"+ new String(ce.getFamilyArray(),ce.getFamilyOffset(),ce.getFamilyLength()));
            String qualifie = new String(ce.getQualifierArray(),ce.getQualifierOffset(),ce.getQualifierLength());
            System.out.println("Qualifier:"+qualifie );
            if(qualifie.equals("name")){
                System.out.println("value:"+ new String(ce.getValueArray(),ce.getValueOffset(),ce.getValueLength()));
            }else{
                System.out.println("value:"+ Bytes.toInt(ce.getValueArray()));
            }
            System.out.println("time:"+ce.getTimestamp());
        }

    }

    @Test
    public void testBatchQuery() throws IOException {
        Scan scan = new Scan();
        scan.setStartRow("u_1232".getBytes());
        scan.setStopRow("u_1235".getBytes());
        ResultScanner result = table.getScanner(scan);
        printScanResult(result);
    }

    private void printScanResult(ResultScanner result) {
        result.forEach(r -> Arrays.stream(r.rawCells()).forEach(
                ce -> {
                    System.out.println("row:"+ new String(ce.getRowArray(),ce.getRowOffset(),ce.getRowLength()));
                    System.out.println("family:"+ new String(ce.getFamilyArray(),ce.getFamilyOffset(),ce.getFamilyLength()));
                    String qualifie = new String(ce.getQualifierArray(),ce.getQualifierOffset(),ce.getQualifierLength());
                    System.out.println("Qualifier:"+qualifie );
                    if(qualifie.equals("name")){
                        System.out.println("value:"+ new String(ce.getValueArray(),ce.getValueOffset(),ce.getValueLength()));
                    }else{
                        System.out.println("value:"+ Bytes.toInt(ce.getValueArray()));
                    }
                    System.out.println("time:"+ce.getTimestamp());
                }
        ));
    }

    /**
     * 全面一种是一行扫描一种是scan扫描 接下来这个是过滤扫描
     */
    @Test
    public void testSingleFilterQuery() throws IOException {
        Scan scan = new Scan();
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);//过滤器列表 本身也是实现过滤器
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes("zhangsan4")));
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes("zhangsan3")));//单一列值过滤器
        scan.setFilter(filterList);
        ResultScanner result = table.getScanner(scan);
        printScanResult(result);
//注意：如果过滤器过滤的列在数据表中有的行中不存在，那么这个过滤器对此行无法过滤。
    }

    @Test
    public void testColumnPreffix() throws IOException {
        Scan scan = new Scan();
        scan.setFilter(new ColumnPrefixFilter("age".getBytes()));
        ResultScanner result = table.getScanner(scan);
        printScanResult(result);
    }
    //多个列值前缀
    @Test
    public void testMColumnPreffix() throws IOException {
        Scan scan = new Scan();
        byte[][] bys = new byte[][]{Bytes.toBytes("n"),Bytes.toBytes("a")};
        scan.setFilter(new MultipleColumnPrefixFilter(bys));
        ResultScanner result = table.getScanner(scan);
        printScanResult(result);
    }
    //对rowkey上做文章的过滤器
    @Test
    public void testRowFilter() throws IOException {
        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator("1239$")));
        ResultScanner result = table.getScanner(scan);
        printScanResult(result);
    }

    @After
    public void close() throws IOException {
        conn.close();
    }
}
