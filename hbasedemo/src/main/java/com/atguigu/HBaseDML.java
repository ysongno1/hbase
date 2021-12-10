package com.atguigu;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

public class HBaseDML {

    public static Connection connection = HbaseDDL.connection;

    /**
     * 插入数据
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
    public static void putCell(String nameSpace,String tableName, String rowKey, String family, String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
    }

    /**
     * 读取数据
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
    public static void  getCell(String nameSpace,String tableName, String rowKey, String family, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //如果想要获取完整一行的数据 直接创建的get对象放入到方法中即可
        //如果想要获取某一列数据 也可以重复添加几列
        get.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));
        //如果想要读多个版本
        get.readVersions(5);

        Result result = table.get(get);

        //处理读到的数据 get得到的数据就是cell数组
        Cell[] cells = result.rawCells();
//        for (Cell cell : cells) {
//            System.out.println(new String(CellUtil.cloneValue(cell)));
//        }

        //类似迭代器 advance就是hasnext ,current 就是 next方法
        CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            System.out.println(new String(CellUtil.cloneRow(cell)) + "-" + new String(CellUtil.cloneFamily(cell)) + "-" + new String(CellUtil.cloneQualifier(cell)) + "-" + new String(CellUtil.cloneValue(cell)));
        }

        //最简单的处理方法
///        System.out.println(new String(result.value()));

        //关闭table
        table.close();
    }

    /**
     * 扫描数据
     * @param nameSpace
     * @param tableName
     * @param startRowKey
     * @param stopRowKey
     * @throws IOException
     */
    public static void scanTable(String nameSpace,String tableName, String startRowKey, String stopRowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        //创建scan对象
        Scan scan = new Scan();
        //填写开始的行和结束的行
        scan.withStartRow(Bytes.toBytes(startRowKey));
        scan.withStopRow(Bytes.toBytes(stopRowKey));
        //调用方法scan
        ResultScanner resultScanner = table.getScanner(scan);
        //结果resultScanner是一个result的数组
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.print(new String(CellUtil.cloneRow(cell)) + "-" + new String(CellUtil.cloneFamily(cell)) + "-" + new String(CellUtil.cloneQualifier(cell)) + "-" + new String(CellUtil.cloneValue(cell))+"\t");
            }
            System.out.println();
        }

        //关闭table
        table.close();
    }

    /**
     * 删除数据
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
    public static void deleteColumn(String nameSpace,String tableName, String rowKey, String family, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //addColumn对应delete 删除一个版本
        //addColumns 对应deleteall 删除全部版本
        delete.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));
        //调用方法删除列
        table.delete(delete);
        //关闭table
        table.close();
    }

    public static void main(String[] args) throws IOException {

        //插入数据
//        putCell("bigdata","student","1001","info","name","zhangsan");

        //查询数据
//        getCell("bigdata","student","1001","info","name");

        //扫描数据
//        scanTable("bigdata","student","1001","1002");

        //删除数据
        deleteColumn("bigdata","student","1001","info","name");

        //关闭连接
        HbaseDDL.close();
    }
}
