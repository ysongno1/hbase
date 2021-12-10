package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseDDL {

    //声明静态属性
    public static Connection connection =null;

    public HbaseDDL(){

    }

    static{

        //创建配置对象
        Configuration conf = new Configuration();

        //添加配置信息
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        //创建hbase连接
        //需要使用ConnectionFactory来实例化
        try {
             connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static  void close() throws IOException {
        connection.close();
    }


    /**
     * 创建命名空间，返回创建是否成功
     * @param nameSpace 命名空间名称
     * @return
     * @throws IOException
     */
    public static boolean createNameSpace(String nameSpace) throws IOException {

        //获取admin
        Admin admin = connection.getAdmin();
        //返回数据标志
        boolean b =  false;
        //创建Descriptor
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(nameSpace);
        //bulid相当于工人，告诉他需要添加什么配置
        //对应命令行方法 create_namespace 'ns1', {'PROPERTY_NAME'=>'PROPERTY_VALUE'}
        builder.addConfiguration("user","songsong");

        NamespaceDescriptor descriptor = builder.build();

        //为什么
        try {
            //创建命名空间
            admin.createNamespace(descriptor);

            //创建成功
            b = true;
        } catch (NamespaceExistException e){
            System.out.println("命名空间已存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
        //关闭资源
        admin.close();
        return b;
    }

    /**
     * 判断表是否存在，提供参数命名空间和表面
     * @param nameSpace
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String nameSpace, String tableName) throws IOException {

        Admin admin = connection.getAdmin();
        boolean b = admin.tableExists(TableName.valueOf(nameSpace,tableName));
        admin.close();
        return b;
    }

    /**
     * 创建表
     * @param nameSpace
     * @param table
     * @param familyNames
     * @return
     * @throws IOException
     */
    public static boolean createTable(String nameSpace, String table, String... familyNames) throws IOException {

        //手动添加异常判断  就不用走后面的流程浪费大量资源
        //列族至少有一个
        if (familyNames.length < 1){
            System.out.println("请至少输入一个列族");
            return false;
        }

        //如果表格已经存在 也不用走下面的一大片信息
        if (isTableExist(nameSpace,table)){
            System.out.println("表已存在");
            return false;
        }


        Admin admin = connection.getAdmin();
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace, table));
        //添加列族信息
        for (String familyName : familyNames) {
            //获取单个列族的descriptor
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName));
            //修改列族参数 create_namespace 'ns1', {'PROPERTY_NAME'=>'PROPERTY_VALUE'}
            columnFamilyDescriptorBuilder.setMaxVersions(5);

            //添加列族信息
            builder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }

        try {
            admin.createTable(builder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();
        return true;
    }

    /**
     * 修改表格
     * @param nameSpace
     * @param table
     * @param familyName
     * @param version
     * @throws IOException
     */
    public static void alterTable(String nameSpace, String table, String familyName, int version) throws IOException {
        //判断表格是否存在
        if (!isTableExist(nameSpace,table)){
            System.out.println("表格不存在 无法修改");
            return;
        }

        Admin admin = connection.getAdmin();

        // 工厂模式修改表格信息的时候 一定要调用原先的表格进行修改  否则会把所有信息全部恢复默认
        //使用admin拿到老的表格描述
        TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(nameSpace, table));
        //建造者模式创建的对象一般不能修改 只能读
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);
        //调用老的列族描述
        ColumnFamilyDescriptor columnFamily = descriptor.getColumnFamily(Bytes.toBytes(familyName));
        //创造对应的建造者 对版本号进行修改
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily);
        columnFamilyDescriptorBuilder.setMaxVersions(version);
        //使用原先的描述 给到建造者 之后用建造者修改
        tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());
        //使用admin修改表格
        try {
            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        //这是找了一个新工人 给你重新造了一个表  你得找见原来那个表
//        //创建表格描述
//        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace,table));
//
//        //创建列族描述 用于修改
//        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName));
//        columnFamilyDescriptorBuilder.setMaxVersions(version);
//        //修改列族的信息
//        tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());
//
//        //调用方法修改表
//        try {
//            admin.modifyTable(tableDescriptorBuilder.build());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        admin.close();
    }

    /**
     * 删除表格
     * @param nameSpace
     * @param table
     * @throws IOException
     */
    public static void dropTable(String nameSpace, String table) throws IOException {

        if (!isTableExist(nameSpace,table)){
            System.out.println("表格不存在，无法删除");
        }
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(nameSpace, table);
        try {
            //想要删除表格 需要先将表格标记为disable
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        admin.close();
    }

    public static void main(String[] args) throws IOException {
        //创建命名空间
//       createNameSpace("bigdata");

        //判断表是否存在
//        isTableExist("bigdata","student");

        //创建表格
       createTable("bigdata","student","info");

        //修改表格
//        alterTable("bigdata","student","info",9);

        //删除表格
//        dropTable("bigdata","student");

        //打印连接
        System.out.println(connection);
    }
}
