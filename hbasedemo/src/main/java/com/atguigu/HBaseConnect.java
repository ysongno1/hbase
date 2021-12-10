package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


public class HBaseConnect {
    public static void main(String[] args) throws IOException {

        //创建配置对象
        Configuration conf = new Configuration();

        //添加配置信息
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        //创建hbase连接
        //需要使用ConnectionFactory来实例化
        Connection connection = ConnectionFactory.createConnection(conf);

        //使用连接
        System.out.println(connection);
        System.out.println(connection.getConfiguration());

        //关闭连接
        connection.close();

    }
}
