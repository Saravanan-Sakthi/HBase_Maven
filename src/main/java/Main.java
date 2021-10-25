import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Scanner;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class Main {
    private static Scanner scan = new Scanner(System.in);
    public static void main(String[] args){
        while (true){
            System.out.println("HBase");
            System.out.print("1. Create new table\n2. Add data to table\n3.Exit\nEnter the option:");
            int option = scan.nextInt();
            scan.nextLine();
            if (option == 3){
                break;
            }
            else if (option == 2){
                System.out.print("Enter table name: ");
                String table_name = scan.nextLine();
                System.out.print("Enter the column family: ");
                String columnFamily= scan.nextLine();
                System.out.print("Enter the number of data: ");
                int num = scan.nextInt();
                scan.nextLine();
                try {
                    addData(table_name, columnFamily, num);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if(option == 1){
                System.out.print("Enter the table name: ");
                String tableName = scan.nextLine();
                System.out.print("Enter the column family name: ");
                String columnFamily = scan.nextLine();
                try {
                    createTable(tableName, columnFamily);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void createTable(String tableName , String columnFamily) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
//        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(toBytes(tableName)));
        HColumnDescriptor family = new HColumnDescriptor(toBytes(columnFamily));
        table.addFamily(family);
        admin.createTable(table);

    }

    private static void addData(String tableName, String columnFamily, int num) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Table table = null;
        try {
            Connection connection = ConnectionFactory.createConnection(conf);

            table = connection.getTable(TableName.valueOf(tableName));
//            table = new HTable(conf , tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i=0 ; i<num ; i++) {
            Put p = new Put(Bytes.toBytes("e - "+i));
            p.addImmutable(Bytes.toBytes(columnFamily), Bytes.toBytes("salary"), Bytes.toBytes("1000"+i));
            table.put(p);
        }
        table.close();
    }
}
