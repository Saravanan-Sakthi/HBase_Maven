package hbaseanalysis;

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

public class HBaseOperation {
    private static Scanner scan = new Scanner(System.in);
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        Connection conn= null;
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
                String tableName = scan.nextLine();
                System.out.print("Enter the number of data: ");
                int num = scan.nextInt();
                scan.nextLine();
                try {
                    addData(conn, tableName, num);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if(option == 1){
                System.out.print("Enter the table name: ");
                String tableName = scan.nextLine();
                System.out.print("Enter the number of Column families: ");
                int numCF = scan.nextInt();
                scan.nextLine();
                String [] cF= new String [numCF];
                System.out.println("Enter the Column Families");
                while (numCF-->0){
                    cF[numCF] = scan.nextLine();
                }
                try {
                    createTable(conn, tableName, cF);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void addData(Connection conn, String tableName, int num) throws IOException{
        Table table = conn.getTable(TableName.valueOf(tableName));
        for (int i=1 ; i<=num ; i++) {
            Put p = new Put(Bytes.toBytes("Tenkasi - "+i));
            String score;
            String status;
            String logical;
            String reasoning;
            String aptitude;
            String number;
            String time;
            String performance;
            String technical;
            String nonTechnical;
            if (i%7==0 || i% 13 == 0 ){
                score="32";
                status="not hired";
                logical="55";
                reasoning="23";
                aptitude="31";
                number="3";
                time="4";
                performance="60";
                technical="weak";
                nonTechnical="better";
            } else {
                score="89";
                status="hired";
                logical="98";
                reasoning="78";
                aptitude="69";
                number="6";
                time="2";
                performance="90";
                technical="strong";
                nonTechnical="strong";
            }
            p.addImmutable(Bytes.toBytes("result"), Bytes.toBytes("score"), Bytes.toBytes(score));
            p.addImmutable(Bytes.toBytes("result"), Bytes.toBytes("status"), Bytes.toBytes(status));
            p.addImmutable(Bytes.toBytes("round1"), Bytes.toBytes("logical"), Bytes.toBytes(logical));
            p.addImmutable(Bytes.toBytes("round1"), Bytes.toBytes("reasoning"), Bytes.toBytes(reasoning));
            p.addImmutable(Bytes.toBytes("round1"), Bytes.toBytes("aptitude"), Bytes.toBytes(aptitude));
            p.addImmutable(Bytes.toBytes("round2"), Bytes.toBytes("number"), Bytes.toBytes(number));
            p.addImmutable(Bytes.toBytes("round2"), Bytes.toBytes("time"), Bytes.toBytes(time));
            p.addImmutable(Bytes.toBytes("round2"), Bytes.toBytes("performance"), Bytes.toBytes(performance));
            p.addImmutable(Bytes.toBytes("round3"), Bytes.toBytes("technical"), Bytes.toBytes(technical));
            p.addImmutable(Bytes.toBytes("round3"), Bytes.toBytes("non-technical"), Bytes.toBytes(nonTechnical));
            table.put(p);
        }
        table.close();
    }

    private static void createTable(Connection conn, String tableName, String[] cF) throws IOException{
        Admin admin = conn.getAdmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(toBytes(tableName)));
        for (String columnFamily:cF) {
            HColumnDescriptor family = new HColumnDescriptor(toBytes(columnFamily));
            table.addFamily(family);
        }
        admin.createTable(table);
        admin.close();
    }
}
