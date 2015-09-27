import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable{

   public static void main(String[] args) throws IOException {

      String tableName = "powers";

      // ****************************************
      // Create Table "powers"
      // ****************************************

      // Instantiate Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instaniate HBaseAdmin class
      HBaseAdmin admin = new HBaseAdmin(config);
      
      // Instantiate table descriptor class
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

      // Add column families to table descriptor
      tableDescriptor.addFamily(new HColumnDescriptor("personal"));
      tableDescriptor.addFamily(new HColumnDescriptor("professional"));

      // Execute the table through admin
      admin.createTable(tableDescriptor);
      //System.out.println(" Table created ");



      // ****************************************
      // Create Table "powers"
      // ****************************************

      // Instantiating HTable class
      HTable hTable = new HTable(config, tableName);


      // Repeat these steps as many times as necessary

         // Instantiating Put class
         // Hint: Accepts a row name
         Put row_superman = new Put(Bytes.toBytes("row1"));

         // Add values using add() method
         // Hints: Accepts column family name, qualifier/row name ,value
         row_superman.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("superman"));
         row_superman.add(Bytes.toBytes("personal"), Bytes.toBytes("power"),Bytes.toBytes("strength"));

         row_superman.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("clark"));
         row_superman.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("100"));

         // Save the table
         hTable.put(row_superman);
         //System.out.println("New Hero Inserted: Superman");


         Put row_batman = new Put(Bytes.toBytes("row2"));

         // Add values using add() method
         // Hints: Accepts column family name, qualifier/row name ,value
         row_batman.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("batman"));
         row_batman.add(Bytes.toBytes("personal"), Bytes.toBytes("power"),Bytes.toBytes("money"));

         row_batman.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("bruce"));
         row_batman.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("50"));

         // Save the table
         hTable.put(row_batman);
         //System.out.println("New Hero Inserted: Batman");


         Put row_wolverine = new Put(Bytes.toBytes("row3"));

         // Add values using add() method
         // Hints: Accepts column family name, qualifier/row name ,value
         row_wolverine.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("wolverine"));
         row_wolverine.add(Bytes.toBytes("personal"), Bytes.toBytes("power"),Bytes.toBytes("healing"));

         row_wolverine.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("logan"));
         row_wolverine.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("75"));

         // Save the table
         hTable.put(row_wolverine);
         //System.out.println("New Hero Inserted: Wolverine");

	
      // Close table
      // hTable.close();


      // ****************************************
      // Create Table "powers"
      // ****************************************

      // Instantiate the Scan class
      Scan scan = new Scan();
     
      // Scan the required columns
      scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));

      // Get the scan result
      ResultScanner scanner = hTable.getScanner(scan);

      // Read values from scan result
      for (Result result = scanner.next(); result != null; result = scanner.next()) {
         // Print scan result
         System.out.println(result);
      }

      // Close the scanner
      scanner.close();
   
      // Htable closer
      hTable.close();
   }
}

