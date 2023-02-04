package com.example.hbasekafka.connection;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
public class HBaseConnection {
    private Connection connection;
    private Admin admin;
    private static final Map<Integer, HBaseConnection> mapHbaseConnection = new ConcurrentHashMap<>();


    public HBaseConnection(String source)  {
        try {
            Configuration config = HBaseConfiguration.create();
            config.addResource(new Path(source));
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
            System.out.println("Connected to Hbase ...");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("HBase is not running." + e.getMessage());
        }
    }



    public Admin getAdmin() {
        return admin;
    }

    public static HBaseConnection getInstance(String source)  {
        int connId = source.hashCode();
        HBaseConnection hBaseConnection;
        synchronized (mapHbaseConnection) {
            if (null == mapHbaseConnection.get(connId)) {
                hBaseConnection = new HBaseConnection(source);
                mapHbaseConnection.put(connId, hBaseConnection);
            } else {
                hBaseConnection = mapHbaseConnection.get(connId);
            }
        }
        return hBaseConnection;
    }

    public void createTable(String tableName, List<String> listFamily)  {
        System.out.println("Creating table: " + tableName);
        try {
            TableDescriptorBuilder table = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            //list column family
            List<ColumnFamilyDescriptor> listColumnFamily = new ArrayList<>();
            for(String cf : listFamily){
                ColumnFamilyDescriptor col1 = ColumnFamilyDescriptorBuilder.of(cf);
                listColumnFamily.add(col1);
            }
            // set table by list column family
            table.setColumnFamilies(listColumnFamily);
            Admin connectionAdmin = connection.getAdmin();
            // create table
            connectionAdmin.createTable(table.build());



            // check
            TableName tableNames = TableName.valueOf(tableName);
            Table tables = connection.getTable(tableNames);
            System.out.println("Table created: " + tables.getName().getNameAsString());
            tables.close();


            connection.close();
        }catch(IOException e){
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

    public void deleteRow(String tableName, String rowKeys) {
        System.out.println("Deleting row " + rowKeys + " of table " + tableName);
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            final byte[] rowToBeDeleted =  Bytes.toBytes(rowKeys);
            Delete delete = new Delete(rowToBeDeleted);
            table.delete(delete);
            System.out.println("Done: Deleting row " + rowKeys + " of table " + tableName);
        } catch (IOException ioException) {
            ioException.printStackTrace();
            System.out.println("Error: Deleting row " + rowKeys + " of table " + tableName);
        }
    }

    public void deleteTable(String name) throws IOException {
        System.out.println("Deleting table " + name);
        if (admin.tableExists(TableName.valueOf(name))) {
            admin.disableTable(TableName.valueOf(name));
            admin.deleteTable(TableName.valueOf(name));
        }
    }

    public void filters(String tableName, String prefix, String startRow, String endRow) throws IOException {
        System.out.println(tableName + "-" + prefix);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Filter prefixFilter = new PrefixFilter(Bytes.toBytes(prefix));
        List<Filter> filters = Collections.singletonList(prefixFilter);
        Scan scan = new Scan();
        scan.setFilter(new FilterList(Operator.MUST_PASS_ALL, filters));
        System.out.println("run scan data....");
        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                for (Cell kv: result.listCells()) {
                    System.out.println("found row " + Bytes.toString(CellUtil.cloneRow(kv)) +
                        ", column " + Bytes.toString(CellUtil.cloneQualifier(kv)) + ", value "
                        + Bytes.toString(CellUtil.cloneValue(kv)));
                }

            }
        }
        System.out.println("Done. ");

    }

    public Result getRow(String tableName, String rowKeys)  {
        Result result = null;
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get g = new Get(Bytes.toBytes(rowKeys));
            result = table.get(g);

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        return result;
    }

    public String getRowData(String tableName, String rowKeys)  {
        Result result = null;
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get g = new Get(Bytes.toBytes(rowKeys));
            result = table.get(g);


            byte[] value = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
            System.out.println("Value: " + Bytes.toString(value));
            return  Bytes.toString(value);

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        return "say hi";
    }


    public List<String> showAllTable() throws IOException {
        List<String> list = new ArrayList<>();
        Admin admin = connection.getAdmin();
        for (HTableDescriptor tableDescriptor : admin.listTables()) {
            System.out.println(tableDescriptor.getNameAsString());
            list.add(tableDescriptor.getNameAsString());
        }
        return list;
    }

    public List<String> showAllTableCreate(String name) throws IOException {
        List<String> list = new ArrayList<>();
        Admin admin = connection.getAdmin();
            for (HTableDescriptor tableDescriptor : admin.listTables()) {
                if (tableDescriptor.getNameAsString() == "name"){
                    list.add(tableDescriptor.getNameAsString());
                }
            }
        if(list.size() ==0){
            System.out.println("k co gi");
        }
            return list;
    }

    public Result getRow(String nameSpace, String tableName, String rowKeys)  {
        Result result = null;
        try {
            Table table = (nameSpace!= null) ? connection.getTable(TableName.valueOf(nameSpace,tableName)):  connection.getTable(TableName.valueOf(tableName));
            Get g = new Get(Bytes.toBytes(rowKeys));
            result = table.get(g);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        return result;
    }

    public void putCol(String tableName, String rowKeys, String family, String col, String value) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Put p = new Put(Bytes.toBytes(rowKeys));
            p.addColumn(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(value));
            table.put(p);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

    }

    public void putCols(String tableName, String rowKeys, String family, Map<String, String> cols, long ttl) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Put p = new Put(Bytes.toBytes(rowKeys));
            if(ttl>0) p.setTTL(ttl);
            cols.forEach((key, value) -> p.addColumn(family.getBytes(), Bytes.toBytes(key), Bytes.toBytes(value)));
            table.put(p);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

    }

    public ResultScanner scanTable(String nameSpace, String tableName, String family, String col)   {
        Table table;
        try {
            table =(nameSpace != null)? connection.getTable(TableName.valueOf(nameSpace,tableName)): connection.getTable(TableName.valueOf(tableName));
        if (null == table) {
            return null;
        }
        Scan scan = new Scan();
        if (null != family && null != col) {
            scan.addColumn(family.getBytes(), Bytes.toBytes(col));
        }
            return table.getScanner(scan);

        } catch (IOException ioException) {
            ioException.printStackTrace();
            return null;
        }
    }

    public ResultScanner scanAllRow(String nameSpace, String tableName){
        Table table;
        try {
            table =(nameSpace != null)? connection.getTable(TableName.valueOf(nameSpace,tableName)): connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            return table.getScanner(scan);

        } catch (IOException ioException) {
            ioException.printStackTrace();
            return null;
        }
    }

    public ResultScanner scanAllRow(String nameSpace, String tableName, String cf){
        Table table;
        try {
            table =(nameSpace != null)? connection.getTable(TableName.valueOf(nameSpace,tableName)): connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes(cf));
            return table.getScanner(scan);

        } catch (IOException ioException) {
            ioException.printStackTrace();
            return null;
        }
    }

    public ResultScanner scanAllRow(String nameSpace, String tableName, long start, long end){
        Table table;
        try {
            table =(nameSpace != null)? connection.getTable(TableName.valueOf(nameSpace,tableName)): connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setTimeRange(start,end);
            return table.getScanner(scan);

        } catch (IOException ioException) {
            ioException.printStackTrace();
            return null;
        }
    }

    public Set<String> filterUsersByLocation(String tableName, String family, String col, List<String> values) throws IOException {
        Set<String> users = new HashSet<>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Filter> filters = new ArrayList<>();
        for (String val: values) {
            SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(col), CompareOperator.EQUAL, Bytes.toBytes(val));
            valueFilter.setFilterIfMissing(true);
            filters.add(valueFilter);
        }
        Scan scan = new Scan();
        scan.setFilter(new FilterList(Operator.MUST_PASS_ONE, filters));
        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                users.add(Bytes.toString(result.getRow()));
                System.out.println(Bytes.toString(result.getRow()) + Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(col))));
            }
        }
        System.out.println("Done. ");
        return users;
    }

}
