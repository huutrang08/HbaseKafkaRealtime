package com.example.hbasekafka.service;

import com.example.hbasekafka.connection.HBaseConnection;
import org.apache.hadoop.hbase.client.Result;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
public class HbaseService {
    HBaseConnection hBaseConnectionML = HBaseConnection.getInstance("src/main/resources/hbase-site.xml");
    public void pushCandidate(String table, String row, String family, Map<String, String> data){
        hBaseConnectionML.putCols(table,row,family,data,24*60*60*1000);
    }

    public List<String> showTable() throws IOException {
        return hBaseConnectionML.showAllTable();
    }

    public List<String> showTableCreate(String name) throws IOException {
        return hBaseConnectionML.showAllTableCreate(name);
    }

    public void createTable(String name,List<String> cf){
        hBaseConnectionML.createTable(name,cf);
    }

    public void writeData(String tableName, String rowKeys, String family, String col, String value){
        hBaseConnectionML.putCol(tableName,rowKeys,family,col,value);
    }

    public String getRow(String tableName,String key){
        return hBaseConnectionML.getRowData(tableName,key);
    }


}
