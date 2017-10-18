package com.dnanexus.spark;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.shims.Utils;

public class HiveMetaStoreFilterHook implements MetaStoreFilterHook {

  public HiveMetaStoreFilterHook(HiveConf conf) {
    System.out.println("****** HiveMetaStore Filter DNAx **** ");
  }

  @Override
  public List<String> filterDatabases(List<String> dbList) throws MetaException {
    printUserName("filterDatabases - " + dbList.toString());
    return dbList;
  }

  @Override
  public Database filterDatabase(Database dataBase) throws NoSuchObjectException {
    printUserName("filterDatabase " + dataBase.toString());
    return dataBase;
  }

  @Override
  public List<String> filterTableNames(String dbName, List<String> tableList) throws MetaException {
    printUserName("filterTableNames - db " + dbName + " .. " + tableList.toString());
    return tableList;
  }

  @Override
  public Table filterTable(Table table)  throws NoSuchObjectException {
    printUserName("filterTable - " + table.toString());
    return table;
  }

  @Override
  public List<Table> filterTables(List<Table> tableList) throws MetaException {
    printUserName("filterTables - " + tableList.toString());
    return tableList;
  }

  @Override
  public List<Partition> filterPartitions(List<Partition> partitionList) throws MetaException {
    printUserName("filterPartitions");
    return partitionList;
  }

  @Override
  public List<PartitionSpec> filterPartitionSpecs(
      List<PartitionSpec> partitionSpecList) throws MetaException {
    printUserName("filterPartitionSpecs");
    return partitionSpecList;
  }

  @Override
  public Partition filterPartition(Partition partition)  throws NoSuchObjectException {
    printUserName("filterPartition");
    return partition;
  }

  @Override
  public List<String> filterPartitionNames(String dbName, String tblName,
      List<String> partitionNames) throws MetaException {
    printUserName("filterPartitionNames");
    return partitionNames;
  }

  @Override
  public Index filterIndex(Index index)  throws NoSuchObjectException {
    printUserName("filterIndex");
    return index;
  }

  @Override
  public List<String> filterIndexNames(String dbName, String tblName,
      List<String> indexList) throws MetaException {
    printUserName("filterIndexNames");
    return indexList;
  }

  @Override
  public List<Index> filterIndexes(List<Index> indexeList) throws MetaException {
    printUserName("filterIndexes");
    return indexeList;
  }

  private void printUserName(String msg) {
    SessionState ss = SessionState.get();
    String userName = null;

    try {
      userName = Utils.getUGI().getUserName();
      System.out.println("**** FilterHook " + userName + " ssUser " + ss.getUserName() + " AuthUser " + 
                         ss.getUserFromAuthenticator() + " **** " + msg);
    } catch (Exception x) {
      System.out.println("**** Exception in printUserName");
      x.printStackTrace();
    }
    if ("asdf".equals(userName)) throw new RuntimeException("Not Authorized - " + userName);
  }
}
