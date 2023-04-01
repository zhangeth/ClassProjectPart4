package CSCI485ClassProject;

import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.TableMetadata;

import java.util.HashMap;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{

  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeTypes, String[] primaryKeyAttributeNames) {
    return null;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    return null;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    return null;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {
    return null;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    return null;
  }

  @Override
  public StatusCode dropAllTables() {
    return null;
  }
}
