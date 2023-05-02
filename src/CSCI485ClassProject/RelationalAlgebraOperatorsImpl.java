package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.iterators.SelectIterator;
import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;

import CSCI485ClassProject.models.TableMetadata;
import CSCI485ClassProject.utils.ComparisonUtils;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// your codes
public class RelationalAlgebraOperatorsImpl implements RelationalAlgebraOperators {

  private final Database db;
  // Impl has own transaction, each respective iterator has its own tx as well
  Transaction tx;

  public RelationalAlgebraOperatorsImpl()
  {
    db = FDBHelper.initialization();
    tx = FDBHelper.openTransaction(db);
  }


  @Override
  public Iterator select(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex) {
    // check table exists
    List<String> tablePath = new ArrayList<>(); tablePath.add(tableName);
    if (FDBHelper.doesSubdirectoryExists(tx, tablePath))
    {
      // check types of predicate
      if (ComparisonUtils.checkComparisonPredicateTypes(predicate))
        return new SelectIterator(tableName, predicate, mode, isUsingIndex, db);
    }

    return null;
  }

  @Override
  public Set<Record> simpleSelect(String tableName, ComparisonPredicate predicate, boolean isUsingIndex) {

    if (!ComparisonUtils.checkComparisonPredicateTypes(predicate))
      return null;

    Set<Record> res = new HashSet<>();
    // check table exists
    List<String> tablePath = new ArrayList<>(); tablePath.add(tableName);
    if (FDBHelper.doesSubdirectoryExists(tx, tablePath))
    {
      SelectIterator si = new SelectIterator(tableName, predicate, Iterator.Mode.READ, isUsingIndex, db);
      Record rec = si.next();
      while (rec != null)
      {
        res.add(rec);
        rec = si.next();
        System.out.println("called");
      }

    }
    return res;
  }

  @Override
  public Iterator project(String tableName, String attrName, boolean isDuplicateFree) {
    return null;
  }

  @Override
  public Iterator project(Iterator iterator, String attrName, boolean isDuplicateFree) {
    return null;
  }

  @Override
  public List<Record> simpleProject(String tableName, String attrName, boolean isDuplicateFree) {
    return null;
  }

  @Override
  public List<Record> simpleProject(Iterator iterator, String attrName, boolean isDuplicateFree) {
    return null;
  }

  @Override
  public Iterator join(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames) {
    return null;
  }

  @Override
  public StatusCode insert(String tableName, Record record, String[] primaryKeys) {
    return null;
  }

  @Override
  public StatusCode update(String tableName, AssignmentExpression assignExp, Iterator dataSourceIterator) {
    return null;
  }

  @Override
  public StatusCode delete(String tableName, Iterator iterator) {
    return null;
  }
}
