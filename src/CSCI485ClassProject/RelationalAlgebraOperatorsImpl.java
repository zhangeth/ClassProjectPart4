package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.iterators.JoinIterator;
import CSCI485ClassProject.iterators.ProjectIterator;
import CSCI485ClassProject.iterators.SelectIterator;
import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;

import CSCI485ClassProject.models.TableMetadata;
import CSCI485ClassProject.utils.ComparisonUtils;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import jdk.nashorn.internal.scripts.JO;

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
      }
    }

    return res;
  }

  @Override
  public Iterator project(String tableName, String attrName, boolean isDuplicateFree) {
      List<String> tablePath = new ArrayList<>(); tablePath.add(tableName);
      if (FDBHelper.doesSubdirectoryExists(tx, tablePath))
      {
        return new ProjectIterator(tableName, attrName, isDuplicateFree, db);
      }

      return null;
  }

  @Override
  public Iterator project(Iterator iterator, String attrName, boolean isDuplicateFree) {
    if (iterator != null)
      return new ProjectIterator(iterator, attrName, isDuplicateFree, db);
    return null;
  }

  @Override
  public List<Record> simpleProject(String tableName, String attrName, boolean isDuplicateFree) {
    List<Record> ans = new ArrayList<>();
    ProjectIterator pi = new ProjectIterator(tableName, attrName, isDuplicateFree, db);
    Record r = pi.next();

    while (r != null)
    {
      if (!isDuplicateFree)
        ans.add(r);
      r = pi.next();
    }
    if (isDuplicateFree)
    {
      // if dupes is enabled, use the subdirectory of the dupes to make the list, because this is auto sorted by fdb
      for (FDBKVPair fdbkvPair : FDBHelper.getAllKeyValuePairsOfSubdirectory(db, pi.getDupeTransaction(), pi.getDuplicateAttrPath()))
      {
        Record rec = new Record();
        rec.setAttrNameAndValue(attrName, fdbkvPair.getKey().get(0));
        ans.add(rec);
      }
    }

    return ans;
  }

  @Override
  public List<Record> simpleProject(Iterator iterator, String attrName, boolean isDuplicateFree) {
    List<Record> ans = new ArrayList<>();
    ProjectIterator pi = new ProjectIterator(iterator, attrName, isDuplicateFree, db);
    Record r = pi.next();

    while (r != null)
    {
      if (!isDuplicateFree)
        ans.add(r);
      r = pi.next();
    }
    if (isDuplicateFree)
    {
      // if dupes is enabled, use the subdirectory of the dupes to make the list, because this is auto sorted by fdb
      for (FDBKVPair fdbkvPair : FDBHelper.getAllKeyValuePairsOfSubdirectory(db, pi.getDupeTransaction(), pi.getDuplicateAttrPath()))
      {
        Record rec = new Record();
        rec.setAttrNameAndValue(attrName, fdbkvPair.getKey().get(0));
        ans.add(rec);
      }
    }

    return ans;
  }

  @Override
  public Iterator join(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames) {
    // for each record in outerIterator, iterate through until predicate is matched
    // check if predicate types ok
    if (!ComparisonUtils.checkComparisonPredicateTypes(predicate))
      return null;
    // idea: read one iterator into temporary table, use then you don't have to make new iterators
    return new JoinIterator(outerIterator, innerIterator, predicate, attrNames, db);
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
