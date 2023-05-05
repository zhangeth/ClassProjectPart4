package CSCI485ClassProject.iterators;

import CSCI485ClassProject.RecordsTransformer;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.Record;

import CSCI485ClassProject.Cursor;
import CSCI485ClassProject.Iterator;
import CSCI485ClassProject.RecordsImpl;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.models.*;

import CSCI485ClassProject.utils.ComparisonUtils;
import CSCI485ClassProject.utils.IndexesUtils;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.*;

public class JoinIterator extends Iterator {
    private final Database db;
    private Transaction tx;
    private Transaction outerTx;

    private Iterator outerIterator;
    private Iterator innerIterator;
    private ComparisonPredicate predicate;
    private Set<String> attrNames;
    private RecordsImpl recordsImpl;
    private List<String> outerPath;
    private DirectorySubspace outerSubspace;
    private String outerTableName;
    private String innerTableName;
    private Record currRecord;

    private Cursor cursor;
    private boolean b = false;

    private int currentOuterIdx;
    private int outerSize;

    private void initOuterSubspace(String attrName)
    {
        // first, make unique subdir
        outerPath = new ArrayList<>(); outerPath.add(this.toString());
        String dirStr = "outer";
        outerPath.add(dirStr);
        outerSubspace = FDBHelper.createOrOpenSubspace(outerTx, outerPath);
        //FDBHelper.commitTransaction(createTx);
        System.out.println("successfully created outer subspace");
    }
    public JoinIterator()
    {
        db = FDBHelper.initialization();
        recordsImpl = new RecordsImpl();
        tx = FDBHelper.openTransaction(db);
    }
    public JoinIterator(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames, Database db)
    {
        this.db = db;
        this.outerIterator = outerIterator;
        this.innerIterator = innerIterator;
        this.predicate = predicate;
        this.attrNames = attrNames;

        recordsImpl = new RecordsImpl();

        tx = FDBHelper.openTransaction(db);
        outerTx = FDBHelper.openTransaction(db);
        if (innerIterator instanceof SelectIterator)
        {
            // cast to selectIterators for now
            SelectIterator si = (SelectIterator)innerIterator;
            innerTableName = si.getTableName();
            System.out.println("printing innername" + innerTableName);
        }
        if (outerIterator instanceof  SelectIterator)
        {
            SelectIterator si2 = (SelectIterator) outerIterator;
            outerTableName = si2.getTableName();
        }
        initOuterSubspace(predicate.getLeftHandSideAttrName());
        loopThroughOuter();
        currentOuterIdx = 0;


    }
    private void loopThroughOuter()
    {
        String attrName = predicate.getLeftHandSideAttrName();
        System.out.println("attr in question: " + attrName);
        outerSize = 0;
        Record r = outerIterator.next();
        System.out.println("looping for attr: " + predicate.getLeftHandSideAttrName());
        for (Map.Entry e :  r.getMapAttrNameToValue().entrySet())
        {
            System.out.println("key: " + e.getKey() + "val: " + e.getValue());
        }

        while (r != null)
        {
            System.out.println("loopin over: " + r.getValueForGivenAttrName("SSN") + " who's dno: " + r.getValueForGivenAttrName("DNO"));

            // commit value of the predicate to the thing
            Tuple keyTuple = new Tuple();
            keyTuple = keyTuple.addObject(r.getValueForGivenAttrName(attrName));
            // want to add pk as value, so can fetch when matched
            Tuple valueTuple = new Tuple();
            // could alternate thingies, and when there's a duplicate, then you know
            for (Map.Entry<String, Record.Value> entry : r.getMapAttrNameToValue().entrySet()) {
                valueTuple = valueTuple.add(entry.getKey());
                valueTuple = valueTuple.addObject(entry.getValue().getValue());
            }

            //valueTuple = valueTuple.addObject(r);
            FDBKVPair kvPair = new FDBKVPair(outerPath, keyTuple, valueTuple);
            FDBHelper.setFDBKVPair(outerSubspace, outerTx, kvPair);

            r = outerIterator.next();
            outerSize++;
        }
        List<FDBKVPair> pairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, outerTx, outerPath);
        System.out.println("outer size: " + outerSize);
        System.out.println("pairs size: " + pairs.size());

        // theory:
        if (!b)
        {
            cursor =  new Cursor(Cursor.Mode.READ, outerTableName, RecordsImpl.getTableMetadataByTableNameYuh(outerTx, outerTableName, db), outerTx);
        }
    }

    private Object applyAlgebraic(Object rightVal)
    {
        if (predicate.getRightHandSideAttrType() == AttributeType.INT)
        {
            long val1 = ComparisonUtils.convertObjectToLong(rightVal);
            if (predicate.getRightHandSideOperator() == AlgebraicOperator.PRODUCT)
            {
                long val2 = ComparisonUtils.convertObjectToLong(predicate.getRightHandSideValue());
                rightVal = (Object)(val1 * val2);
            }

        }
        else if (predicate.getRightHandSideAttrType() == AttributeType.DOUBLE)
        {
            double val1 = (double) rightVal;
            double val2 = (double) predicate.getRightHandSideValue();
            if (predicate.getRightHandSideOperator() == AlgebraicOperator.PRODUCT) {
                rightVal = (Object) (val1 * val2);
            }
        }
        return rightVal;
    }

    public Record next()
    {
        Record rightRecord;
        // now when calling next on inner Iterator, can loop through outer and compare, inner is dept currRecord refers to current inner record
        if (currRecord == null) {
            currRecord = innerIterator.next();
        }
        rightRecord = currRecord;

        while (rightRecord != null)
        {
            Object rightVal = rightRecord.getValueForGivenAttrName(predicate.getRightHandSideAttrName());
            // check type of right Record for applying algebraic, and apply it
            rightVal = applyAlgebraic(rightVal);

            Record reco;
            // loop through all of outer subdir
            if (!b) {
                reco = recordsImpl.getFirst(cursor);
                b = true;
            }
            else
                reco = recordsImpl.getNext(cursor);

                while (reco != null)
                {
                    Object leftVal = reco.getValueForGivenAttrName(predicate.getLeftHandSideAttrName());
                    if (ComparisonUtils.compareTwoObjects(leftVal, rightVal, predicate))
                    {
                        Record res = new Record();
                        // add the value alternating thingies
                        for (Map.Entry<String, Record.Value> entry : reco.getMapAttrNameToValue().entrySet())
                        {
                            boolean found = false;
                            if (attrNames != null)
                            {
                                for (String s :attrNames)
                                {
                                    if (entry.getKey().equals(s))
                                        found = true;
                                }
                            }
                            if (!found)
                                res.setAttrNameAndValue(entry.getKey(), entry.getValue().getValue());
                        }

                        HashMap<String, Record.Value> currMap = res.getMapAttrNameToValue();
                        // now add right Record
                        for (Map.Entry<String, Record.Value> entry : rightRecord.getMapAttrNameToValue().entrySet()) {
                            String key = entry.getKey();
                            boolean found = false;
                            if (attrNames != null)
                            {
                                for (String s :attrNames)
                                {
                                    if (entry.getKey().equals(s))
                                        found = true;
                                }
                            }

                            if (!found)
                            {
                                if (currMap.containsKey(key))
                                {
                                    res.updateJoinedRecord(key, outerTableName);
                                    String str = innerTableName + "." + key;
                                    res.setAttrNameAndValue(str, entry.getValue().getValue());
                                }
                                else {
                                    res.setAttrNameAndValue(key, entry.getValue().getValue());
                                }
                            }

                        }
                        currentOuterIdx++;
                        System.out.println("Matched employee: " + res.getValueForGivenAttrName("SSN") + " with: " + res.getValueForGivenAttrName("Employee.DNO"));
                        return res;
                    }
                    reco = recordsImpl.getNext(cursor);
                }

            // if reached end, reset outerIdx, and change rightRecord;
            cursor =  new Cursor(Cursor.Mode.READ, outerTableName, RecordsImpl.getTableMetadataByTableNameYuh(outerTx, outerTableName, db), outerTx); b = false;
            //currRecord = recordsImpl.getFirst(cursor);
            rightRecord = innerIterator.next();
            currRecord = rightRecord;
        }

        return null;
    }
    public void commit()
    {
        FDBHelper.commitTransaction(tx);
    }

    public void abort()
    {
        FDBHelper.abortTransaction(tx);
    }
}
