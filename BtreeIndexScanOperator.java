package query;

import database.Database;
import database.DatabaseException;
import databox.DataBox;
import index.BPlusTree;
import table.Record;
import table.RecordId;
import table.Schema;
import table.RecordIterator;
import table.Table;

import java.util.ArrayList;
import java.util.Iterator;

public class BtreeIndexScanOperator extends QueryOperator {
    private Database.Transaction transaction;
    private String tableName;
    private BPlusTree bPlusTree;
	
    public enum SearchType {
        EQUAL,
        GREATER,
        LESS,
        GREATER_OR_EQ,
        LESS_OR_EQ
    }

    public BtreeIndexScanOperator(Database.Transaction transaction,
                                  String tableName,
                                  BPlusTree bPlusTree) throws QueryPlanException, DatabaseException {
        super(OperatorType.INDEXSCAN);
        this.transaction = transaction;
     	this.tableName = tableName;
	this.bPlusTree = bPlusTree;
	this.setOutputSchema(this.computeSchema());
    }

    @Override
    public Iterator<Record> iterator() throws DatabaseException {
        return this.transaction.getRecordIterator(this.tableName);
    }

    @Override
    public Schema computeSchema() throws QueryPlanException {
        try {
		return this.transaction.getFullyQualifiedSchema(this.tableName);
	} catch (DatabaseException de) {
		throw new QueryPlanException(de);
	}
    }

    @Override
    public Iterator<Record> execute(Object... arguments) throws QueryPlanException, DatabaseException {
        SearchType search_type = (SearchType)arguments[0];
        DataBox val = (DataBox) arguments[1];

        if (search_type == SearchType.EQUAL){
            return find_equal(val);
        }

        return null;
    }

    public Iterator<Record> find_equal(DataBox val) throws DatabaseException{ 
        Iterator<RecordId> it = bPlusTree.scanEqual(val);
	Table t = transaction.getTable2(tableName);
	return new RecordIterator(t, it);
	//throw new UnsupportedOperationException("TODO: implement");
    }
}
