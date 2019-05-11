package query;

import database.Database;
import database.DatabaseException;
import table.Record;
import databox.DataBox;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.List;
public class INLJOperator extends JoinOperator {

    public INLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException {
        super(leftSource,
                rightSource,
                leftColumnName,
                rightColumnName,
                transaction,
                JoinType.INLJ);

    }

    @Override
    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
       	return new INLJIterator();
	// throw new UnsupportedOperationException("TODO: implement");
    }
    private class INLJIterator implements Iterator<Record> {
        private Iterator<Record> leftIterator;
        private Iterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;

        public INLJIterator() throws QueryPlanException, DatabaseException {
            this.leftIterator = INLJOperator.this.getLeftSource().iterator();
            this.rightIterator = null;
            this.leftRecord = null;
            this.nextRecord = null;
        }
     
    	public boolean hasNext() {
            if (this.nextRecord != null) {
                return true;
            }
            while (true) {
                if (this.leftRecord == null) {
                    if (this.leftIterator.hasNext()) {
                        this.leftRecord = this.leftIterator.next();
			DataBox leftJoinValue = this.leftRecord.getValues().get(INLJOperator.this.getLeftColumnIndex());
                        try {
                            this.rightIterator = INLJOperator.this.getRightSource().execute(BtreeIndexScanOperator.SearchType.EQUAL, leftJoinValue);
                        } catch (QueryPlanException q) {
                            return false;
                        } catch (DatabaseException e) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
		while (this.rightIterator.hasNext()) {
                    Record rightRecord = this.rightIterator.next();
                        List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
                        List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);
                        
			return true;
                }
                this.leftRecord = null;
            }
        }
        public Record next() {
            if (this.hasNext()) {
                Record r = this.nextRecord;
                this.nextRecord = null;
                return r;
            }
            throw new NoSuchElementException();
        }

        public void remove() { throw new UnsupportedOperationException(); }		
    }
}
