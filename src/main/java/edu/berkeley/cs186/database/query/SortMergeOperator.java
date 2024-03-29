package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

import javax.xml.crypto.Data;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private  DataBox leftJoinValue;
        private DataBox rightJoinValue;

        int i = 0;

        private SortMergeIterator() {
            super();
            // TODO(proj3_part1): implement
            //first sort two tables
            SortOperator sort_left = new SortOperator(getTransaction(), getLeftTableName(), new LeftRecordComparator());
            SortOperator sort_right = new SortOperator(getTransaction(), getRightTableName(), new RightRecordComparator());
            String sortedLeftTableName  = sort_left.sort();
            String sortedRightTableName = sort_right.sort();
            //get iterator from sorted table
            this.leftIterator = getTableIterator(sortedLeftTableName);
            this.rightIterator = getTableIterator(sortedRightTableName);
            this.leftRecord = this.leftIterator.next();
            this.rightRecord = this.rightIterator.next();
            this.marked = false;

            try{
                this.fetchNextRecord();
            }catch(NoSuchElementException e){
                this.nextRecord = null;
            }

        }

        private void fetchNextRecord(){
            if (this.leftRecord == null) { throw new NoSuchElementException("No new record to fetch"); }
            this.nextRecord = null;
            updateJoinValue();

            while(!hasNext()) {
                updateJoinValue();
                if (this.rightRecord == null) //there is no more matches, break while loop and nextRecord equals null
                    break;
                if (!marked) {
                    while (leftJoinValue.compareTo(rightJoinValue) < 0){
                        this.leftRecord = this.leftIterator.hasNext()? this.leftIterator.next(): null;
                        updateJoinValue(); //we have to call this each time leftRecord or rightRecord changes
                    }
                    while (leftJoinValue.compareTo(rightJoinValue) > 0){
                        this.rightRecord = this.rightIterator.hasNext()? this.rightIterator.next(): null;
                        updateJoinValue();
                    }
                    marked = true;
                    this.rightIterator.markPrev();
                }
                if (leftJoinValue.equals(rightJoinValue)) {
                    this.nextRecord = joinRecords(this.leftRecord, this.rightRecord);
                    this.rightRecord = this.rightIterator.hasNext()? this.rightIterator.next(): null;
                }
                else{
                    this.rightIterator.reset();
                    this.rightRecord = this.rightIterator.next();
                    this.leftRecord = this.leftIterator.hasNext()? this.leftIterator.next(): null;
                    marked = false;
                }
            }
        }

        private  void updateJoinValue(){
            //assign value to leftJoinValue and rightJoinvalue
            //Case1: this means our join opration complete
            if (this.leftRecord == null){
                throw new NoSuchElementException("No new record to fetch");
            }
            //Case2: rightIterator reach the end, but there is still left record to be matched, so we have to reset rightIterator
            if (this.rightRecord == null && this.leftIterator.hasNext()){
                this.rightIterator.reset();
                this.rightRecord = this.rightIterator.next();
                this.leftRecord = this.leftIterator.next();
                marked = false;
            }
            this.leftJoinValue = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
            if (this.rightRecord != null) //if rightRecord is still null, means that leftIterator don't have next
                this.rightJoinValue = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
        }

        private Record joinRecords(Record leftRecord, Record rightRecord) {
            //copy from BNLJ
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(proj3_part1): implement
            //same logic as BNLJ
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(proj3_part1): implement
            //same logic as BNLJ
            if (!this.hasNext()){
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try{
                this.fetchNextRecord();
            }catch(NoSuchElementException e){
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
