package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;

import javax.naming.ldap.UnsolicitedNotification;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        long log_lsn =  this.logManager.appendToLog(new CommitTransactionLogRecord(transNum, this.transactionTable.get(transNum).lastLSN));
        this.logManager.flushToLSN(log_lsn);
        this.transactionTable.get(transNum).lastLSN = log_lsn;
        this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
        return log_lsn;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        long log_lsn = this.logManager.appendToLog(new AbortTransactionLogRecord(transNum, this.transactionTable.get(transNum).lastLSN));
        this.transactionTable.get(transNum).lastLSN = log_lsn;
        this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.ABORTING);
        return log_lsn;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        Transaction transaction = this.transactionTable.get(transNum).transaction;
        if(transaction.getStatus() == Transaction.Status.ABORTING){
            long prevLSN = this.transactionTable.get(transNum).lastLSN;
            LogRecord logRecord;
            Pair<LogRecord, Boolean> pair;
            while(prevLSN > 0){
                logRecord = this.logManager.fetchLogRecord(prevLSN);
                if(logRecord.isUndoable()){
                    pair = logRecord.undo(this.transactionTable.get(transNum).lastLSN);
                    LogRecord CLR = pair.getFirst();
                    long logLSN = this.logManager.appendToLog(CLR);
                    this.transactionTable.get(transNum).lastLSN = logLSN;
                    if(pair.getSecond()){
                        this.logManager.flushToLSN(logLSN);
                    }
                    CLR.redo(diskSpaceManager, bufferManager);
                    updateDPT(CLR);
                    prevLSN = CLR.getUndoNextLSN().get();
                }
                else{
                    if(logRecord.getUndoNextLSN().isPresent()){
                        prevLSN = logRecord.getUndoNextLSN().get();
                    }
                    else {
                        prevLSN = logRecord.getPrevLSN().get();
                    }
                }
            }
            transaction.setStatus(Transaction.Status.COMPLETE);
            long log_lsn = this.logManager.appendToLog(new EndTransactionLogRecord(transNum, this.transactionTable.get(transNum).lastLSN));
            this.transactionTable.remove(transNum);
            return log_lsn;
        }
        else{
            transaction.setStatus(Transaction.Status.COMPLETE);
            long log_lsn = this.logManager.appendToLog(new EndTransactionLogRecord(transNum, this.transactionTable.get(transNum).lastLSN));
            this.transactionTable.remove(transNum);
            return log_lsn;
        }
    }

    public void updateDPT(LogRecord CLR){
        if(CLR.type == LogType.UNDO_UPDATE_PAGE){
            if(!this.dirtyPageTable.containsKey(CLR.getPageNum().get())){
                this.dirtyPageTable.put(CLR.getPageNum().get(), CLR.LSN);
            }
        }
        if(CLR.type == LogType.UNDO_ALLOC_PAGE){
            if(this.dirtyPageTable.containsKey(CLR.getPageNum().get())){
                this.dirtyPageTable.remove(CLR.getPageNum().get());
            }
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);

        // TODO(proj5): implement
        if(before.length > BufferManager.EFFECTIVE_PAGE_SIZE/2){
            long log_lsn = this.logManager.appendToLog(new UpdatePageLogRecord(transNum, pageNum, this.transactionTable.get(transNum).lastLSN, pageOffset, before, null));
            log_lsn = this.logManager.appendToLog(new UpdatePageLogRecord(transNum, pageNum, log_lsn, pageOffset, null, after));
            this.transactionTable.get(transNum).lastLSN = log_lsn;
            this.transactionTable.get(transNum).touchedPages.add(pageNum);
            if(!this.dirtyPageTable.containsKey(pageNum)){
                this.dirtyPageTable.put(pageNum, log_lsn);
            }
            return log_lsn;
        }
        else{
            long log_lsn = this.logManager.appendToLog(new UpdatePageLogRecord(transNum, pageNum, this.transactionTable.get(transNum).lastLSN, pageOffset, before, after));
            this.transactionTable.get(transNum).lastLSN = log_lsn;
            this.transactionTable.get(transNum).touchedPages.add(pageNum);
            if(!this.dirtyPageTable.containsKey(pageNum)){
                this.dirtyPageTable.put(pageNum, log_lsn);
            }
            return log_lsn;
        }
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        //LogRecord logRecord = this.logManager.fetchLogRecord(this.transactionTable.get(transNum).lastLSN);
        long prevLSN = this.transactionTable.get(transNum).lastLSN;
        LogRecord logRecord;
        Pair<LogRecord, Boolean> pair;
        while(prevLSN > LSN){
            logRecord = this.logManager.fetchLogRecord(prevLSN);
            if(logRecord.isUndoable()){
                pair = logRecord.undo(this.transactionTable.get(transNum).lastLSN);
                LogRecord CLR = pair.getFirst();
                long logLSN = this.logManager.appendToLog(CLR);
                this.transactionTable.get(transNum).lastLSN = logLSN;
                if(pair.getSecond()){
                    this.logManager.flushToLSN(logLSN);
                }
                CLR.redo(diskSpaceManager, bufferManager);
                updateDPT(CLR);
                prevLSN = CLR.getUndoNextLSN().get();
            }
            else{
                if(logRecord.getUndoNextLSN().isPresent()){
                    prevLSN = logRecord.getUndoNextLSN().get();
                }
                else {
                    prevLSN = logRecord.getPrevLSN().get();
                }
            }
        }

    }



    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        for(Map.Entry<Long, Long> entry: dirtyPageTable.entrySet()){
            long pageNum = entry.getKey();
            boolean flag = EndCheckpointLogRecord.fitsInOneRecord(dpt.size() + 1, txnTable.size(), touchedPages.size(), numTouchedPages);
            if(!flag){
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);
                dpt.clear();
            }
            dpt.put(pageNum, entry.getValue());
        }

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            boolean flag = EndCheckpointLogRecord.fitsInOneRecord(dpt.size(), txnTable.size()+1, touchedPages.size(), numTouchedPages);
            if(!flag){
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);
                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }
            txnTable.put(transNum, new Pair<>(entry.getValue().transaction.getStatus(), entry.getValue().lastLSN));
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    // TODO(proj5): add any helper methods needed

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        // TODO(proj5): implement
        restartAnalysis();
        restartRedo();
        List<Long> dirtyPages = new ArrayList<>();
        bufferManager.iterPageNums((curPageNum, dirty) -> {
            if (dirty){
                dirtyPages.add(curPageNum);
            }
        });
        for(Map.Entry<Long, Long> entry: dirtyPageTable.entrySet()){
            if(!dirtyPages.contains(entry.getKey())){
                dirtyPageTable.remove(entry.getKey());
            }
        }
        return () -> {
            restartUndo();
            checkpoint();
        };
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;

        // TODO(proj5): implement
        Iterator<LogRecord> iter = logManager.scanFrom(LSN);
        while(iter.hasNext()){
            LogRecord logRecord = iter.next();

            if(logRecord.type == LogType.COMMIT_TRANSACTION){
                if(logRecord.getTransNum().isPresent()){
                    Long tranNum = logRecord.getTransNum().get();
                    if(transactionTable.containsKey(tranNum)){
                        transactionTable.get(tranNum).transaction.setStatus(Transaction.Status.COMMITTING);
                        transactionTable.get(tranNum).lastLSN = logRecord.LSN;
                    }
                    else{
                        transactionTable.put(tranNum, new TransactionTableEntry(newTransaction.apply(tranNum)));
                        transactionTable.get(tranNum).transaction.setStatus(Transaction.Status.COMMITTING);
                        transactionTable.get(tranNum).lastLSN = logRecord.LSN;
                    }

                }
            }
            else if(logRecord.type == LogType.ABORT_TRANSACTION){
                if(logRecord.getTransNum().isPresent()){
                    Long tranNum = logRecord.getTransNum().get();
                    if(transactionTable.containsKey(tranNum)){
                        transactionTable.get(tranNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                        transactionTable.get(tranNum).lastLSN = logRecord.LSN;
                    }
                    else{
                        transactionTable.put(tranNum, new TransactionTableEntry(newTransaction.apply(tranNum)));
                        transactionTable.get(tranNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                        transactionTable.get(tranNum).lastLSN = logRecord.LSN;
                    }

                }
            }
            else if(logRecord.type == LogType.END_TRANSACTION){
                if(logRecord.getTransNum().isPresent()){
                    Long tranNum = logRecord.getTransNum().get();
                    if(transactionTable.containsKey(tranNum)){
                        transactionTable.get(tranNum).transaction.cleanup();
                        transactionTable.get(tranNum).transaction.setStatus(Transaction.Status.COMPLETE);
                        transactionTable.remove(tranNum);
                    }
                    else{
                        Transaction t = newTransaction.apply(tranNum);
                        t.cleanup();
                        t.setStatus(Transaction.Status.COMPLETE);
                    }
                }
            }
            else if(logRecord.type == LogType.UPDATE_PAGE || logRecord.type == LogType.UNDO_UPDATE_PAGE){
                if(logRecord.getTransNum().isPresent()){
                    Long tranNum = logRecord.getTransNum().get();
                    if(!transactionTable.containsKey(tranNum)){
                        transactionTable.put(tranNum, new TransactionTableEntry(newTransaction.apply(tranNum)));
                    }
                    transactionTable.get(tranNum).lastLSN = logRecord.LSN;
                    if(logRecord.getPageNum().isPresent()){
                        Long pageNum = logRecord.getPageNum().get();
                        transactionTable.get(tranNum).touchedPages.add(pageNum);
                        acquireTransactionLock(transactionTable.get(tranNum).transaction, getPageLockContext(pageNum), LockType.X);
                        if(!dirtyPageTable.containsKey(pageNum)){
                            dirtyPageTable.put(pageNum, logRecord.LSN);
                        }
                    }
                }

            }
            else if(logRecord.type == LogType.BEGIN_CHECKPOINT){
                if(logRecord.getMaxTransactionNum().isPresent()){
                    updateTransactionCounter.accept(Math.max(getTransactionCounter.get(), logRecord.getMaxTransactionNum().get()));
                }
            }
            else if(logRecord.type == LogType.END_CHECKPOINT){
                if(!logRecord.getDirtyPageTable().isEmpty()){
                    for(Map.Entry<Long, Long> entry: logRecord.getDirtyPageTable().entrySet()){
                        dirtyPageTable.put(entry.getKey(), entry.getValue());
                    }
                }
                if(!logRecord.getTransactionTable().isEmpty()){
                    for(Map.Entry<Long, Pair<Transaction.Status, Long>> entry : logRecord.getTransactionTable().entrySet()){
                        if(transactionTable.containsKey(entry.getKey())){
                            if(entry.getValue().getSecond() > transactionTable.get(entry.getKey()).lastLSN){
                                transactionTable.get(entry.getKey()).lastLSN = entry.getValue().getSecond();
                                transactionTable.get(entry.getKey()).transaction.setStatus(entry.getValue().getFirst());
                            }
                            if(transactionTable.get(entry.getKey()).transaction.getStatus() == Transaction.Status.RUNNING){
                                transactionTable.get(entry.getKey()).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                            }
                        }
                        else{
                            Transaction transaction = newTransaction.apply(entry.getKey());
                            transactionTable.put(entry.getKey(), new TransactionTableEntry(transaction));
                            startTransaction(transaction);
                            if(transaction.getStatus() == Transaction.Status.COMPLETE){
                                transactionTable.remove(entry.getKey());
                            }
                        }
                    }
                }
                if(!logRecord.getTransactionTouchedPages().isEmpty()){
                    for(Map.Entry<Long, List<Long>> entry : logRecord.getTransactionTouchedPages().entrySet()){
                        if(transactionTable.get(entry.getKey()).transaction.getStatus() != Transaction.Status.COMPLETE){
                            transactionTable.get(entry.getKey()).touchedPages.addAll(entry.getValue());
                            for(Long pageNum: entry.getValue()){
                                acquireTransactionLock(transactionTable.get(entry.getKey()).transaction, getPageLockContext(pageNum), LockType.X);
                            }
                        }
                    }
                }

            }
            else{ // AllocPage/FreePage/UndoAllocPage/UndoFreePage
                if(logRecord.getTransNum().isPresent()){
                    Long tranNum = logRecord.getTransNum().get();
                    if(!transactionTable.containsKey(tranNum)){
                        transactionTable.put(tranNum, new TransactionTableEntry(newTransaction.apply(tranNum)));
                    }
                    transactionTable.get(tranNum).lastLSN = logRecord.LSN;
                    if(logRecord.getPageNum().isPresent()){
                        Long pageNum = logRecord.getPageNum().get();
                        transactionTable.get(tranNum).touchedPages.add(pageNum);
                        acquireTransactionLock(transactionTable.get(tranNum).transaction, getPageLockContext(pageNum), LockType.X);
                        if(logRecord.type == LogType.FREE_PAGE){
                            dirtyPageTable.remove(pageNum);
                        }
                    }

                }

            }
        }
        for(Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()){
            if(entry.getValue().transaction.getStatus() == Transaction.Status.COMMITTING){
                entry.getValue().transaction.cleanup();
                entry.getValue().transaction.setStatus(Transaction.Status.COMPLETE);
                logManager.appendToLog(new EndTransactionLogRecord(entry.getKey(), entry.getValue().lastLSN));
                transactionTable.remove(entry.getKey());
            }
            if(entry.getValue().transaction.getStatus() == Transaction.Status.RUNNING){
                entry.getValue().transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                long abortLSN = logManager.appendToLog(new AbortTransactionLogRecord(entry.getKey(), entry.getValue().lastLSN));
                entry.getValue().lastLSN = abortLSN;
            }
        }


    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        // TODO(proj5): implement
        Long startLSN = Long.MAX_VALUE;
        for(Long LSN: dirtyPageTable.values()){
            if(startLSN > LSN){
                startLSN = LSN;
            }
        }
        Iterator<LogRecord> iter = logManager.scanFrom(startLSN);
        while(iter.hasNext()){
            LogRecord logRecord = iter.next();
            if(logRecord.isRedoable()){
                if(logRecord.getPageNum().isPresent()){
                    if(dirtyPageTable.containsKey(logRecord.getPageNum().get()) && logRecord.LSN >= dirtyPageTable.get(logRecord.getPageNum().get())){
                        Page page = bufferManager.fetchPage(getPageLockContext(logRecord.getPageNum().get()), logRecord.getPageNum().get(), false);
                        try{
                            if(page.getPageLSN() < logRecord.LSN){
                                logRecord.redo(diskSpaceManager, bufferManager);
                            }
                        }finally {
                            page.unpin();
                        }
                    }
                }
                else if(logRecord.getPartNum().isPresent()){
                    logRecord.redo(diskSpaceManager, bufferManager);
                }
            }
        }
    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        Queue<Pair<Long, Long>> queue = new PriorityQueue<>(new PairFirstReverseComparator());
        for(Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()){
            if(entry.getValue().transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING){
                queue.add(new Pair(entry.getValue().lastLSN, entry.getKey()));
            }
        }
        while(!queue.isEmpty()){
            Pair<Long, Long> pair = queue.poll();
            Long logLSN = pair.getFirst();
            LogRecord logRecord = logManager.fetchLogRecord(logLSN);

            if(logRecord.isUndoable()){
                Pair<LogRecord, Boolean> temp = logRecord.undo(transactionTable.get(pair.getSecond()).lastLSN);
                LogRecord CLR = temp.getFirst();
                long LSN = logManager.appendToLog(CLR);
                transactionTable.get(pair.getSecond()).lastLSN = LSN;
                if(temp.getSecond()){
                    logManager.flushToLSN(LSN);
                }
                CLR.redo(diskSpaceManager, bufferManager);
                updateDPT(CLR);
            }
            long newLSN;
            if (logRecord.getUndoNextLSN().isPresent()) {
                newLSN = logRecord.getUndoNextLSN().get();
            }
            else {
                newLSN = logRecord.getPrevLSN().get();
            }

            if(newLSN == 0){
                Transaction transaction = transactionTable.get(pair.getSecond()).transaction;
                transaction.cleanup();
                transaction.setStatus(Transaction.Status.COMPLETE);
                logManager.appendToLog(new EndTransactionLogRecord(transaction.getTransNum(), transactionTable.get(pair.getSecond()).lastLSN));
                transactionTable.remove(pair.getSecond());
            }
            else{
                queue.add(new Pair<>(newLSN, pair.getSecond()));
            }

        }

    }

    // TODO(proj5): add any helper methods needed

    // Helpers ///////////////////////////////////////////////////////////////////////////////

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                                 lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
        Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
