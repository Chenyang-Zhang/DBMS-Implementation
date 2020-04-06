package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import java.util.*;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(proj4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        LockType curr_LockType = lockContext.getEffectiveLockType(transaction);
        if(transaction == null || curr_LockType == lockType || lockType == LockType.NL){
            //do nothing
            return;
        }
        if(lockType == LockType.S){
            Deque<LockContext> contextStack = getParentContext(lockContext, transaction, LockType.IS);
            while(!contextStack.isEmpty()){
                contextStack.removeFirst().acquire(transaction, LockType.IS);
            }
            if(curr_LockType == LockType.IS){
                lockContext.promote(transaction, lockType);
            }
            else{
                lockContext.acquire(transaction, LockType.S);
            }
        }
        else{
            Deque<LockContext> contextStack = getParentContext(lockContext, transaction, LockType.IX);
            while(!contextStack.isEmpty()){
                contextStack.removeFirst().acquire(transaction, LockType.IX);
            }
            if(curr_LockType == LockType.IX){
                lockContext.promote(transaction, lockType);
            }
            else{
                lockContext.acquire(transaction, LockType.X);
            }

        }
        return;
    }

    // TODO(proj4_part2): add helper methods as you see fit
    private void escalate(){

    }

    private static Deque<LockContext> getParentContext(LockContext lockContext, TransactionContext transaction, LockType lockType) {
        Deque<LockContext> contextStack = new LinkedList<>();
        LockContext parent = lockContext.parentContext();
        while (parent != null) {
            if (parent.getEffectiveLockType(transaction) == lockType) {
                break;
            }
            contextStack.addFirst(parent);
            parent = parent.parentContext();
        }
        return contextStack;
    }

}
