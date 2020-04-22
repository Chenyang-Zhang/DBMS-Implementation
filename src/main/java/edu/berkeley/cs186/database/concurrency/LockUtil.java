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
        if(transaction == null || curr_LockType == lockType|| LockType.substitutable(curr_LockType, lockType) || lockType == LockType.NL){
            //do nothing
            return;
        }
        if(lockContext.readonly){
            return;
        }
        LockContext parent = lockContext.parent;
        if(parent != null && parent.autoEscalte  && parent.parent.parent == null){
            //do autoEsacalte
            if(parent.saturation(transaction) >= 0.2 && parent.capacity >= 10){
                parent.escalate(transaction);
                if(LockType.substitutable(lockContext.getEffectiveLockType(transaction), lockType)){
                    return;
                }
            }
        }

        List<LockType> lockTypes = new ArrayList<>();
        if(lockType == LockType.S){
            lockTypes.add(LockType.IS);
            lockTypes.add(LockType.IX);
            Deque<LockContext> contextStack = getParentContext(lockContext, transaction, lockTypes);
            while(!contextStack.isEmpty()){ //ensure parent's has required locks
                LockContext context = contextStack.removeFirst();
                LockType contextLockType = context.getEffectiveLockType(transaction);
                if(contextLockType == LockType.NL){
                    context.acquire(transaction, LockType.IS);
                }
            }

            if(curr_LockType == LockType.NL){
                lockContext.acquire(transaction, LockType.S);
            }
            else if(curr_LockType == LockType.IS){
                if(lockContext.descendants(transaction).size() != 0){
                    lockContext.escalate(transaction);
                }
                else{
                    lockContext.promote(transaction, lockType);
                }
            }
            else if(curr_LockType == LockType.IX){
                if(lockContext.descendants(transaction).size() != 0){
                    lockContext.specialEscalate(transaction);
                }
                else{
                    lockContext.promote(transaction, LockType.SIX);
                }
            }

        }
        else{ //LockType.X
            //ancestor should have IX or SIX lock
            lockTypes.add(LockType.IX);
            lockTypes.add(LockType.SIX);
            Deque<LockContext> contextStack = getParentContext(lockContext, transaction, lockTypes);
            while(!contextStack.isEmpty()){ //ensure parent's has required locks
                LockContext context = contextStack.removeFirst();
                LockType contextLockType = context.getEffectiveLockType(transaction);
                if(contextLockType == LockType.NL){
                    context.acquire(transaction, LockType.IX);
                }
                else if(contextLockType == LockType.IS){
                    context.promote(transaction, LockType.IX);
                }
                else{
                    context.promote(transaction, LockType.SIX);
                }
            }

            if(lockContext.getExplicitLockType(transaction) == LockType.NL){
                //if no lock is currently held, use acquire method
                lockContext.acquire(transaction, LockType.X);
            }
            else{
                lockContext.escalate(transaction);
                if(lockContext.getEffectiveLockType(transaction) != lockType){
                    lockContext.promote(transaction, LockType.X);
                }
            }
        }
        return;
    }

    // TODO(proj4_part2): add helper methods as you see fit
    private static Deque<LockContext> getParentContext(LockContext lockContext, TransactionContext transaction, List<LockType> lockTypes) {
        Deque<LockContext> contextStack = new LinkedList<>();
        LockContext parent = lockContext.parentContext();
        while (parent != null) {
            if (lockTypes.contains(parent.getEffectiveLockType(transaction))) {
                break;
            }
            contextStack.addFirst(parent);
            parent = parent.parentContext();
        }
        return contextStack;
    }

    public static void ensureIXLockHeld(LockContext lockContext) {
        TransactionContext transaction = TransactionContext.getTransaction();
        LockType curr_LockType = lockContext.getEffectiveLockType(transaction);
        if (transaction == null || curr_LockType == LockType.IX || LockType.substitutable(curr_LockType, LockType.IX)) {
            //do nothing
            return;
        }
        if (lockContext.readonly) {
            return;
        }
        if(curr_LockType == LockType.X){
            return;
        }

        List<LockType> lockTypes = new ArrayList<>();
        lockTypes.add(LockType.SIX);
        lockTypes.add(LockType.IX);
        Deque<LockContext> contextStack = getParentContext(lockContext, transaction, lockTypes);
        while (!contextStack.isEmpty()) { //ensure parent's has required locks
            LockContext context = contextStack.removeFirst();
            LockType contextLockType = context.getEffectiveLockType(transaction);
            if (contextLockType == LockType.NL) {
                context.acquire(transaction, LockType.IX);
            }
            else if(curr_LockType == LockType.S){
                context.promote(transaction, LockType.SIX);
            }
        }
    }




}
