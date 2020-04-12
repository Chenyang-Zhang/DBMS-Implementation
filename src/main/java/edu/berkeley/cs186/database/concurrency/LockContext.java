package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;
    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;
    // The name of the resource this LockContext represents.
    protected ResourceName name;
    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;
    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;
    // The number of children that this LockContext has, if it differs from the number of times
    // LockContext#childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity should be the number of pages in the table, so we use this
    // field to override the return value for capacity().
    protected int capacity;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    //boolean value for enable autoEscalte
    public boolean autoEscalte;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.capacity = -1;
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
        this.autoEscalte = false;
    }

    /**
     * Gets a lock context corresponding to NAME from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        //First handling exceptions
        if(readonly){
            throw new UnsupportedOperationException("Unsupported Operation!");
        }
        //duplicate lock request exception
        if (getExplicitLockType(transaction) != LockType.NL){
            throw new DuplicateLockRequestException("A lock is already held by TRANSACTION");
        }

        //general cases for invalid lock exception
        if (parentContext() != null) {
            if (!LockType.canBeParentLock(parentContext().getExplicitLockType(transaction), lockType)){
                throw new InvalidLockException("Lock request is invalid!");
            }

        }
        //special case, prohibit IS/S lock if any ancestor has SIX
        if (hasSIXAncestor(transaction)){
            if (lockType == LockType.S || lockType == LockType.IS){
                throw new InvalidLockException("Lock request is invalid!");
            }
        }

        //If no exception happens, guarantee the lock via lock manager
        lockman.acquire(transaction, name, lockType);
        if(parentContext() != null){
            update_numChildLocks(parentContext(), transaction.getTransNum(), 1);
        }


    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
    throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        //Handling Exceptions
        //UnsupportedOperationException
        if (readonly){
            throw new UnsupportedOperationException("Unsupported Operation!");
        }
        //NoLockHeldException
        if(getExplicitLockType(transaction) == LockType.NL){
            throw new NoLockHeldException("No lock is held by TRANSACTION");
        }
        //InvalidLockException
        if(saturation(transaction) > 0){
            throw new InvalidLockException("Lock release is invalid!");
        }

        //If no exception happens, release the lock via lock manager
        lockman.release(transaction, name);
        if(parentContext() != null){
            update_numChildLocks(parentContext(), transaction.getTransNum(), -1);
        }

    }

    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE. For promotion to SIX from IS/IX/S, all S,
     * IS, and SIX locks on descendants must be simultaneously released. The helper function sisDescendants
     * may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)). A promotion
     * from lock type A to lock type B is valid if B is substitutable
     * for A and B is not equal to A, or if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        //Handling Exceptions
        //UnsupportedOperationException
        if(readonly){
            throw new UnsupportedOperationException("Unsupported Operation!");
        }
        LockType curr_LockType = getExplicitLockType(transaction);
        //NoLockHeldException
        if(curr_LockType == LockType.NL){
            throw new NoLockHeldException("No lock is held by TRANSACTION");
        }
        //DuplicateLockRequestException
        if (curr_LockType == newLockType){
            throw new DuplicateLockRequestException("NewLockType is already held by TRANSACTION");
        }
        //InvalidLockException
        //special case for SIX
        if(newLockType == LockType.SIX && hasSIXAncestor(transaction)){
            throw new InvalidLockException("Already has SIX lock on an ancestor!");
        }
        List <ResourceName> resourceNames = sisDescendants(transaction);
        if(newLockType == LockType.SIX && (curr_LockType == LockType.IS || curr_LockType == LockType.IX || curr_LockType == LockType.S)){
            resourceNames.add(name);
            lockman.acquireAndRelease(transaction, name, newLockType, resourceNames);
            for(ResourceName n: resourceNames){
                //update numChildLocks
                LockContext release_context = fromResourceName(lockman, n);
                if(release_context.parentContext() != null){
                    release_context.update_numChildLocks(release_context.parentContext(), transaction.getTransNum(), -1);
                }
            }
            return;
        }

        List<ResourceName> self = new ArrayList<>();
        self.add(name);
        if(curr_LockType == LockType.IS && newLockType == LockType.S){
            lockman.acquireAndRelease(transaction, name, newLockType, self);
            return;
        }
        if(curr_LockType == LockType.IX && newLockType == LockType.X){
            lockman.acquireAndRelease(transaction, name, newLockType, self);
            return;
        }

        //case that not a promotion
        if(!LockType.substitutable(newLockType, curr_LockType)){
            throw new InvalidLockException("Not a promotion");
        }
        //if no exception happens, guarantee the promotion
        lockman.promote(transaction, name, newLockType);
        //we don't need to update numChildLocks during promotion except for the situation above
        return;
    }

    /**
     * Escalate TRANSACTION's lock from descendants of this context to this level, using either
     * an S or X lock. There should be no descendant locks after this
     * call, and every operation valid on descendants of this context before this call
     * must still be valid. You should only make *one* mutating call to the lock manager,
     * and should only request information about TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *
     * You should not make any mutating calls if the locks held by the transaction do not change
     * (such as when you call escalate multiple times in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all relevant contexts, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        //Handling Exceptions
        if(readonly){
            throw new UnsupportedOperationException("Unsupported Operation!");
        }
        if(getExplicitLockType(transaction) == LockType.NL){
            throw new NoLockHeldException("No lock held by transaction at this level");
        }

        List<ResourceName> resourceNames = descendants(transaction);
        if(resourceNames.size() == 0){
            resourceNames.add(name);
            if(getExplicitLockType(transaction) == LockType.IS){
                lockman.acquireAndRelease(transaction, name, LockType.S, resourceNames);
                return;
            }
            if(getExplicitLockType(transaction) == LockType.IX){
                lockman.acquireAndRelease(transaction, name, LockType.X, resourceNames);
            }
            return;
        }
        resourceNames.add(name);
        Set<LockType> set = new HashSet<>();
        for(ResourceName n: resourceNames){
            List<Lock> locks = lockman.getLocks(n);
            for(Lock l: locks){
                set.add(l.lockType);
            }
        }
        //escalate
        if(set.contains(LockType.X) || set.contains(LockType.IX) || set.contains(LockType.SIX)){
            lockman.acquireAndRelease(transaction, name, LockType.X, resourceNames);
        }
        else{
            lockman.acquireAndRelease(transaction, name, LockType.S, resourceNames);
        }
        //update numChildLocks
        for(ResourceName n: resourceNames){
            LockContext release_context = fromResourceName(lockman, n);
            if(release_context.parentContext() != null){
                release_context.update_numChildLocks(release_context.parentContext(), transaction.getTransNum(), -1);
            }
        }
        return;
    }

    /**
     * Update the numChildLocks of parent Context
     * @param parentContext
     * @param transaction_num
     */
    public void update_numChildLocks(LockContext parentContext, long transaction_num, Integer num){
        parentContext.numChildLocks.put(transaction_num, parentContext.numChildLocks.getOrDefault(transaction_num, 0)+num);
    }
    /**
     * Helper method to get a list of resourceNames of all locks(not NL) that are descendants of current context
     * for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction holds any lock
     */
    public List<ResourceName> descendants(TransactionContext transaction) {
        List<ResourceName> resourceNames = new ArrayList<>();
        for(Lock l: lockman.getLocks(transaction)){
            if(l.name.isDescendantOf(name)){
                if(l.lockType != LockType.NL){
                    resourceNames.add(l.name);
                }
            }
        }
        return resourceNames;
    }

    /**
     *special case for escalating: already has IX lock and want S lock. Acquire SIX lock and release S/IS lock on descendants
     * @param transaction
     */
    public void specialEscalate(TransactionContext transaction){
        List<ResourceName> resourceNames = sisDescendants(transaction);
        resourceNames.add(name);
        lockman.acquireAndRelease(transaction, name, LockType.SIX, resourceNames);
        for(ResourceName n: resourceNames){
            LockContext release_context = fromResourceName(lockman, n);
            if(release_context.parentContext() != null){
                release_context.update_numChildLocks(release_context.parentContext(), transaction.getTransNum(), -1);
            }
        }
    }

    /**
     * Gets the type of lock that the transaction has at this level, either implicitly
     * (e.g. explicit S lock at higher level implies S lock at this level) or explicitly.
     * Returns NL if there is no explicit nor implicit lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implement
        LockType explicit_LockType = getExplicitLockType(transaction);

        LockType implicit_LockType = LockType.NL;
        LockContext parent_Context = parentContext();
        while(parent_Context != null){
            implicit_LockType = parent_Context.getExplicitLockType(transaction);
            if (implicit_LockType == LockType.S || implicit_LockType == LockType.SIX){
                implicit_LockType = LockType.S;
                break;
            }
            if (implicit_LockType == LockType.X){
                break;
            }
            parent_Context = parent_Context.parentContext();
        }
        if(explicit_LockType == LockType.NL && (implicit_LockType == LockType.S || implicit_LockType == LockType.X)){
            return implicit_LockType;
        }
        if(explicit_LockType == LockType.IX && implicit_LockType == LockType.S){
            return LockType.SIX;
        }
        return explicit_LockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockType locktype = getExplicitLockType(transaction);
        if (locktype == LockType.SIX){
            return true;
        }
        LockContext parent = parentContext();
        while(parent != null){
            locktype = parent.getExplicitLockType(transaction);
            if (locktype == LockType.SIX){
                return true;
            }
            parent = parent.parentContext();
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or IS and are descendants of current context
     * for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction holds a S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> resourceNames = new ArrayList<>();
        for(Lock l: lockman.getLocks(transaction)){
            if(l.name.isDescendantOf(name)){
                if(l.lockType == LockType.S || l.lockType == LockType.IS){
                    resourceNames.add(l.name);
                }
            }
        }
        return resourceNames;
    }

    /**
     * Get the type of lock that TRANSACTION holds at this level, or NL if no lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implement
        for (Lock l: lockman.getLocks(name)){
            if (l.transactionNum == transaction.getTransNum()){
                return l.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name NAME (with a readable version READABLE).
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                                           this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Sets the capacity (number of children).
     */
    public synchronized void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity. Defaults to number of child contexts if never explicitly set.
     */
    public synchronized int capacity() {
        return this.capacity < 0 ? this.children.size() : this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(TransactionContext transaction) {
        if (transaction == null || capacity() == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity();
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

