package edu.berkeley.cs186.database.concurrency;

// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (a == NL || b == NL){
            return true;
        }
        if (a == X || b == X){
            return false;
        }
        if (a == IS || b == IS){
            return true;
        }
        if (a == SIX || b ==SIX){
            return false;
        }
        if (a == b){
            return true;
        }
        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (childLockType == NL){
            return true;
        }
        else if (childLockType == IS){
            if (parentLockType == IS || parentLockType ==IX){
                return true;
            }
        }
        else if (childLockType == IX){
            if (parentLockType == IX || parentLockType == SIX){
                return true;
            }
        }
        else if(childLockType == S){
            if (parentLockType == IS ||parentLockType == IX){
                return true;
            }
        }
        else if (childLockType == SIX){
            if (parentLockType == IX){
                return true;
            }
        }
        else{
            if (parentLockType == IX || parentLockType == SIX){
                return true;
            }
        }
        return false;

    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (substitute == required){
            return true;
        }
        else if (substitute == NL){
            return false;
        }
        else if (required == NL){
            return true;
        }
        else if (substitute == IS){
            return false;
        }
        else if (substitute == X){
            return true;
        }
        else if (required == SIX || required == X){
            return false;
        }
        else if (required == IX){
            return false;
        }
        else if (required == IS){
            if (substitute == IX || substitute == S){
                return true;
            }
            return false;
        }
        else if (substitute == IX){
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

