package edu.berkeley.cs186.database.query;

public class temp {

    //Returns a 2-array of table name, column name
    private String [] getJoinLeftColumnNameByIndex(int i) {
        return this.joinLeftColumnNames.get(i).split("\\.");
    }

    //Returns a 2-array of table name, column name
    private String [] getJoinRightColumnNameByIndex(int i) {
        return this.joinRightColumnNames.get(i).split("\\.");
    }

    public void join(String tableName, String aliasTableName, String leftColumnName,
                     String rightColumnName) {
        if (this.aliases.containsKey(aliasTableName)) {
            throw new QueryPlanException("table/alias " + aliasTableName + " already in use");
        }
        this.joinTableNames.add(aliasTableName);
        this.aliases.put(aliasTableName, tableName);
        this.joinLeftColumnNames.add(leftColumnName);
        this.joinRightColumnNames.add(rightColumnName);
        this.transaction.setAliasMap(this.aliases);
    }

    private void addJoins() {
        int index = 0;

        for (String joinTable : this.joinTableNames) {
            SequentialScanOperator scanOperator = new SequentialScanOperator(this.transaction, joinTable);

            this.finalOperator = new SNLJOperator(finalOperator, scanOperator,
                    this.joinLeftColumnNames.get(index), this.joinRightColumnNames.get(index),
                    this.transaction);

            index++;
        }
    }
}