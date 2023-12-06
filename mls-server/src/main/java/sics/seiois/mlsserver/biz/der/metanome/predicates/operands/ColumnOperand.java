package sics.seiois.mlsserver.biz.der.metanome.predicates.operands;

import java.io.Serializable;

import sics.seiois.mlsserver.biz.der.metanome.input.ParsedColumn;
import sics.seiois.mlsserver.biz.der.mining.utils.ParsedColumnLight;

public class ColumnOperand<T extends Comparable<T>> implements Serializable {

    private static final long serialVersionUID = -2632843229256239029L;

    private ParsedColumn<T> column;

    // 轻量级列信息（索引等）
    private ParsedColumnLight<T> columnLight;

    private int index;

    public ColumnOperand(ParsedColumn<T> column, int index) {
        this.column = column;
        this.index = index;
        this.columnLight = null;
    }

    public ParsedColumnLight<T> getColumnLight() {
        return columnLight;
    }

    // 轻量级列信息（索引等）
    public void setColumnLight(ParsedColumnLight<?> columnLight) {
        this.columnLight = (ParsedColumnLight<T>) columnLight;
    }

    public void initialParsedColumnLight() {
        if (this.columnLight == null) {
            this.columnLight = new ParsedColumnLight<>(column, column.getType());
        }
    }

    public void accmIndex() {
        index++;
    }

    public T getValue(int line1, int line2) {
        return column.getValue(index == 0 ? line1 : line2);
    }

    public ParsedColumn<T> getColumn() {
        return column;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) { this.index = index; }

    public ColumnOperand<T> getInvT1T2() {
        return new ColumnOperand<>(getColumn(), index == 0 ? 1 : 0);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((column == null) ? 0 : column.hashCode());
        result = prime * result + index;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        return this.toString().equals(obj.toString());
        /*
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ColumnOperand<?> other = (ColumnOperand<?>) obj;
        if (column == null) {
            if (other.column != null)
                return false;
        } else if (!column.equals(other.column))
            return false;
        if (index != other.index)
            return false;
        return true;

         */
    }

    @Override
    public String toString() {
        return column.getTableName() + "." + "t" + index + "." + column.toString();
    }

    public String toString_(int index) {
//        return column.getTableName() + ".t" + index + "." + column.toString();
        //标准列名是不能
        return column.getTableName() + ".t" + index + "." + column.toString().replace(",", "||','||");
    }

    public String toString(int index) {
        return "t" + index + "." + column.toString();
    }

    public void clearData() {
        column.clearData();
        columnLight.clearData();
    }

}
