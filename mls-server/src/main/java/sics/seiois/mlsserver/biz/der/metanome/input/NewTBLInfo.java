package sics.seiois.mlsserver.biz.der.metanome.input;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class NewTBLInfo {
    String tblName;
    int tidStart;
    int tupleNum;

    @Override
    public String toString() {
        return "NewTblName:" + tblName + " tidStart:" + tidStart + " tupleNum:" + tupleNum;
    }
}
