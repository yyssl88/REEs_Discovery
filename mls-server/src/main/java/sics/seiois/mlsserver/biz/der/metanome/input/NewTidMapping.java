package sics.seiois.mlsserver.biz.der.metanome.input;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
@Getter
@Setter
public class NewTidMapping {
    int newTid;
    List<Integer> tuple;
}
