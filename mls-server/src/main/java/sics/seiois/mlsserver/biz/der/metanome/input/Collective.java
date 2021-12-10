package sics.seiois.mlsserver.biz.der.metanome.input;

import sics.seiois.mlsserver.biz.der.metanome.input.partitions.clusters.PLI;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.bitset.IBitSet;

import de.metanome.algorithm_integration.Operator;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.*;
import java.io.*;

// import org.apache.xpath.operations.String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Store information of key-foreign key of multi-relations
 */
public class Collective implements java.io.Serializable {
    private HashMap<String, List<String>> ccm;
    // order of relation names
    //private HashMap<String, Integer> relation_names;

    private HashMap<String, List<Predicate>> pccm;

    // predicates of key and foreign key
    private Collection<Predicate> foreign_keys;
    // map that a tuple id joint with another tuple id
    private List<HashMap<Integer, List<Integer> >> tuple_foreign_maps;
    private Set<Integer> joint_tuple_ids;
    // map a tuple ID pair to IBitSet
    private HashMap<String, IBitSet> collective_evidset;

    // foreign keys
    HashSet<String> fkra;

    private static Logger log = LoggerFactory.getLogger(Collective.class);
    public void configure(Set<Predicate> predicates_fk, String fk_file) {

        if (StringUtils.isEmpty(fk_file)) {
            return;
        }
        this.readFKFile(fk_file);
        // loop all existing foreign keys
        for (Predicate fk : predicates_fk) {
            // check whether the predicate is a FK
            String fk_key_1 = fk.getOperand1().getColumn().getTableName()
                    + "." + fk.getOperand1().getColumn().getName();
            String fk_key_2 = fk.getOperand2().getColumn().getTableName()
                    + "." + fk.getOperand2().getColumn().getName();

            if (! (fkra.contains(fk_key_1) && fkra.contains(fk_key_2))) {
                continue;
            }

            // remove duplicatte FKs
            if (fk.getOperand1().getIndex() != 0 || fk.getOperand2().getIndex() != 0) {
                continue;
            }

            // insert CCM
            this.addJointTables(fk);
            this.addJointPredicates(fk);
            foreign_keys.add(fk);
        }
        this.constructForeignKeyMap();

        // FK
        log.info("# of foreign key predicates is " + foreign_keys.size());
        for (Predicate p : foreign_keys) {
            log.info("FK predicate : " +  p.toString());
        }
    }

    public void readFKFile(String filename) {
        fkra = new HashSet<>();

        FileReader fr = null;
        BufferedReader br = null;
        try {
            File file = new File(filename);
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            StringBuffer sb = new StringBuffer();
            String line;

            while ( (line = br.readLine()) != null) {
                fkra.add(line);
            }

        } catch (IOException e) {
            log.error("IOException error", e);
        } finally{
            try {
                if(br != null){
                    br.close();
                }
            } catch (Exception e) {
                log.error("BufferedReader close error", e);
            }

            try {
                if(fr != null){
                    fr.close();
                }
            } catch (Exception e) {
                log.error("FileReader close error", e);
            }
        }
    }

    public List<Integer> getJointTupleIDs(Predicate join, Integer p1) {
        int sc = 0;
        for (Predicate pp : foreign_keys) {
            if (pp.equals(join)) {
                break;
            }
            sc++;
        }
        // find joint tuple IDs;
        if (tuple_foreign_maps.get(sc).get(p1) != null) {
            return tuple_foreign_maps.get(sc).get(p1);
        }
        return null;
    }

    public IBitSet getEvidence(Integer p1, Integer p2) {
        String key = p1.toString() + "." + p2.toString();
        if (collective_evidset.get(key) == null) {
            return null;
        }
        return collective_evidset.get(key);
    }

    public Collective () {
        ccm = new HashMap<String, List<String>>();
        pccm = new HashMap<>();
        joint_tuple_ids = new HashSet<Integer>();
        foreign_keys = new ArrayList<>();
    }

    public void addJointTables(Predicate p) {
        String relation_name_1 = p.getOperand1().getColumn().getTableName();
        String relation_name_2 = p.getOperand2().getColumn().getTableName();
        if (relation_name_1.equals(relation_name_2) || p.getOperator() != Operator.EQUAL) {
            return;
        }

        if (ccm.get(relation_name_1) == null) {
            List<String> ll = new ArrayList<>();
            ccm.put(relation_name_1, ll);
        }
        // skip duplicate relations
        List<String> jointables = ccm.get(relation_name_1);
        boolean noContain = true;
        for (String r : jointables) {
            if (r.equals(relation_name_2)) {
                noContain = false;
                break;
            }
        }
        if (noContain) {
            ccm.get(relation_name_1).add(relation_name_2);
        }
    }

    public void addJointPredicates(Predicate p) {
        String relation_name_1 = p.getOperand1().getColumn().getTableName();
        String relation_name_2 = p.getOperand2().getColumn().getTableName();
        if (relation_name_1.equals(relation_name_2) || p.getOperator() != Operator.EQUAL) {
            return;
        }

        String key = relation_name_1 + "." + relation_name_2;

        if(pccm.get(key) == null) {
            List<Predicate> ll = new ArrayList<>();
            pccm.put(key, ll);
        }
        pccm.get(key).add(p);
    }

    public Collection<String> checkJointTables(String relation_name) {
       if (this.ccm.get(relation_name) != null) {
           return ccm.get(relation_name);
       }
       return null;
    }

    public Collection<Predicate> checkJointPredicates(String relation_name, String relation_name_exclude) {
        Collection<String> joint_relation_names = checkJointTables(relation_name);
        if (joint_relation_names == null) {
            return null;
        }
        ArrayList<Predicate> jointPredicates = new ArrayList<Predicate>();

        for (String r : joint_relation_names) {
            if (r.equals(relation_name_exclude)) {
                continue;
            }
            if (pccm.get(relation_name + "." + r) != null) {
                jointPredicates.addAll(pccm.get(relation_name + "." + r));
            }
        }

        /*
        int index = relation_names.get(relation_name);
        int index_excl = relation_names.get(relation_name_exclude);
        for (String r : joint_relation_names) {
            int index_r = relation_names.get(r);
            // new joint relation should not be the two existing relations
            if (index == index_r || index_excl == index_r) continue;
            else if (index < index_r) {
                if (pccm.get(relation_name + r) != null) {
                    for (Predicate p : pccm.get(relation_name + r)) {
                        jointPredicates.add(p);
                    }
                }
            } else {
                if (pccm.get(r + relation_name) != null) {
                    for (Predicate p : pccm.get(r + relation_name)) {
                        jointPredicates.add(p);
                    }
                }
            }
        }
        */
        return jointPredicates;
    }


    public void constructForeignKeyMap() {
        int fk = 0;
        tuple_foreign_maps = new ArrayList<>();
        for (Predicate eq : foreign_keys) {
            PLI pliPivot = eq.getOperand1().getColumn().getPli();
            PLI pliProbe = eq.getOperand2().getColumn().getPli();

            Collection<Integer> valuesPivot = pliPivot.getValues();

            HashMap<Integer, List<Integer>> tuple_foreign_map = new HashMap<>();

            for (Integer vPivot : valuesPivot) {
                List<Integer> tidsProbe = pliProbe.getTpIDsForValue(vPivot);
                if (tidsProbe != null) {
                    List<Integer> tidsPivot = pliPivot.getTpIDsForValue(vPivot);
                    for (Integer tidPivot : tidsPivot) {
                        for (Integer tidProbe : tidsProbe) {
                            if (tidPivot.equals(tidProbe)) {
                                continue;
                            }
                            joint_tuple_ids.add(tidPivot);
                            joint_tuple_ids.add(tidProbe);
                            // store the fk relation
                            if (tuple_foreign_map.get(tidPivot) != null) {
                                tuple_foreign_map.get(tidPivot).add(tidProbe);
                            } else {
                                List<Integer> ll = new ArrayList<>();
                                ll.add(tidProbe);
                                tuple_foreign_map.put(tidPivot, ll);
                            }

                            if (tuple_foreign_map.get(tidProbe) != null) {
                                tuple_foreign_map.get(tidProbe).add(tidPivot);
                            } else {
                                List<Integer> ll = new ArrayList<>();
                                ll.add(tidPivot);
                                tuple_foreign_map.put(tidProbe, ll);
                            }

                        }
                    }
                }
            }
            tuple_foreign_maps.add(tuple_foreign_map);
            fk++;
        }
    }

    public boolean ifJoinable(Integer tuple_id) {
        //if (tuple_foreign_maps.get(tuple_id) != null)
        if (joint_tuple_ids.contains(tuple_id)) {
            return true;
        }
        return false;
    }


    public void constructTupleIDPairEvidSet(Map<IBitSet, List<ImmutablePair<Integer, Integer>>> map) {
        collective_evidset = new HashMap<>();
        for (Map.Entry<IBitSet, List<ImmutablePair<Integer, Integer>>> joint : map.entrySet()) {
            for (ImmutablePair<Integer, Integer> pair : joint.getValue()) {
                collective_evidset.put(pair.getLeft().toString() + "." + pair.getRight(), joint.getKey());
            }
        }
    }

}
