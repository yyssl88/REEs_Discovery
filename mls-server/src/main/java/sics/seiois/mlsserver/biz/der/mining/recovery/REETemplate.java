package sics.seiois.mlsserver.biz.der.mining.recovery;

import sics.seiois.mlsserver.biz.der.metanome.evidenceset.IEvidenceSet;
import sics.seiois.mlsserver.biz.der.metanome.evidenceset.TroveEvidenceSet;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.utils.ParsedColumnLight;
import sics.seiois.mlsserver.biz.der.mining.utils.PredicateProviderIndex;
import sics.seiois.mlsserver.biz.der.mining.utils.WorkUnit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/*
    one REE template
 */
public class REETemplate {
    private ArrayList<Predicate> constantTemplatesX;
    private ArrayList<Predicate> nonConstantPredicatesX;

    private Predicate rhs;
    private boolean ifConstantRHS; // true if RHS is a constant template, otherwise false;

    static public String LIMITER = ".";


    // store all valid constant predicates with keys : relation_name + LIMITER + tid + LIMITER + attribute_name
    HashMap<String, HashMap<Integer, Predicate>> validConstantValues;

    public ArrayList<WorkUnit> generateWorkUnts_() {
        ArrayList<WorkUnit> workUnits = new ArrayList<>();
        WorkUnit workUnit = new WorkUnit();
        workUnit.addRHS(this.rhs);
        for (Predicate p : this.constantTemplatesX) {
            workUnit.addCurrent(p);
        }
        for (Predicate p : this.nonConstantPredicatesX) {
            workUnit.addCurrent(p);
        }
        workUnits.add(workUnit);
        return workUnits;

    }

    /**
     * Enumerate all constant predicates
     * @return
     */
    private void enumCombinationConstants(Predicate[] currentCPs, ArrayList<Predicate[]> results,
                                          int script, int constantNum, ArrayList<ArrayList<Predicate>> cps) {
        if (script >= constantNum) {
            Predicate[] com = currentCPs.clone();
            results.add(com);
            return;
        }
        for (int i = 0; i < cps.get(script).size(); i++) {
            currentCPs[script] = cps.get(script).get(i);
            enumCombinationConstants(currentCPs, results, script + 1, constantNum, cps);

        }
    }

    public ArrayList<WorkUnit> generateWorkUnits(ArrayList<Predicate> allRealConstantPredicates) {
        HashMap<String, ArrayList<Predicate>> stat = new HashMap<>();
        for (Predicate cp : allRealConstantPredicates) {
            String k = this.genKeyConstantPredicate(cp);
            if (stat.containsKey(k)) {
                stat.get(k).add(cp);
            } else {
                ArrayList<Predicate> cplist = new ArrayList<>();
                cplist.add(cp);
                stat.put(k, cplist);
            }
        }

        ArrayList<WorkUnit> workUnits = new ArrayList<>();
        ArrayList<ArrayList<Predicate>> cps = new ArrayList<>();
        for (int i = 0; i < this.constantTemplatesX.size(); i++) {
            cps.add(stat.get(this.genKeyConstantPredicate(this.constantTemplatesX.get(i))));
        }

        ArrayList<Predicate[]> cpCombs = new ArrayList<>();
        Predicate[] currentCPs = new Predicate[this.constantTemplatesX.size()];
        this.enumCombinationConstants(currentCPs, cpCombs, 0, this.constantTemplatesX.size(), cps);
        for (Predicate[] cplist : cpCombs) {
            WorkUnit workUnit = new WorkUnit();
            workUnit.addRHS(this.rhs);
            for (Predicate p : cplist) {
                workUnit.addCurrent(p);
            }
            for (Predicate p : this.nonConstantPredicatesX) {
                workUnit.addCurrent(p);
            }
            workUnits.add(workUnit);
        }
        return workUnits;
    }

    public void update(PredicateProviderIndex ppi, ArrayList<Predicate> allPredicates, HashMap<String, ParsedColumnLight<?>> colsMap) {


        for (Predicate p : this.constantTemplatesX) {
            p.getOperand1().setColumnLight(colsMap.get(p.getOperand1().getColumn().toStringData()));
            p.getOperand2().setColumnLight(colsMap.get(p.getOperand2().getColumn().toStringData()));
        }
        for (Predicate p : this.nonConstantPredicatesX) {
            Long supp = ppi.getSupportOnePredicate(p);
            if (supp == null) {
                System.out.println("wrong....");
            }
            p.getOperand1().setColumnLight(colsMap.get(p.getOperand1().getColumn().toStringData()));
            p.getOperand2().setColumnLight(colsMap.get(p.getOperand2().getColumn().toStringData()));
            p.setSupport(supp);
        }

        this.rhs.getOperand1().setColumnLight(colsMap.get(this.rhs.getOperand1().getColumn().toStringData()));
        this.rhs.getOperand2().setColumnLight(colsMap.get(this.rhs.getOperand2().getColumn().toStringData()));
        Long supp = ppi.getSupportOnePredicate(this.rhs);
        if (supp == null) {
            System.out.println("wrong RHS....");
        }
        this.rhs.setSupport(supp);

    }

    public REETemplate(ArrayList<Predicate> cTemplates, ArrayList<Predicate> nonCPredicates, Predicate rhs) {
        this.constantTemplatesX = cTemplates;
        this.nonConstantPredicatesX = nonCPredicates;
        if (rhs.isConstant()) {
            this.rhs = rhs;
            this.ifConstantRHS = true;
        } else {
            this.rhs = rhs;
            this.ifConstantRHS = false;
        }
        this.validConstantValues = new HashMap<>();
    }

    private String genKeyConstantPredicate(Predicate cp) {
        return cp.getOperand1().getColumnLight().getTableName() + LIMITER + Integer.toString(cp.getIndex1()) + LIMITER + cp.getOperand1().getColumnLight().getName();
    }

    public void fillRealConstantPredicates(ArrayList<Predicate> realConstantPredicates) {
        for (Predicate cp : realConstantPredicates) {
            String key = this.genKeyConstantPredicate(cp);
            if (this.validConstantValues.containsKey(key)) {
                if (! this.validConstantValues.get(key).containsKey(cp.getConstantInt())) {
                    this.validConstantValues.get(key).put(cp.getConstantInt(), cp);
                }
            } else {
                HashMap<Integer, Predicate> mapP = new HashMap<>();
                mapP.put(cp.getConstantInt(), cp);
                this.validConstantValues.put(key, mapP);
            }
        }
    }

    public Predicate findRealConstantPredicate(Predicate cTemplate, int value) {
        String key = this.genKeyConstantPredicate(cTemplate);
        if (this.validConstantValues.containsKey(key)) {
            if (this.validConstantValues.get(key).containsKey(value)) {
                return this.validConstantValues.get(key).get(value);
            }
        }
        return null;
    }

    public ArrayList<Predicate> getConstantTemplatesX() {
        return this.constantTemplatesX;
    }

    public ArrayList<Predicate> getNonConstantPredicatesX() {
        return this.nonConstantPredicatesX;
    }

    public Predicate getRHS() {
        return this.rhs;
    }


    // check whether constant template
    public boolean ifConstantTemplateRHS() {
        return this.ifConstantRHS;
    }

}
