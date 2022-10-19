import random
import pandas as pd


# data_name = "airports"
# data_name = "inspection"
# data_name = "ncvoter"
data_name = "hospital"


if data_name == "hospital":
    rules_file = data_name + "/" + data_name + "_rules_discovered.csv"
else:
    rules_file = data_name + "/" + data_name + "_rules_discovered.txt"
relation = data_name
revised_rules_file = data_name + "/" + data_name + "_rules.txt"
num_pairs = 800

def parse(X):
    ss = [e.strip() for e in X.split()]
    # indices = []
    # for i in range(len(ss)-1, -1, -1):
    #     if ss[i] == "==":
    #         indices.append(i)
    # for i in range(0, len(indices)-1):
    #     if indices[i] - indices[i-1] > 3:
    #         for j in range():

    tag = False
    while tag is False:
        tag = True
    for i in range(0, len(ss) - 3):
        if relation is "ncvoter" and "election_phase" in ss[i] and ss[i + 1] == "==" and "election_phase" not in ss[i + 2]:
            ss[i + 2] = ss[i + 2] + " " + ss[i + 3]
            ss.remove(ss[i + 3])
            tag = False
        if relation is "inspection" and "Inspection_Type" in ss[i] and ss[i + 1] == "==" and "Canvass" in ss[i + 2] and "Re-Inspection" in ss[i + 3]:
            ss[i + 2] = ss[i + 2] + " " + ss[i + 3]
            ss.remove(ss[i + 3])
            tag = False
        if relation is "inspection" and "Facility_Type" in ss[i] and ss[i + 1] == "==" and "Grocery" in ss[i + 2] and "Store" in ss[i + 3]:
            ss[i + 2] = ss[i + 2] + " " + ss[i + 3]
            ss.remove(ss[i + 3])
            tag = False

    scc = -1
    for sc in range(len(ss)):
        for rname in relation.split(";"):
            if ss[sc].find(rname) != -1:
                scc = sc
                break
        if scc != -1:
            break
    res = []
    for i in range(scc, len(ss), 3):
        idx1 = ss[i].find(".")
        idx2 = ss[i+2].find(".")
        res.append(ss[i][idx1+1:] + " " + ss[i + 1] + " " + ss[i + 2][idx2+1:])
    res.sort()
    return res


def loadRule(filename):
    rules = []
    supports = []
    confidences = []
    concisenesses = []
    for line in open(filename):
        line = line.strip()
        if line.find("->") == -1:
            continue
        idx = line.find("[")
        line = line[idx:]
        if line[0:1] == "[":
            line = line[1:]
        idx = line.find("]")
        supp = None
        conf = None
        if idx != -1:
            supp_conf = line[idx+2:]
            supp = supp_conf.split(",")[0].split(":")[1].strip()
            conf = supp_conf.split(",")[1].split(":")[1].strip()
            line = line[:idx]
        ss = line.split("->")
        X, rhs = ss[0].strip(), ss[1].strip()

        X_ss = parse(X)
        rhs = rhs.split('], supp')[0].split(" ")
        idx1 = rhs[0].find(".")
        idx2 = rhs[2].find(".")
        rhs = rhs[0][idx1+1:] + " " + rhs[1] + " " + rhs[2][idx2+1:]
        conciseness = 1.0 / len(X_ss)
        rule = relation + "(t0) ⋀ " + relation + "(t1) ⋀ " + (" ⋀ ").join(X_ss) + "  ->  " + rhs + "," + supp + "," + conf + "," + str(conciseness)
        rules.append(rule)
        supports.append(int(supp))
        confidences.append(float(conf))
        concisenesses.append(conciseness)
    return rules, supports, confidences, concisenesses


def main():
    if data_name == "hospital":
        rules_info = pd.read_csv(rules_file)
        rules = rules_info["rule"].tolist()
        supports = rules_info["cr"].tolist()
        confidences = rules_info["confidence"].tolist()
        concisenesses = []
        for rule in rules:
            num_predicates_X = len(rule.split("⋀")) - 2
            concisenesses.append(1.0 / num_predicates_X)
    else:
        rules, supports, confidences, concisenesses = loadRule(rules_file)

    with open(revised_rules_file, "w", encoding='utf-8') as f:
        f.write("rule,support_ratio,confidence,conciseness\n")
        if data_name != "hospital":
            for rule in rules:
                f.write(rule)
                f.write("\n")
        else:
            for i in range(len(rules)):
                f.write(rules[i] + "," + str(supports[i]) + "," + str(confidences[i]) + "," + str(concisenesses[i]))
                f.write("\n")
    f.close()

    size = len(rules)
    rule_pairs = set()
    while len(rule_pairs) < num_pairs:
        id1 = random.randint(0, size-1)
        id2 = random.randint(0, size-1)
        while id1 == id2:
            id2 = random.randint(0, size)
        if id1 <= id2:
            rule_pairs.add((id1, id2))
        else:
            rule_pairs.add((id2, id1))
    print(len(rule_pairs), len(rules))

    f1 = open(data_name + "/rulesToBeLabeled.txt", "w", encoding="utf-8")
    f2 = open(data_name + "/labeled_data.txt", "w", encoding="utf-8")
    k = 0
    for pair in rule_pairs:
        rid1 = pair[0]
        rid2 = pair[1]
        f1.write(str(k) + ": (" + str(rid1) + "," + str(rid2) + ") \n")
        if data_name != "hospital":
            f1.write(rules[rid1])
            f1.write("\n")
            f1.write(rules[rid2])
        else:
            f1.write(rules[rid1] + "," + str(supports[rid1]) + "," + str(confidences[rid1]) + "," + str(concisenesses[rid1]))
            f1.write("\n")
            f1.write(rules[rid2] + "," + str(supports[rid2]) + "," + str(confidences[rid2]) + "," + str(concisenesses[rid2]))
        f1.write("\n\n")
        k = k + 1

        # method-1: manually
        # f2.write(str(rid1) + " " + str(rid2) + " \n")

        # method-2: automatically
        f2.write(str(rid1) + " " + str(rid2) + " ")
        # if concisenesses[rid1] == concisenesses[rid2]:
        #     if supports[rid1] == supports[rid2]:
        #         if confidences[rid1] > confidences[rid2]:
        #             f2.write("0\n")
        #         else:
        #             f2.write("1\n")
        #     elif supports[rid1] > supports[rid2]:
        #         f2.write("0\n")
        #     else:
        #         f2.write("1\n")
        # elif concisenesses[rid1] > concisenesses[rid2]:
        #     f2.write("0\n")
        # else:
        #     f2.write("1\n")

        if confidences[rid1] == confidences[rid2]:
            if supports[rid1] == supports[rid2]:
                if concisenesses[rid1] > concisenesses[rid2]:
                    f2.write("0\n")
                else:
                    f2.write("1\n")
            elif supports[rid1] > supports[rid2]:
                f2.write("0\n")
            else:
                f2.write("1\n")
        elif confidences[rid1] > confidences[rid2]:
            f2.write("0\n")
        else:
            f2.write("1\n")

    f1.close()
    f2.close()


if __name__ == '__main__':
    main()