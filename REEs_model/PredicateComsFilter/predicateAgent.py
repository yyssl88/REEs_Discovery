import numpy as np
import pandas as pd
import copy
import math
import sys
from collections import defaultdict
from enum import Enum

RELATION_ATTRIBUTE = '___'

class Predicate(object):
    def __init__(self, predicate_str):
        self.index1 = None
        self.index2 = None
        self.operator = None
        self.operand1 = None
        self.operand2 = None
        self.constant = None
        res = self.parsePredicate(predicate_str)
        print("Parse predicate : {}, {}".format(predicate_str, res))
        self.index1 = res[0]
        self.operand1 = {'relation': res[1].split(RELATION_ATTRIBUTE)[0].strip(), 'attribute': res[1].split(RELATION_ATTRIBUTE)[1].strip()}
        self.operator = res[2]
        self.index2 = res[3]
        if self.index1 == self.index2:
            # constant
            self.constant = res[4]
        else:
            # operand2
            self.operand2 = {'relation': res[4].split(RELATION_ATTRIBUTE)[0].strip(), 'attribute': res[4].split(RELATION_ATTRIBUTE)[1].strip()}

    def print(self):
        # constant predicates
        if self.constant != None:
            return self.index1 + '.' + self.operand1['relation'] + '.' + self.operand1['attribute'] + ' ' + self.operator + ' ' + self.constant
        else:
            return self.index1 + '.' + self.operand1['relation'] + '.' + self.operand1['attribute'] + ' ' + self.operator + ' ' +  self.index2 + '.' + self.operand2['relation'] + '.' + self.operand2['attribute'] 
            

    def parsePredicate(self, predicate):
        """
        提取规则 核心数据
        :param predicate:
        :return:
        """
        # print('PREDICATE  : ', predicate)
        # check Relation(t0)  ---- 提取规则中的 t0 t1这样的 比如： rule=casorgcn(t0) rule=order(t1)  提取的都是括号里面的 t0 t1
        if predicate.find("(") != -1 and predicate.find(")") != -1 and predicate[:2] != 'ML' and predicate[:len(
                'similar')] != 'similar':
            ss = predicate[predicate.find("(") + 1:predicate.find(")")]
            for i in range(1, len(ss), 1):
                if ss[:i].isalpha() and ss[i:].isdigit():
                    print(ss, i, ss[:i], ss[i:], ss[:i].isalpha, ss[i:].isdigit)
                    return None

        res = None
        # check ML(t0.A, t1.B)
        if predicate[:2] == "ML":
            t = predicate.split('(')
            operator = t[0]
            # find substring in '( ... )'
            op = predicate[predicate.find("(") + 1:predicate.find(")")]
            operand1, operand2 = op.split(',')[0].strip(), op.split(',')[1].strip()
            res = [operand1, operator, operand2]
        elif len(predicate) >= len('similar') and predicate[:len('similar')] == "similar":
            ss = [e.strip() for e in predicate[len('similar') + 1:-1].split(',')]
            # print(ss)
            op = ss[0] + ' ' + ss[-1]
            res = [ss[1], op, ss[2]]
        else:  # check other predicates
            t = predicate.split()
            operand1 = t[0]
            operator = t[1]
            operand2 = ' '.join(t[2:])
            res = [operand1, operator, operand2]

        # process relation operand1 and operand2 and constant
        # relation.tx.attribute or constant (assume that constant does not have ".")
        operand1_ = res[0].split(".")
        index1_ = operand1_[1]
        operand1_new = operand1_[0] + RELATION_ATTRIBUTE + operand1_[2]
        operator_ = res[1]
        operand2_ = res[2].split(".")
        if len(operand2_) != 3 or (len(operand2_) > 1 and operand2_[1][:1] != 't'):
            index2_ = index1_
            operand2_new = res[2]
        else:
            index2_ = operand2_[1]
            operand2_new = operand2_[0] + RELATION_ATTRIBUTE +operand2_[2]

        return [index1_, operand1_new, operator_, index2_, operand2_new]

    def isValidRelation(self, t_index, relation):
        if self.index1 == t_index and self.operand1['relation'] == relation:
            return True
        elif self.index2 == t_index and self.operand2 != None and self.operand2['relation'] == relation:
            return True
        else:
            return False

    def getRelations(self):
        if self.index1 != self.index2:
            return {self.index1: self.operand1['relation'], self.index2: self.operand2['relation']}
        else:
            return {self.index1: self.operand1['relation']}

    def isConstantPredicate(self):
        if self.constant == None:
            return False
        return True

class Status(Enum):
    SUCCESS = 0     # supp >= suppThreshold and conf >= confThreshold
    FAILURE = 1     # supp < suppThreshold
    PENDING = 2     # promising path

class PredicateAgent(object):
    def __init__(self, predicates_num, support_tau, confidence_tau, predicateStrArr):
        super(PredicateAgent, self).__init__()
        self.predicates_num = predicates_num
        self.support_tau = support_tau
        self.confidence_tau = confidence_tau
        self.predicatesArr = []
        for pid, predicate_str in enumerate(predicateStrArr):
            self.predicatesArr.append(Predicate(predicate_str))

        self.nonConstantPredicateIDsArr = []
        for pid, p in enumerate(self.predicatesArr):
            if not p.isConstantPredicate():
                self.nonConstantPredicateIDsArr.append(pid)

    # random selected one non-constant predicate as RHS, return rhs_id
    def randomRHSID(self):
        sc = np.random.randint(0, len(self.nonConstantPredicateIDsArr))
        return self.nonConstantPredicateIDsArr[sc]

    def getAllPredicates(self):
        return self.predicatesArr

    def getPredicatesNums(self):
        return self.predicates_num

    def printAllPredicates(self):
        for pid, p in enumerate(self.predicatesArr):
            print(pid, p.print())

    def reset(self):
        self.current_state = np.zeros(self.predicates_num)
        return copy.deepcopy(self.current_state)

    def initialAction(self, rhs_id):
        # start one nonConstantPredicate
        unselected = []
        for pid, predicate in enumerate(self.predicatesArr):
            if predicate.isConstantPredicate():
                continue
            unselected.append(pid)
        action_id = np.random.randint(0, len(unselected))
        action = unselected[action_id]
        while action == rhs_id:
            action_id = np.random.randint(0, len(unselected))
            action = unselected[action_id]
        self.current_state[action] = 1.0
        return copy.deepcopy(self.current_state)
        '''
        action = np.random.randint(0, self.predicates_num)
        while action == rhs_id:
            action = np.random.randint(0, self.predicates_num)
        self.current_state[action] = 1.0
        return copy.deepcopy(self.current_state)
        '''

    def transformPredicates(self, state):
        predicates_arr = []
        for pid, e in enumerate(state):
            if e == 1:
                predicates_arr.append(self.predicatesArr[pid])
        return predicates_arr

    def step(self, action, rhs_id, validator, max_lhs_predicates, current_conf):
        base_action = np.zeros(self.predicates_num)
        base_action[action] = 1.0
        # next state
        next_state = copy.deepcopy(self.current_state)
        next_state[action] = 1.0
        # reward function
        # 1. get LHS predicates
        lhs_indices = np.nonzero(next_state)[0]
        sequence = " ".join(str(idx) for idx in lhs_indices)
        sequence += ','
        sequence += str(rhs_id)
        #reward = validator.getConfidence(sequence)[0] - current_conf
        print("SEQUENCE : ", sequence)
        validator.getSupportConfidence(sequence)
        support = validator.getSupports()[0]
        confidence = validator.getConfidences()[0]
        reward = confidence - current_conf
        done = False
        if support < self.support_tau or len(lhs_indices) > max_lhs_predicates:
            reward=-10
            done = True

        if confidence >= self.confidence_tau:
            reward = 10
            done = True

        # update state
        self.current_state = next_state
        return copy.deepcopy(self.current_state), reward, done, confidence

    def calculateConf(self, state_eval, rhs_id, validator):
        lhs_indices = np.nonzero(state_eval)[0]
        sequence = " ".join(str(idx) for idx in lhs_indices)
        sequence += ','
        sequence += str(rhs_id)
        # reward = validator.getConfidence(sequence)[0] - current_conf
        validator.getSupportConfidence(sequence)
        #support = validator.getSupports()[0]
        confidence = validator.getConfidences()[0]
        return confidence

    def calculateSuppConf(self, selectedPredicatesXPIDs, rhsPredicatePID, validator):
        '''
        :param selectedPredicatesXPIDs: an array of predicate pids
        :param rhsPredicatePID:
        :param validator:
        :return:
        '''
        sequence = " ".join(str(idx) for idx in selectedPredicatesXPIDs)
        sequence += ","
        sequence += str(rhsPredicatePID)
        validator.getSupportConfidence(sequence)
        support = validator.getSupports()[0]
        confidence = validator.getConfidences()[0]
        return support, confidence

    def stepBegin(self, rhs_id, validator):
        lhs_indices = np.nonzero(self.current_state)[0]
        sequence = " ".join(str(idx) for idx in lhs_indices)
        sequence += ','
        sequence += str(rhs_id)
        # reward = validator.getConfidence(sequence)[0] - current_conf
        validator.getSupportConfidence(sequence)
        support = validator.getSupports()[0]
        confidence = validator.getConfidences()[0]
        done = False
        if support < self.support_tau:
            done = True
        return confidence, done

    ''' Confidence and Support Generate training instances
    '''
    def selectOnePathRandom(self, maxLength, validator):
        selectedPredicatesPIDs = []
        # choose one rhs
        #rhsPredicatePID = np.random.randint(0, self.predicates_num)
        rhsPredicatePID = self.randomRHSID()
        sameRelationsDict = defaultdict(str)
        # update relations of RHS
        r_dict = self.predicatesArr[rhsPredicatePID].getRelations()
        for k, v in r_dict.items():
            if k not in sameRelationsDict:
                sameRelationsDict[k] = v
       
        for _ in range(maxLength):
            unselected = []
            for pid, predicate in enumerate(self.predicatesArr):
                # check rhs
                if pid == rhsPredicatePID:
                    continue
                if pid not in selectedPredicatesPIDs:
                    unselected.append(pid)
                    r_dict = predicate.getRelations()
                    print(r_dict)
                    for k, v in r_dict.items():
                        if k not in sameRelationsDict:
                            sameRelationsDict[k] = v
            print(sameRelationsDict)
            # find all unselected predicate
            unselected_new = []
            if len(selectedPredicatesPIDs) == 0:
                for pid in unselected:
                    if self.predicatesArr[pid].isConstantPredicate():
                         continue
                    # check valid
                    r_dict = self.predicatesArr[pid].getRelations()
                    isValid = True
                    for k, v in r_dict.items():
                        if v != sameRelationsDict[k]:
                            isValid = False
                            break
                    if isValid:
                        unselected_new.append(pid)
            else:
                for pid in unselected:
                    predicate = self.predicatesArr[pid]
                    r_dict = predicate.getRelations()
                    isValid = True
                    for k, v in r_dict.items():
                        if v != sameRelationsDict[k]:
                            isValid = False
                            break
                        if isValid:
                            unselected_new.append(pid)
            # random select one
            sc = np.random.randint(0, len(unselected_new))
            selectedPredicatePID = unselected_new[sc]

            selectedPredicatesPIDs.append(selectedPredicatePID)
            # calculate support and confidence
            support, confidence = self.calculateSuppConf(selectedPredicatesPIDs, rhsPredicatePID, validator)
            if support >= self.support_tau and confidence >= self.confidence_tau:
                return selectedPredicatesPIDs, rhsPredicatePID, Status.SUCCESS
            elif support < self.support_tau:
                return selectedPredicatesPIDs, rhsPredicatePID, Status.FAILURE
        # padding
        return selectedPredicatesPIDs, rhsPredicatePID, Status.PENDING

    def selectMultiPathsRandom(self, validator, N=200):
        maxLength = 5
        data = []
        for _ in range(N):
            pathLength = np.random.randint(1, maxLength)
            selectedPIDs, rhsPID, status = self.selectOnePathRandom(pathLength, validator)
            data.append([selectedPIDs, rhsPID, status])
        return data

    def generateFeature(self, selectedPIDs, rhsPID):
        observation = np.zeros(self.predicates_num * 2)
        observation[selectedPIDs] = 1.0
        observation[self.predicates_num + rhsPID]
        return observation

    def generateTrainingInstances(self, rawTrainData, DQN, validator, max_lhs_predicates):
        trainData, trainLabel = [], []
        for record in rawTrainData:
            selectedPIDs, rhsPID, status = record[0], record[1], record[2]
            if status == Status.SUCCESS:
                feature = self.generateFeature(selectedPIDs, rhsPID)
                trainData.append(feature)
                trainLabel.append(1)
            elif status == Status.FAILURE:
                feature = self.generateFeature(selectedPIDs, rhsPID)
                trainData.append(feature)
                trainLabel.append(0)
            else:
                feature, label = self.generateLabelPerRecord(selectedPIDs, rhsPID, validator, DQN, max_lhs_predicates)
                trainData.append(feature)
                trainLabel.append(label)
        return trainData, trainLabel

    def generateLabelPerRecord(self, selectedPIDs, rhsPID, validator, DQN, max_lhs_predicates):
        selectedPIDs_new = copy.deepcopy(selectedPIDs)
        while True:
            # choose action
            observation = self.generateFeature(selectedPIDs_new, rhsPID)
            action = DQN.choose_action(observation, rhsPID, self.predicatesArr)
            if action == -1:
                break
            status = self.stepForLabel(selectedPIDs_new, action, rhsPID, validator, max_lhs_predicates)
            # take action
            if status == Status.SUCCESS:
                return observation, 1
            elif status == Status.FAILURE:
                return observation, 0
            # go to the next step
            selectedPIDs_new.append(action)
        # generate training instance
        observation = self.generateFeature(selectedPIDs, rhsPID)
        return observation, 0

    def stepForLabel(self, selectedPredicatesPIDs, actionPredicatePID, rhsPID, validator, max_lhs_predicates):
        # next state
        # 1. get LHS predicates
        lhs_indices = selectedPredicatesPIDs + [actionPredicatePID]
        sequence = " ".join(str(idx) for idx in lhs_indices)
        sequence += ','
        sequence += str(rhsPID)
        #reward = validator.getConfidence(sequence)[0] - current_conf
        validator.getSupportConfidence(sequence)
        support = validator.getSupports()[0]
        confidence = validator.getConfidences()[0]
        status = Status.PENDING
        if support < self.support_tau or len(lhs_indices) > max_lhs_predicates:
            status = Status.FAILURE

        if confidence >= self.confidence_tau and support >= self.support_tau:
            status = Status.SUCCESS
        return status






