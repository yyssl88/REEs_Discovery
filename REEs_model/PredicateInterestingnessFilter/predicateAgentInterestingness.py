import numpy as np
import pandas as pd
import copy
import math
import sys
from collections import defaultdict
from enum import Enum
import sys
sys.path.append('../../REEs_model')
from REEs_model.parameters import *
from REEs_model.utils import *

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
        self.predicateStr = predicate_str
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
                if ss[:i].isalpha and ss[i:].isdigit:
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
        if len(operand2_) < 3:
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


class PredicateAgentInterestingness(object):
    def __init__(self, predicates_num, predicateStrArr):
        super(PredicateAgentInterestingness, self).__init__()
        self.predicates_num = predicates_num
        self.predicatesArr = []
        for pid, predicate_str in enumerate(predicateStrArr):
            self.predicatesArr.append(Predicate(predicate_str))

    def getAllPredicates(self):
        return self.predicatesArr

    def getPredicatesNums(self):
        return self.predicates_num

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

    def transformPredicates(self, state):
        predicates_arr = []
        for pid, e in enumerate(state):
            if e == 1:
                predicates_arr.append(self.predicatesArr[pid])
        return predicates_arr

    def generateInterestingnessFeatures(self, selectedPIDs, rhsPID, tokenVobs):
        # transform to REE rule
        ree = LHS_DELIMITOR_SYMBOL.join([self.predicatesArr[pid].predicateStr for pid in selectedPIDs])
        ree += " " + LHS_TO_RHS_SYMBOL + " " + self.predicatesArr[rhsPID].predicateStr
        rees_lhs, rees_rhs = processAllRules([ree], tokenVobs)
        return rees_lhs, rees_rhs

    def step(self, action, rhs_id, InterestingnessModel, max_lhs_predicates, current_interestingness, tokenVobs):
        # if action is STAY
        if action == len(self.predicatesArr):
            done = True
            return copy.deepcopy(self.current_state), 0, done, 0
        # next state
        next_state = copy.deepcopy(self.current_state)
        next_state[action] = 1.0
        # first check STAY action
        if action == self.predicates_num:
            return copy.deepcopy(next_state), 0, True, current_interestingness
        # reward function
        # 1. get LHS predicates
        selectedPIDs = np.nonzero(next_state)[0]
        rees_lhs, rees_rhs = self.generateInterestingnessFeatures(selectedPIDs, rhs_id, tokenVobs)
        interestingnessScore = InterestingnessModel.compute_interestingness(rees_lhs, rees_rhs)[0]
        reward = interestingnessScore - current_interestingness
        done = False
        if len(selectedPIDs) > max_lhs_predicates:
            done = True
        # update state
        self.current_state = next_state
        return copy.deepcopy(self.current_state), reward, done, interestingnessScore

    def calculateInterestingness(self, selectedPredicatesXPIDs, rhsPredicatePID, InterestingnessModel):
        '''
        :param selectedPredicatesXPIDs: an array of predicate pids
        :param rhsPredicatePID:
        :param validator:
        :return:
        '''
        rees_lhs, rees_rhs = self.generateInterestingnessFeatures(selectedPredicatesXPIDs, rhsPredicatePID)
        interestingnessScore = InterestingnessModel.compute_interestingness(rees_lhs, rees_rhs)
        return interestingnessScore

    def stepBegin(self, rhs_id, InterestingnessModel):
        lhs_indices = np.nonzero(self.current_state)[0]
        rees_lhs, rees_rhs = self.generateInterestingnessFeatures(lhs_indices, rhs_id)
        interestingnessScore = InterestingnessModel.compute_interestingness(rees_lhs, rees_rhs)
        return interestingnessScore

    ''' Interestingness Score Generate training instances
    '''
    def selectOnePathRandom(self, maxLength, InterestingnessModel):
        selectedPredicatesPIDs = []
        # choose one rhs
        rhsPredicatePID = np.random.randint(0, self.predicates_num)
        sameRelationsDict = defaultdict(str)
        for _ in range(maxLength):
            unselected = []
            for pid, predicate in enumerate(self.predicatesArr):
                # check rhs
                if pid == rhsPredicatePID:
                    continue
                if pid not in selectedPredicatesPIDs:
                    unselected.append(pid)
                    r_dict = predicate.getRelations()
                    for k, v in r_dict.items():
                        if k not in sameRelationsDict:
                            sameRelationsDict[k] = v
            # find all unselected predicate
            unselected_new = []
            if len(selectedPredicatesPIDs) == 0:
                for pid in unselected:
                    if self.predicatesArr[pid].isConstantPredicate():
                         continue
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
        # calculate the interestingness score
        interestingnessScore = self.calculateInterestingness(selectedPredicatesPIDs, rhsPredicatePID, InterestingnessModel)

        return selectedPredicatesPIDs, rhsPredicatePID, interestingnessScore

    def selectMultiPathsRandom(self, InterestingnessModel, N=200):
        maxLength = 5
        data = []
        for _ in range(N):
            pathLength = np.random.randint(1, maxLength)
            selectedPIDs, rhsPID, interestingnessScore = self.selectOnePathRandom(pathLength, InterestingnessModel)
            data.append([selectedPIDs, rhsPID, interestingnessScore])
        return data

    def generateFeature(self, selectedPIDs, rhsPID):
        observation = np.zeros(self.predicates_num * 2)
        observation[selectedPIDs] = 1.0
        observation[self.predicates_num + rhsPID]
        return observation

    def generateTrainingInstances(self, rawTrainData, DQN, InterestingnessModel, max_lhs_predicates):
        trainData, trainLabel = [], []
        for record in rawTrainData:
            selectedPIDs, rhsPID, score = record[0], record[1], record[2]

            feature, label = self.generateLabelPerRecord(selectedPIDs, rhsPID, InterestingnessModel, DQN, max_lhs_predicates)
            trainData.append(feature)
            trainLabel.append(label)
        return trainData, trainLabel

    def generateLabelPerRecord(self, selectedPIDs, rhsPID, InterestingnessModel, DQN, max_lhs_predicates):
        selectedPIDs_new = copy.deepcopy(selectedPIDs)
        interestingnessScoreMax = 0
        while True:
            if len(selectedPIDs_new) > max_lhs_predicates:
                break
            # choose action
            observation = self.generateFeature(selectedPIDs, rhsPID)
            action = DQN.choose_action(observation, rhsPID, self.predicatesArr)
            if action == -1 or action == self.predicates_num:
                break
            interestingnessScore = self.calculateInterestingness(selectedPIDs_new + [action], rhsPID, InterestingnessModel)
            # go to the next step
            selectedPIDs_new.append(action)
            interestingnessScoreMax = max(interestingnessScoreMax, interestingnessScore)
        # generate training instance
        observation = self.generateFeature(selectedPIDs_new, rhsPID)
        return observation, interestingnessScoreMax

