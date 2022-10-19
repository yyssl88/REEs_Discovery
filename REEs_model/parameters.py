from sklearn import metrics

LHS_TO_RHS_SYMBOL = '->'
LHS_DELIMITOR_SYMBOL = '^'
MAX_LHS_PREDICATES = 10
MAX_RHS_PREDICATES = 1
TOKENS_OF_PREDICATE = 5
PADDING_VALUE = 'PAD'

def __eval__(predictions, labels):
    accuracy = metrics.accuracy_score(labels, predictions)
    recall = metrics.recall_score(labels, predictions)
    precision = metrics.precision_score(labels, predictions)
    f1_score = metrics.f1_score(labels, predictions)
    return accuracy, recall, precision, f1_score


def __evalR__(predictions, labels):
    mae = metrics.mean_absolute_error(labels, predictions)
    mse = metrics.mean_squared_error(labels, predictions)
    return mae, mse
