import pandas as pd
from sklearn.tree import DecisionTreeRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVR



source_data = pd.read_excel('past.xlsx')

test_data = pd.read_excel('test.xlsx')

source_data_train_independent = source_data.drop(['defect'],axis=1)
source_data_train_dependent = source_data['defect'].copy()

sc_x = StandardScaler()
xtrain=sc_x.fit_transform(source_data_train_independent.values)
ytrain=source_data_train_dependent
xtest=sc_x.transform(test_data.values)


svm_reg=SVR(kernel="linear",C=1)
svm_reg.fit(xtrain,ytrain)

predictions=svm_reg.predict(xtest)

print(f"prediction {round(predictions[0],2)}")

tree_reg=DecisionTreeRegressor()

tree_reg.fit(xtrain,ytrain)

decision_predictions=tree_reg.predict(xtest)

print(f"decision prediction {round(decision_predictions[0],2)}")


