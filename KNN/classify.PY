import numpy as np
import pandas as pd

data = pd.read_csv("C:\\Users\\Holmes\\Desktop\\iris.csv",index_col=False)
data=data.drop(labels='Unnamed: 0',axis=1)
print(data.head())

trainingSet = list(range(150)) # 训练样本下标
testSet = []           
for i in range(45):
    randIndex = int(np.random.uniform(0, len(trainingSet))) # 获得0~len(trainingSet)的一个随机数
    testSet.append(trainingSet[randIndex])
    del(trainingSet[randIndex])

a= pd.DataFrame()
for item in trainingSet:
    a = a.append(data.iloc[item,0:5])


b= pd.DataFrame()
for item in testSet:
    b = b.append(data.iloc[item,0:5])   


a.to_csv("C:\\Users\\Holmes\\Desktop\\train.csv")
b.to_csv("C:\\Users\\Holmes\\Desktop\\predict.csv")