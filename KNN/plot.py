from sklearn.decomposition import PCA
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from pylab import *  
mpl.rcParams['font.sans-serif'] = ['SimHei'] 
plt.rcParams['axes.unicode_minus']=False

data = pd.read_csv("./predict.csv")


target = data.iloc[:,-2:]
data = data.iloc[:, :-2]



pca = PCA(n_components=2)
pca = pca.fit(data)
data_pca = pca.transform(data)


plt.figure()
plt.scatter(data_pca[target["Species"]=="setosa", 0], data_pca[target["Species"]=="setosa",1], c='red', label="setosa")
plt.scatter(data_pca[target["Species"]=="versicolor", 0], data_pca[target["Species"]=="versicolor", 1], c="blue", label="versicolor")
plt.scatter(data_pca[target["Species"]=="virginica", 0], data_pca[target["Species"]=="virginica", 1], c="orange", label="virginica")
plt.scatter(data_pca[target["Species"]!=target["predict"], 0], data_pca[target["Species"]!=target["predict"],1], s=100, c='black', label="wrong")


plt.legend()
plt.title('预测结果分布')
plt.show()
