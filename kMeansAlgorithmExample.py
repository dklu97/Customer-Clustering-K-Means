#importing libraries needed for the k means clustering

import pandas as pd
import numpy as np
import random as rd
import matplotlib.pyplot as plt


data = pd.read_csv("clustering.csv")
print(data.head())

x = data[["LoanAmount", "ApplicantIncome"]]
# visualize the data points
print(x)

plt.scatter(x["ApplicantIncome"],x["LoanAmount"], c="black")
plt.show()

k=2

# Select random as centroids
centroids = (x.sample(n=k))
plt.scatter(x["ApplicantIncome"],x["LoanAmount"], c = "black")
plt.scatter(centroids["ApplicantIncome"], centroids["LoanAmount"], c = "red")
plt.show()

diff = 1
j = 0

while (diff!=0):
    xd = x
    i = 1
    for index1, row_c in centroids.iterrows():
        ed =[]
        for index2,row_d in xd.iterrows():
            d1=(row_c["ApplicantIncome"]-row_d["ApplicantIncome"])**2
            d2=(row_c["LoanAmount"]-row_d["LoanAmount"])**2
            d=np.sqrt(d1+d2)
            ed.append(d)
        x[i]=ed
        i = i+1
    c = []
    for index,row in x.iterrows():
        print(row[1])
        min_dist=row[1]
        pos=1
        for i in range (k):
            if row[i+1] < min_dist:
                min_dist = row[i+1]
                pos= i+1
        c.append(pos)
    print(c)
    x["Cluster"]=c

    centroids_new = x.groupby(["Cluster"]).mean()[["LoanAmount","ApplicantIncome"]]
    if j == 0:
        diff=1
        j=j+1
    else:
        diff = (centroids_new["LoanAmount"] - centroids["LoanAmount"]).sum() + (centroids_new["ApplicantIncome"] - centroids["ApplicantIncome"]).sum()
        print(diff.sum())
    centroids = x.groupby(["Cluster"]).mean()[["LoanAmount","ApplicantIncome"]]

color=["blue","green","cyan"]
for o in range(k):
    data=x[x["Cluster"]==o+1]
    plt.scatter(data["ApplicantIncome"],data["LoanAmount"],c=color[o])
plt.scatter(centroids["ApplicantIncome"],centroids["LoanAmount"], c="red")

plt.show()