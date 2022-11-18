import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt


df = pd.read_csv("../../Desktop/TestData1.csv", header=0, delimiter=',')
print(df.head())
df = df.drop(['mandant_id','loyaltycardnumber','min_k_bon_dat','max_k_bon_dat'], axis=1)
print(df.head())

regional_features = ['cnt_distinct_wup_id']
continous_features = ['cnt_distinct_wg_id','cnt_distinct_bon_id',
                      'cnt_distinct_kl_art_id','cnt_items_in_promo','avg_menge',
                      'avg_umsatz_brutto','sum_menge','sum_umsatz_brutto','avg_number_of_days_between_receipt']

print(df[continous_features].describe())
for col in df[continous_features]:
    dummies = pd.get_dummies(data=[col], prefix=col)
    df = pd.concat([df,dummies], axis=1)
    df.drop(col, axis=1, inplace=True)
mms = MinMaxScaler()
mms.fit(df)
df = df.fillna(0)
data_transformed = mms.transform(df)

Sum_of_squared_distances = []
K = range(1,15)
for k in K:
    km = KMeans(n_clusters=k)
    km = km.fit(data_transformed)
    Sum_of_squared_distances.append(km.inertia_)

print(Sum_of_squared_distances)

plt.plot(K, Sum_of_squared_distances, 'bx-')
plt.xlabel('k')
plt.ylabel('Sum of squared distances')
plt.title('elbow method for optimal k')
plt.show()