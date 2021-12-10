import numpy as np
from sklearn.mixture import GaussianMixture
X = np.loadtxt("z.txt")
gmm = GaussianMixture(n_components=2)
gmm.fit(X)
labels = gmm.predict(X)
print(labels)