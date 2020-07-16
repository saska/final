import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

class CrossSectionalRegression:
    """Cross sectional (Fama-Macbeth) regression.
    """
    def __init__(self, data, grouper, x_name, y_name):
        self.data = data
        self.grouper = grouper
        self.x_name = x_name
        self.y_name = y_name
        
    def fit(self):
        self.regs = {}
        for name, frame in self.data.groupby(self.grouper):
            reg = LinearRegression()
            reg.fit(frame[self.x_name], frame[self.y_name])
            self.regs[name] = reg
        return self
    
    def coefs(self, names=False):
        """Average coefficients across all regressions"""
        try:
            coefs = np.array([r.coef_ for r in self.regs.values()]).mean(axis=0)
        except AttributeError as e:
            raise AttributeError('Regression has not been fit') from e
        if not names: return coefs
        return dict(zip(self.x_name, coefs))
    
    def predict(self, df, grouper=None, x_name=None):
        grouper = grouper if grouper else self.grouper
        x_name = x_name if x_name else self.x_name
        preds = pd.Series(index=df.index, dtype='float64')
        for name, frame in df.groupby(grouper):
            preds.loc[frame.index] = self.regs[name].predict(frame[x_name])
        return preds