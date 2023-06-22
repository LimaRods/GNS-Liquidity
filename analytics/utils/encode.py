import functools
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import LabelBinarizer

class TargetEncoder(BaseEstimator, TransformerMixin):

    """
    Args:
        columns (list or str): Columns to target encode
        suffix (str): suffix to attach to newly created variables
        prior_weight (int): weight to assign to the mean. Related to the number of observations necessary before you begin to trust the data over the prior
            ie at 100 we want the global mean to have more weight until we have 101 data points for that category in a feature
    """

    def __init__(self, columns=None, suffix='_enc', prior_weight=100):
        self.columns = columns
        self.suffix = suffix
        self.prior_weight = prior_weight

    def fit(self, X, y=None, **fit_params):

        if not isinstance(X, pd.DataFrame):
            raise ValueError("X must be a DataFrame")

        if not isinstance(y, pd.Series):
            raise ValueError("y must be a Series")

        X = X.copy()

        if self.columns is None:
            columns = X.columns[(X.dtypes == object) | (X.dtypes == 'category')]
            self.columns = columns
        else:
            columns = self.columns

        X = pd.concat([X[columns], y.rename('y')], axis='columns')
        self.prior_ = y.mean()
        self.posteriors_ = {}

        for col in columns:
            agg = X.groupby(col)['y'].agg(['count', 'mean'])
            counts = agg['count']
            means = agg['mean']
            prior_weight = self.prior_weight
            self.posteriors_[col] = ((prior_weight * self.prior_ + counts * means)/ (prior_weight + counts))

        return self

    def transform(self, X, y=None, replace=True):
        
        if not isinstance(X, pd.DataFrame):
            raise ValueError('X has to be a DataFrame')

        X = X.copy()

        for col in self.columns:
            posteriors = self.posteriors_[col]
            if replace:
                X[col] = X[col].map(posteriors).fillna(self.prior_).astype(float)
            else:
                X[col + self.suffix] = X[col].map(posteriors).fillna(self.prior_).astype(float)

        return X