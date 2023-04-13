import itertools
from datetime import datetime
import random
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.model_selection import KFold

from category_encoders import TargetEncoder
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.ensemble import StackingRegressor
from sklearn.preprocessing import LabelEncoder

DATASET_PATH = 'final_csv.csv'

df = pd.read_csv(DATASET_PATH)

df['offer_date'].fillna(df['offer_date'].mode()[0], inplace=True)

df['offer_date'] = df['offer_date'].apply(lambda x: datetime.strptime(x, "%d.%m.%Y"))

# Convert the 'date' parameter to a datetime object
df['offer_date'] = pd.to_datetime(df['offer_date'], format='%d.%m.%Y')

# Convert the 'date' parameter to a numerical feature
df['date_num'] = (df['offer_date'] - df['offer_date'].min()).dt.days

# Remove the original 'date' parameter
df = df.drop('offer_date', axis=1)
df = df[['cost', 'total_area', 'floor', 'rooms_count', 'region', 'district', 'metro', 'metro_distance', 'floors_number',
         'longitude',
         'latitude', 'date_num']]
df = df.reset_index(drop=True)

X = df.drop('cost', axis=1)
y = df['cost']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Encode categorical variables
cat_cols = ['region', 'district', 'metro']
for col in cat_cols:
    X_train[col] = X_train[col].astype('category')
    X_test[col] = X_test[col].astype('category')

# Define the LightGBM dataset
train_data = lgb.Dataset(X_train, label=y_train)
test_data = lgb.Dataset(X_test, label=y_test)

# Define the hyperparameters
params = {
    'learning_rate': 0.05,
    'boosting_type': 'gbdt',
    'objective': 'regression',
    'metric': 'mae',
    'lambda_l1': 0.2,
    'lambda_l2': 0.2,
    'num_leaves': 127,
    'max_depth': 40,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'verbose': -1,
    'n_jobs': -1
}

params_list = [{
    'learning_rate': 0.05,
    'boosting_type': 'gbdt',
    'objective': 'regression',
    'metric': 'mae',
    'lambda_l1': 0.2,
    'lambda_l2': 0.2,
    'num_leaves': 127,
    'max_depth': 40,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'verbose': -1,
    'n_jobs': -1
}, {
    'learning_rate': 0.01,
    'boosting_type': 'gbdt',
    'objective': 'regression',
    'metric': 'mae',
    'lambda_l1': 0.1,
    'lambda_l2': 0.2,
    'num_leaves': 50,
    'max_depth': 20,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'verbose': -1,
    'n_jobs': -1
}, {
    'learning_rate': 0.1,
    'boosting_type': 'gbdt',
    'objective': 'regression',
    'metric': 'mae',
    'lambda_l1': 0.1,
    'lambda_l2': 0.05,
    'num_leaves': 30,
    'max_depth': 10,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'verbose': -1,
    'n_jobs': -1
}, {
    'learning_rate': 0.05,
    'boosting_type': 'gbdt',
    'objective': 'regression',
    'metric': 'mae',
    'lambda_l1': 0.1,
    'lambda_l2': 0.1,
    'num_leaves': 150,
    'max_depth': 50,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'verbose': -1,
    'n_jobs': -1
}]

n_folds = 5
kf = KFold(n_splits=n_folds, shuffle=True, random_state=42)

feature_sets = [
    [ 'total_area', 'floor', 'rooms_count', 'region', 'district', 'metro', 'metro_distance', 'floors_number',
     'longitude',
     'latitude', 'date_num'], ['total_area', 'floor', 'region', 'district', 'metro', 'metro_distance',
                               'longitude',
                               'latitude', 'date_num'],
    [ 'total_area', 'floor', 'rooms_count', 'region', 'district', 'metro', 'metro_distance',
     'longitude',
     'latitude', 'date_num'],
    [ 'total_area', 'floor', 'region', 'district', 'metro', 'metro_distance', 'floors_number',
     'longitude',
     'latitude', 'date_num']]

feature_x = [ 'total_area', 'floor', 'rooms_count', 'region', 'district', 'metro', 'metro_distance', 'floors_number',
     'longitude',
     'latitude', 'date_num']

models = []

for params, features in zip(params_list, feature_sets):
    for train_index, val_index in kf.split(X_train):
        X_train_fold = X_train[feature_x].iloc[train_index]
        y_train_fold = y_train.iloc[train_index]
        X_val_fold = X_train[feature_x].iloc[val_index]
        y_val_fold = y_train.iloc[val_index]
        lgb_train = lgb.Dataset(X_train_fold, y_train_fold)
        lgb_val = lgb.Dataset(X_val_fold, y_val_fold, reference=lgb_train)
        model = lgb.train(params, lgb_train, num_boost_round=10000,
                          valid_sets=[lgb_train, lgb_val], early_stopping_rounds=100,
                          verbose_eval=50)
        models.append(model)

preds_list = []
for model in models:
    preds = model.predict(X_test)
    preds_list.append(preds)
ensemble_preds = np.mean(preds_list, axis=0)

# Evaluate the performance of the model using mean absolute error
mae = mean_absolute_error(y_test, ensemble_preds)
mape = mean_absolute_percentage_error(y_test, ensemble_preds)
print('MAE: ', mae)
print('MAPE: ', mape * 100)

# Feature importance analysis
feature_importances = pd.DataFrame()
feature_importances['feature'] = X.columns
feature_importances['importance'] = model.feature_importance()
feature_importances = feature_importances.sort_values('importance', ascending=False)
print(feature_importances)
