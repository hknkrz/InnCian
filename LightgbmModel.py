from datetime import datetime
import lightgbm as lgb
import pandas as pd
from sklearn.metrics import mean_absolute_error, roc_auc_score
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.model_selection import KFold


DATASET_PATH = 'big_dataset_fixed_drop_diplicates.csv'

df = pd.read_csv(DATASET_PATH)

upper_quantile = df['cost'].quantile(0.99)
upper_area_quantile = df['total_area'].quantile(0.99)
lower_area_quantile = df['total_area'].quantile(0.01)

df = df[df['cost'] < upper_quantile]
df = df[(df['total_area'] > lower_area_quantile)&(df['total_area'] < upper_area_quantile)]
df['date'] = df['date'].apply(lambda x: int(datetime.strptime(x, '%d.%m.%Y').strftime('%Y%m%d')))


X = df.drop(['cost','url', 'description'], axis=1)
y = df['cost']


#X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Encode categorical variables
cat_cols = ['region', 'district','metro']
for col in cat_cols:
    #X_train[col] = X_train[col].astype('category')
    #X_test[col] = X_test[col].astype('category')
    X[col] = X[col].astype('category')

# Define the LightGBM dataset
#train_data = lgb.Dataset(X_train, label=y_train)
#test_data = lgb.Dataset(X_test, label=y_test)

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

n_folds = 10
kf = KFold(n_splits=n_folds,shuffle=True,random_state=23)

# Train the model
#model = lgb.train(params, train_data, valid_sets=[train_data, test_data], num_boost_round=10000,
                  #early_stopping_rounds=100)

for train_index, test_index in kf.split(X):
    X_train, X_test = X.iloc[train_index], X.iloc[test_index]
    y_train, y_test = y.iloc[train_index], y.iloc[test_index]
    train_data = lgb.Dataset(X_train, label=y_train)
    test_data = lgb.Dataset(X_test, label=y_test)
    model = lgb.train(params, train_data, num_boost_round=5000, valid_sets=[test_data], early_stopping_rounds=100)


# Predict the cost of properties in the testing dataset
y_pred = model.predict(X_test, num_iteration=model.best_iteration)

# Evaluate the performance of the model using mean absolute error
mae = mean_absolute_error(y_test, y_pred)
mape = mean_absolute_percentage_error(y_test, y_pred)
print('MAE: ', mae)
print('MAPE: ', mape * 100)

# Feature importance analysis
feature_importances = pd.DataFrame()
feature_importances['feature'] = X.columns
feature_importances['importance'] = model.feature_importance()
feature_importances = feature_importances.sort_values('importance', ascending=False)
print(feature_importances)