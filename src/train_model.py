import pandas as pd
import os, sys, json

from statsmodels.tsa.api import VAR
from sklearn.metrics import mean_squared_error
from mango import scheduler, Tuner

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
data_path = os.path.join(root_path, 'data')


def invert_transformation(X_train, pred, diff_order=1):
    forecast = pred.copy()
    columns = X_train.columns
    base_values = X_train.iloc[-1]
    
    for col in columns:
        diff_cumsum = forecast[str(col)+'_pred'].cumsum()
        inverse_diff = base_values[col]

        for _ in range(diff_order):
            inverse_diff += inverse_diff.diff().fillna(0)
        
        forecast[str(col)+'_pred'] = inverse_diff + diff_cumsum
        
    return forecast



class VARIMA:
    def __init__(self, train, order) -> None:
        self.train = train
        p,d,q = order
        self.d = d
        self.p = p
        self.q = q
    
    def fit(self):
        if self.d == 0:
            self.train_transformed = train
        else:
            self.transform()
            
        
        self.fit_ar_model()
        self.fit_ma_model()

    def forecast(self, n_obs):
        input_data = self.train_transformed.values[-self.p:]
        input_data_index = self.train_transformed.index[-self.p:]
        input_data_columns = self.train_transformed.columns
        residual_data = self.ar_residuals.values[-self.q:]
        
        self.pred_ar = self.ar_model.forecast(y=input_data, steps=n_obs)
        self.pred_residual    = self.ma_residuals_model.forecast(y=residual_data, steps=n_obs)
        self.pred_transformed = self.pred_ar + self.pred_residual
        pred_index = pd.date_range(start=input_data_index[-1], periods=n_obs+1, freq='MS')[1:]
        pred_columns = input_data_columns + '_pred'
        self.pred_transformed = (pd.DataFrame(self.pred_transformed, index=pred_index, columns=pred_columns))

        if self.d == 0:
            self.pred = self.pred_transformed
        else:
            self.pred = invert_transformation(self.train, self.pred_transformed, self.d)
      

    def transform(self):
        train_transformed = self.train.copy()
        for _ in range(self.d):
            train_transformed = train_transformed.diff().dropna()
        self.train_transformed = train_transformed
    
    def fit_ar_model(self):
        try:
            ar_model = VAR(self.train_transformed)
            self.ar_model = ar_model.fit(maxlags=self.p, ic='aic')
            self.ar_residuals  = self.ar_model.resid
        except Exception as e:
            raise e
            
    
    def fit_ma_model(self):
        try:
            ma_residuals_model = VAR(self.ar_residuals)
            self.ma_residuals_model = ma_residuals_model.fit(maxlags=self.q, ic='aic')
        except Exception as e:
            raise e

def varima_objective_function(args_list):
    global train, test
    params_evaluated = []
    results = []

    for params in args_list:
        try:
            p, d, q = params['p'],params['d'], params['q']
            model = VARIMA(train, order=(p,d,q))
            model.fit()
            n_obs = test.shape[0]
            model.forecast(n_obs)
            predictions = model.pred
            mse  = mean_squared_error(test.values, predictions.values)
            params_evaluated.append(params)
            results.append(mse)
        except:
            params_evaluated.append(params)
            results.append(1e10)
    return params_evaluated, results


def auto_VARIMA(train, test):
    p_set = range(1, 6)
    d_set = range(0, 3)
    q_set = range(1, 6)

    param_space = dict(p=p_set ,
                   d= d_set ,
                   q =q_set
                  )
    conf_Dict = dict()
    conf_Dict['num_iteration'] = len(p_set)*len(d_set)*len(q_set)
    tuner = Tuner(param_space, varima_objective_function, conf_Dict)
    results = tuner.minimize()
    print('best parameters:', results['best_params'])
    print('best loss:', results['best_objective'])
    return results['best_params']

if __name__ == "__main__":

    print(">>> Cargando Datos")
    df = pd.read_csv(os.path.join(data_path, 'processed', 'processed.csv'), encoding='utf-8', parse_dates=['date'] )
    df.delitos = df.delitos.astype(int)
    print(">>> Terminado\n")

    print(">>> Creando Tabla Pivote")
    df_piv = df.pivot_table(index='date', columns='cod_subtipo', values='delitos', aggfunc='sum')
    df_piv.reset_index(inplace=True)
    df_piv.columns.name = None
    df_piv.set_index('date', inplace=True)
    print(">>> Terminado\n")

    print(">>> Creando Conjuntos de Entranamiento y Prueba")
    train = df_piv[:-4]
    test =  df_piv[-4:]
    print(">>> Terminado\n")


    print(">>> Ajustando Parámetros del Modelo")
    params = auto_VARIMA(train, test)
    p,d,q = params['p'],params['d'], params['q']
    print(">>> Terminado\n")


    print(">>> Entrenando Modelo")
    model = VARIMA(df_piv, order=(p,d,q))
    model.fit()
    model.forecast(3)
    future_predictions = model.pred

    print(future_predictions)
    print(">>> Terminado\n")



