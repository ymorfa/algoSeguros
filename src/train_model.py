import pandas as pd
import os, sys, json

from statsmodels.tsa.arima.model  import ARIMAResults

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
data_path = os.path.join(root_path, 'data')


class AlgoSegurosForecast:
    def __init__(self, train_data: pd.DataFrame, test_data: pd.DataFrame ) -> None:
        self.all_data = pd.concat([train_data, test_data])
        self.train_data = train_data
        self.test_data = test_data
        self.get_data_params()
        self.get_model()


    def get_data_params(self) ->  None:
        json_file = os.path.join(data_path, 'parameters.json')
        with open(json_file, 'r') as json_file:
            json_data = json.load(json_file)
            params =  json_data.get('param_stand_all')
            self.param_data_mean = params.get('mean')
            self.param_data_std = params.get('std')
    
    def get_model(self) -> None:
        arima_file = os.path.join(root_path,'models', 'arima.pkl')
        self.model_stand_arima = ARIMAResults.load(arima_file)
    

if __name__ == "__main__":
    train = pd.read_csv(os.path.join(data_path, 'processed', 'train_set.csv'), encoding='utf-8')
    test = pd.read_csv(os.path.join(data_path, 'processed', 'test_set.csv'), encoding='utf-8')
    forecast_model = AlgoSegurosForecast(train, test)
