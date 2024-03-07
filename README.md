# AlgoSeguros

## Project Structure

```bash
.
│── data            
│   ├── final                       # data after training the model
│   ├── processed                   # data after processing
│   └── raw                         # raw data
├── docs                            # documentation
├── .gitignore                      # ignore files that cannot commit to Git
├── models                          # store models
├── notebooks                       # store notebooks
├── README.md                       # describe the project
└── src                             # store source code
    ├── __init__.py                 # make src a Python module 
    ├── process.py                  # process data before training model
    └── train_model.py              # train model

```

## Configura el ambiente
1. Crea un nuevo ambiente con el comando:

```bash
python3.8 -m venv .venv
```
2. Activa el ambiente

3. Intala las dependencias necesarias usando

```bash
pip install -r requirements.txt
```
4. 
