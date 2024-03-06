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

## Set up the environment
1. Create the virtual environment:
```bash
python3.8 -m venv .venv
```
2. Activate the virtual environment

3. Install dependencies:

- To install necesary dependencies, run:
```bash
pip install -r requirements.txt
```

