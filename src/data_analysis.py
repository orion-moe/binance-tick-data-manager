from statsmodels.tsa.stattools import adfuller
from sklearn.model_selection import KFold
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.signal import lfilter
import statsmodels.api as sm
import glob
import os

def adf_stat(close_prices, n=50, downsample=False):

    if downsample == True:
        # Downsample the data by taking every nth point

        downsampled_data = close_prices[::n]
    else:
        downsampled_data = close_prices

    # Perform the ADF test on the downsampled data
    result = adfuller(downsampled_data, maxlag=n, autolag='AIC')

    print('ADF Statistic:', result[0])
    print('p-value:', result[1])
    for key, value in result[4].items():
        print('Critical Value (%s): %.3f' % (key, value))

    if result[1] < 0.05:
        print("Rejected the null hypothesis - time series is stationary.")
        stat = True
    else:
        print("Failed to reject the null hypothesis - time series is not stationary.")
        stat = False
    return stat

# Função para calcular os pesos da diferenciação fracionária
def get_frac_diff_weights(d, n_terms):
    """
    Calcula os pesos para a diferenciação fracionária com um número fixo de termos.
    """
    w = [1.0]
    for k in range(1, n_terms):
        w_k = -w[-1] * (d - k + 1) / k
        w.append(w_k)
    return np.array(w)

# Função otimizada para aplicar a diferenciação fracionária
def fractional_difference(series, d, n_terms):
    """
    Aplica a diferenciação fracionária a uma série temporal de forma eficiente.
    """
    # Calcula os pesos
    weights = get_frac_diff_weights(d, n_terms)
    # Inverte os pesos para usar com lfilter
    weights = weights[::-1]
    # Aplica o filtro
    diff_series = lfilter(weights, [1], series.values)
    # Define os primeiros n_terms - 1 valores como NaN
    diff_series[:n_terms - 1] = np.nan
    # Retorna como uma Série do Pandas
    return pd.Series(diff_series, index=series.index)

def calculate_high_low_volatility_k1(df):
    k1 = 4 * np.log(2)  # Constant k1
    log_ratios = np.log(df['high'] / df['low'])  # Log-ratio of high/low
    volatility_k1 = np.sqrt(log_ratios**2 / k1)  # Apply k1 formula
    return volatility_k1

def calculate_high_low_volatility_k2(df):
    k2 = np.sqrt(np.pi / 8)  # Constant k2
    log_ratios = np.log(df['high'] / df['low'])  # Log-ratio of high/low
    volatility_k2 = log_ratios / k2  # Apply k2 formula
    return volatility_k2

def monteCarlo(serie_predita, constant, params, order):
    out = len(serie_predita)
    Y = np.zeros(out)
    Y[:(order)] = serie_predita.iloc[:(order)]

    for i in range(order, out):
        # Extract the relevant data and transpose if needed
        data = serie_predita.iloc[i-order:i].values
        phi_transpose = np.transpose(params)

        Y[i] = data @ phi_transpose + constant

    Y = pd.DataFrame(Y)
    Y.index = serie_predita.index
    Y.rename(columns={0: 'fraqdiff_pred'}, inplace=True)
    return Y

def auto_reg(order, serie_predita):
    # Create lag matrix
    X = np.column_stack([np.roll(serie_predita, i+1) for i in range(order)])
    X = sm.add_constant(X[order:])  # Add a constant term to the model

    # Fit the autoregressive model
    model = sm.OLS(serie_predita[order:], X)
    result = model.fit()

    # Get the parameters
    constant, params = result.params[0], result.params[:0:-1]
    print(result.summary())

    Y = monteCarlo(serie_predita, constant, params, order)
    return Y, constant, params



# Function to load trades from parquet files
def load_trades_from_parquet(directory, start_date, end_date):
    """
    Load trade data from .parquet files within the specified date range.
    """
    # Get list of parquet files
    parquet_files = glob.glob(os.path.join(directory, '*.parquet'))

    # Initialize list to collect data
    all_data = []

    # Load data from each parquet file
    for file in parquet_files:
        # Read parquet file
        df = pd.read_parquet(file)

        # Convert 'time' to datetime if not already
        if not np.issubdtype(df['time'].dtype, np.datetime64):
            df['time'] = pd.to_datetime(df['time'])

        # Filter data within the date range
        df = df[(df['time'] >= start_date) & (df['time'] < end_date)]

        if not df.empty:
            all_data.append(df)

    # Concatenate all data
    if all_data:
        data = pd.concat(all_data, ignore_index=True)
        # Sort by 'time'
        data = data.sort_values('time').reset_index(drop=True)
        return data
    else:
        print("No data found in the specified date range.")
        return pd.DataFrame()