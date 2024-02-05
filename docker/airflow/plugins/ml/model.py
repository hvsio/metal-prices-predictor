import datetime
import os
import time
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sktime.forecasting.arima import ARIMA

rng = np.random.default_rng()

AR_LOWER = 0.1
AR_UPPER = 0.6
MEAN_LOWER = 1000
MEAN_UPPER = 2000
STD = 1


def generate_integrated_autocorrelated_series(
    p: float, mean: float, std: float, length: int
) -> np.ndarray:
    x = 0
    abc = [x * p * x + rng.normal(0, 1) for _ in range(length)]
    return np.cumsum(abc * std) + mean


def generate_sample_data(
    cols: List[str], x_size: int, y_size: int
) -> Tuple[pd.DataFrame, pd.DataFrame, Tuple[np.ndarray, np.ndarray]]:
    """Generates sample training and test data for specified columns. The data consists of autocorrelated series, each created with randomly generated autoregression coefficients and means. The method also returns the generated autocorrelation coefficients and means for reference. 'x_size' determines the length of the training set, and 'y_size' determines the length of the test set. 'cols' determines the names of the columns."""
    ar_coefficients = rng.uniform(AR_LOWER, AR_UPPER, len(cols))
    means = rng.uniform(MEAN_LOWER, MEAN_UPPER, len(cols))
    full_dataset = pd.DataFrame.from_dict(
        {
            col_name: generate_integrated_autocorrelated_series(
                ar_coefficient, mean, STD, x_size + y_size
            )
            for ar_coefficient, mean, col_name in zip(
                ar_coefficients, means, cols
            )
        }
    )
    return (
        full_dataset.head(x_size),
        full_dataset.tail(y_size),
        (ar_coefficients, means),
    )


class Model:
    def __init__(self, tickers: List[str], x_size: int, y_size: int) -> None:
        self.tickers = tickers
        self.x_size = x_size
        self.y_size = y_size
        self.models: dict[str, ARIMA] = {}

    def train(self, use_generated_data: bool = False, data=False) -> None:
        # model expects a dataframe
        if use_generated_data:
            data, _, _ = generate_sample_data(
                self.tickers, self.x_size, self.y_size
            )
        elif not isinstance(data, pd.DataFrame) or len(data) == 0:
            raise ValueError('Missing data')
        for ticker in self.tickers:
            dataset = data[data['metal_type'] == ticker]['price'].values
            model = ARIMA(
                order=(1, 1, 0), with_intercept=True, suppress_warnings=True
            )
            model.fit(dataset)
            self.models[ticker] = model

    def save(
        self, path_to_dir: str, s3_hook: S3Hook, bucket_name: str
    ) -> None:
        path_to_dir = Path(path_to_dir)
        path_to_dir.mkdir(parents=True, exist_ok=True)
        for ticker in self.tickers:
            full_path = path_to_dir / ticker
            self.models[ticker].save(full_path)
            s3_hook.load_file(
                f'{full_path}.zip',
                key=f'{ticker}_{time.time()}.gzip',
                bucket_name=bucket_name,
                gzip=True,
            )
            if os.path.exists(f'{full_path}.zip'):
                os.remove(f'{full_path}.zip')
                os.remove(f'{full_path}.zip.gz')
