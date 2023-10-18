from airflow.models import BaseOperator
import numpy as np
import pandas as pd
from pathlib import Path
from sktime.forecasting.arima import ARIMA
import ast


rng = np.random.default_rng()

AR_LOWER = 0.1
AR_UPPER = 0.6
MEAN_LOWER = 1000
MEAN_UPPER = 2000
STD = 1


def generate_integrated_autocorrelated_series(
    p: float, mean: float, std: float, length: int
) -> np.ndarray:
    """Generates an integrated autocorrelated time series using a specified autoregression parameter, mean and standard deviation of the normal distribution, and the desired length of the series."""
    x = 0
    ar1_series = np.asarray([x := p * x + rng.normal(0, 1) for _ in range(length)])
    return np.cumsum(ar1_series * std) + mean


def generate_sample_data(
    cols: list[str], x_size: int, y_size: int
) -> tuple[pd.DataFrame, pd.DataFrame, tuple[np.ndarray, np.ndarray]]:
    """
    Generates sample training and test data for specified columns.
    The data consists of autocorrelated series, each created with randomly generated autoregression coefficients and means.
    The method also returns the generated autocorrelation coefficients and means for reference.
    'x_size' determines the length of the training set, and 'y_size' determines the length of the test set.
    'cols' determines the names of the columns.
    """
    ar_coefficients = rng.uniform(AR_LOWER, AR_UPPER, len(cols))
    means = rng.uniform(MEAN_LOWER, MEAN_UPPER, len(cols))
    full_dataset = pd.DataFrame.from_dict(
        {
            col_name: generate_integrated_autocorrelated_series(
                ar_coefficient, mean, STD, x_size + y_size
            )
            for ar_coefficient, mean, col_name in zip(ar_coefficients, means, cols)
        }
    )
    return (
        full_dataset.head(x_size),
        full_dataset.tail(y_size),
        (ar_coefficients, means),
    )


class Model:

    def __init__(self, tickers: list[str], x_size: int = 12, y_size: int = 1) -> None:
        """
        Initialize a Model object.

        Parameters:
            tickers (list[str]): A list of ticker symbols.
            x_size (int, optional): The size of the input data (default is 12).
            y_size (int, optional): The size of the output data (default is 1).
        """
        self.tickers = tickers
        self.x_size = x_size
        self.y_size = y_size
        self.models: dict[str, ARIMA] = {}

    def train(
        self,
        /,
        use_generated_data: bool = False,
        data_str: str | None = None,
    ) -> None:
        """
        Train the model using data.

        Parameters:
            use_generated_data (bool, optional): Whether to use generated data (default is False).
            data_str (str | None, optional): A string containing data or None if not using generated data.
        """
        if use_generated_data:
            data, _, _ = generate_sample_data(self.tickers, self.x_size, self.y_size)
        else:
            data_list = ast.literal_eval(data_str)
            data = pd.DataFrame(data_list)
            data = data.head(self.x_size)

        for ticker in self.tickers:
            dataset = data[ticker].values.astype(float)
            model = ARIMA(order=(1, 1, 0), with_intercept=True, suppress_warnings=True)
            model.fit(dataset)
            self.models[ticker] = model

    def save(self, path_to_dir: str | Path) -> None:
        """
        Save the trained models to a directory.

        Parameters:
            path_to_dir (str | Path): The directory path where models will be saved.
        """
        path_to_dir = Path(path_to_dir)
        path_to_dir.mkdir(parents=True, exist_ok=True)
        for ticker in self.tickers:
            full_path = path_to_dir / ticker
            self.models[ticker].save(full_path)


class TrainModelOperator(BaseOperator):
    """
    Operator to train ML model.
    """

    template_fields = ("_datetime", "_data_str")

    def __init__(self, datetime: str, data_str: str, *args, **kwargs):
        """
        Initialize the operator.

        Args:
        datetime: Datetime string for file/folder naming. (Templatable)
        data_str: Xcom serialized data (str). (Templatable)
        """
        super().__init__(*args, **kwargs)
        self._datetime = datetime
        self._data_str = data_str

    def execute(self, context):
        """
        Execute the operator.
        """

        model = Model(["XAUUSD", "XAGUSD", "XPTUSD", "XPDUSD"])
        model.train(data_str=self._data_str)
        model.save(f"data/models/{self._datetime}")


# if __name__ == "__main__":
#     """FOR TESTING PURPOSES"""
#     model = Model(["XAUUSD", "XAGUSD", "XPTUSD", "XPDUSD"], 12, 1)
#     model.train(use_generated_data=True)
