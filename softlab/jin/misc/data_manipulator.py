"""Manipulating functions on data"""

import numpy as np
import pandas as pd
import io
from matplotlib.figure import Figure
import imageio.v3 as iio

def figure_to_array(figure: Figure, **kwargs) -> np.ndarray:
    """Convert matplot figure to NDArray"""
    buffer = io.BytesIO()
    figure.savefig(buffer, format='png', **kwargs)
    return iio.imread(buffer, index=None)

def compare_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    """Compare columns of ``df1`` and ``df2``, returns True if them match"""
    if isinstance(df1, pd.DataFrame) and isinstance(df2, pd.DataFrame):
        col1 = df1.columns
        col2 = df2.columns
        if len(col1) == len(col2):
            return not (col1.sort_values() != col2.sort_values()).any()
    return False

if __name__ == '__main__':
    import matplotlib.pyplot as plt
    print('Try convert figure to ndarray')
    fig = plt.figure(figsize=(3, 2))
    plt.plot(np.linspace(0, 10, 11), np.linspace(0, 20, 11))
    array = figure_to_array(fig)
    print(f'Shape of ndarray of figure: {array.shape}')
    array = figure_to_array(fig, dpi=360)
    print(f'Shape changes to {array.shape} with dpi 360')

    print('Compare dataframes')
    df1 = pd.DataFrame({'a': [0, 1], 'b': [2, 3]})
    df2 = pd.DataFrame(columns=['b', 'a'])
    df3 = pd.DataFrame(columns=['c', 'b', 'a'])
    df4 = pd.DataFrame({'a': [0, 1], 'c': [2, 3]})
    for a, b in [(df1, df2), (df1, df3), (df1, df4)]:
        print(f'{a.columns}, {b.columns}: {compare_dataframes(a, b)}')
