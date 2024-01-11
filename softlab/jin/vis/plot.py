from typing import (
    Any,
    Optional,
    Sequence,
    Tuple,
    Dict,
)
import pandas as pd
import matplotlib.pylab as plt
from matplotlib.figure import Figure
from softlab.shui.data import DataRecord

PLOT_COLORS = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
"""Recommend cadidate colors to plot"""


def plot_record(record: DataRecord,
                ref: str,
                vars: Sequence[str],
                styles: Optional[Sequence[str]] = None,
                title: Optional[str] = None,
                xlabel: Optional[str] = None,
                ylabel: Optional[str] = None,
                legends: Optional[Sequence[str]] = None,
                figsize: Optional[Tuple[float, float]] = None,
                **kwargs) -> Figure:
    """
    Plot record into a figure

    Arguments:
    - record --- DataRecord to plot
    - ref --- key of column as x-axis
    - vars --- sequence of column keys to plot
    - styles --- sequence of styles, optional
    - title --- plot title, optional
    - xlabel --- x-axis label, optional, use ref's label if None
    - ylabel --- y-axis label, optional
    - legends --- sequence of legend labels, use columns' labels if None
    - figsize --- size of figure, optional
    - kwargs --- other keywork arguments passing to ``plt.plot`` function
    """
    if not isinstance(record, DataRecord):
        raise TypeError(f'Invalid record {type(record)}')
    columns = dict(map(lambda info: (info['name'], info), record.columns))
    if not ref in columns:
        raise ValueError(f'Invalid ref column {ref}')
    if not isinstance(vars, Sequence) or len(vars) == 0:
        raise ValueError(f'Empty variable list')
    for v in vars:
        if not v in columns:
            raise ValueError(f'Invalid variable {v}')
    if isinstance(styles, Sequence) and len(styles) != len(vars):
        raise ValueError(f'Sizes of variables {len(vars)} and '
                         f'styles {len(styles)} are different')
    if isinstance(legends, Sequence) and len(legends) != len(vars):
        raise ValueError(f'Sizes of variables {len(vars)} and '
                         f'legends {len(legends)} are different')
    if not isinstance(styles, Sequence):
        styles = list(map(
            lambda i: '{}o-'.format(PLOT_COLORS[i % len(PLOT_COLORS)]),
            range(len(vars))))
    if not isinstance(legends, Sequence):
        legends = list(map(lambda v: columns[v]['label'], vars))
    fig = plt.figure(figsize=figsize)
    ref_col = record.column(ref)
    ref_info = columns[ref]
    for v, s in zip(vars, styles):
        plt.plot(ref_col, record.column(v), s, **kwargs)
    if isinstance(title, str):
        plt.title(title)
    if isinstance(xlabel, str):
        plt.xlabel(xlabel)
    else:
        plt.xlabel(ref_info['label'])
    if isinstance(ylabel, str):
        plt.ylabel(ylabel)
    plt.legend(legends)
    return fig


def subplot_record(record: DataRecord,
                   ref: str,
                   vars: Sequence[str],
                   style: Optional[str] = None,
                   title: Optional[str] = None,
                   xlabel: Optional[str] = None,
                   ylabels: Optional[Sequence[str]] = None,
                   subsize: Optional[Tuple[float, float]] = None,
                   **kwargs) -> Figure:
    """
    Illustrate record as a subplot figure

    Arguments:
    - record --- DataRecord to plot
    - ref --- key of column as x-axis
    - vars --- sequence of column keys to plot
    - styles --- sequence of styles, optional
    - title --- plot title, optional
    - xlabel --- x-axis label, optional, use ref's label if None
    - ylabels --- sequence of subplot y-axis labels, use columns' labels if None
    - subsize --- size of each subplot, use (7, 4) if None
    - kwargs --- other keywork arguments passing to ``plt.plot`` function
    """
    if not isinstance(record, DataRecord):
        raise TypeError(f'Invalid record {type(record)}')
    columns = dict(map(lambda info: (info['name'], info), record.columns))
    if not ref in columns:
        raise ValueError(f'Invalid ref column {ref}')
    if not isinstance(vars, Sequence) or len(vars) == 0:
        raise ValueError(f'Empty variable list')
    for v in vars:
        if not v in columns:
            raise ValueError(f'Invalid variable {v}')
    if not isinstance(style, str) or len(style) == 0:
        style = f'{PLOT_COLORS[0]}o-'
    if isinstance(ylabels, Sequence) and len(ylabels) != len(vars):
        raise ValueError(f'Sizes of variables {len(vars)} and '
                         f'ylabels {len(ylabels)} are different')
    if not isinstance(ylabels, Sequence):
        ylabels = list(map(lambda v: columns[v]['label'], vars))
    if not isinstance(subsize, Tuple):
        subsize = (7.0, 4.0)
    figsize = (subsize[0], subsize[1] * len(vars))
    fig = plt.figure(figsize=figsize)
    ref_col = record.column(ref)
    ref_info = columns[ref]
    ax1 = plt.subplot(len(vars), 1, 1)
    if isinstance(title, str):
        plt.title(title)
    for idx, v, label in zip(range(len(vars)), vars, ylabels):
        if idx > 0:
            plt.subplot(len(vars), 1, idx + 1, sharex=ax1)
        plt.plot(ref_col, record.column(v), style, **kwargs)
        if idx == len(vars) - 1:
            plt.xlabel(xlabel if isinstance(
                xlabel, str) else ref_info['label'])
        plt.ylabel(label)
    return fig


def colormesh_record(record: DataRecord,
                     refs: Tuple[str, str],
                     var: str,
                     shape: Tuple[int, int],
                     cmap: str = 'jet',
                     bar_visible: bool = True,
                     title: Optional[str] = None,
                     xlabel: Optional[str] = None,
                     ylabel: Optional[str] = None,
                     figsize: Optional[Tuple[float, float]] = None,
                     **kwargs) -> Figure:
    """
    Illustrate record as a color-mesh figure

    Arguments:
    - record --- DataRecord to plot
    - refs --- pair of columns' keys corresponding x-axis and y-axis
    - var --- key of column as color value
    - shape --- shape of colormesh, tuple of two integers, their product must be
                equal to size of data
    - cmap --- color mapping style, default is "jet"
    - bar_visible --- whether to show color bar, default is False
    - title --- plot title, optional, use var's label if None
    - xlabel --- x-axis label, optional, use refs[0]'s label if None
    - ylabel --- y-axis label, optional, use refs[1]'s label if None
    - figsize --- size of figure, optional
    - kwargs --- other keywork arguments passing to ``plt.pcolormesh`` function
    """
    if not isinstance(record, DataRecord):
        raise TypeError(f'Invalid record {type(record)}')
    elif record.shape[0] == 0:
        raise ValueError(f'No data in {record}')
    columns = dict(map(lambda info: (info['name'], info), record.columns))
    if not isinstance(refs, Tuple) or len(refs) != 2:
        raise ValueError(f'Invalid ref pair {refs}')
    ref0 = refs[0]
    ref1 = refs[1]
    if not ref0 in columns:
        raise ValueError(f'Reference {ref0} non exist')
    if not ref1 in columns:
        raise ValueError(f'Reference {ref0} non exist')
    if not var in columns:
        raise ValueError(f'Variable {var} non exist')
    if ref0 == ref1 or ref0 == var or ref1 == var:
        raise ValueError(f'Multiple usage of columns {ref0}, {ref1}, {var}')
    if not isinstance(shape, Tuple) or len(shape) != 2 or \
            not isinstance(shape[0], int) or not isinstance(shape[1], int) or \
            shape[0] * shape[1] != record.shape[0]:
        raise ValueError(f'Invalid shape {shape}')
    df = record.table.loc[:, (ref0, ref1, var)].sort_values(by=[ref0, ref1])
    fig = plt.figure(figsize=figsize)
    plt.pcolormesh(df[ref0].to_numpy().reshape(shape),
                   df[ref1].to_numpy().reshape(shape),
                   df[var].to_numpy().reshape(shape),
                   cmap=cmap, **kwargs)
    plt.title(title if isinstance(title, str) else columns[var]['label'])
    plt.xlabel(xlabel if isinstance(xlabel, str) else columns[ref0]['label'])
    plt.ylabel(ylabel if isinstance(ylabel, str) else columns[ref1]['label'])
    if bar_visible:
        plt.colorbar()
    return fig
