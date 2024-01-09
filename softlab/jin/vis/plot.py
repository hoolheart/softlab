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

def plot_record(record: DataRecord,
                ref: Optional[str] = None,
                vars: Optional[Sequence[str]] = None,
                styles: Optional[Sequence[str]] = None,
                title: Optional[str] = None,
                xlabel: Optional[str] = None,
                ylabel: Optional[str] = None,
                legends: Optional[Sequence[str]] = None,
                figsize: Tuple[float, float] = (7.0, 4.0)) -> Figure:
    if not isinstance(record, DataRecord):
        raise TypeError(f'Invalid record {type(record)}')
    columns = record.columns
    x_info: Dict[str, Any] = {}
    x_col: Optional[pd.DataFrame] = None

def subplot_record(record: DataRecord,
                   ref: Optional[str] = None,
                   vars: Optional[Sequence[str]] = None,
                   style: Optional[str] = None,
                   title: Optional[str] = None,
                   xlabel: Optional[str] = None,
                   ylabels: Optional[Sequence[str]] = None,
                   figsize: Tuple[float, float] = (7.0, 4.0)) -> Figure:
    pass

def heatmap_record(record: DataRecord,
                   refs: Tuple[str, str],
                   var: str,
                   cmap: str = 'jet',
                   bar_visible: bool = True,
                   title: Optional[str] = None,
                   xlabel: Optional[str] = None,
                   ylabel: Optional[str] = None,
                   figsize: Tuple[float, float] = (7.0, 7.0)) -> Figure:
    pass
