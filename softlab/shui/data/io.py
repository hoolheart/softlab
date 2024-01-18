"""
IO functions of data group
"""
import os
from typing import (
    Any,
    Dict,
    Optional,
    Sequence,
)
import logging
from softlab.shui.data.base import (
    DataGroup,
    DataRecord,
    DataChart,
)
from softlab.shui.data.backend import DataBackend
from softlab.shui.data.hdf5 import HDF5DataBackend
from softlab.shui.data.sqlite3 import Sqlite3DataBackend

_logger = logging.getLogger(__name__)  # prepare logger


def get_data_backend(type: str,
                     args: Optional[Dict[str, Any]] = None,
                     connect: bool = True) -> DataBackend:
    """
    Get data backend

    Arguments:
    - type -- backend type
    - args -- connect arguments
    - connect -- whether to connect at beginning

    Returns:
    the backend with the given type

    Throws:
    - If the given type is empty, raise a value error
    - If there is no backend implementation of the given type, raise a
      not-implemented error
    """
    backend_type = str(type)
    if len(backend_type) == 0:
        raise ValueError('Backend type is empty')
    if backend_type == 'hdf5':
        backend = HDF5DataBackend()
    elif backend_type == 'sqlite3':
        backend = Sqlite3DataBackend()
    else:
        raise NotImplementedError(
            f'Backend of type {type} is not implemented'
        )
    if connect:
        if not backend.connect(args):
            _logger.warning(f'Failed to connect: {backend.last_error}')
    return backend


def get_data_backend_by_info(info: Dict[str, Any],
                             connect: bool = True) -> DataBackend:
    """
    Get data backend by using the given information

    Arguments:
    - info -- backend information, 'type' key is necessary, and the optional
            key 'arguments' related to connect arguments
    - connect -- whether to connect at beginning

    Returns:
    the backend with the given type

    Throws:
    - If the given info is not a dictionary, raise a type error
    """
    if not isinstance(info, Dict):
        raise TypeError(f'Type of info is invalid: {type(info)}')
    return get_data_backend(info['type'], info.get('arguments', None), connect)

def load_groups(type: str,
                args: Optional[Dict[str, Any]] = None) -> Sequence[DataGroup]:
    """
    Load all groups from the backend with the given type and arguments

    Arguments:
    - type -- backend type
    - args -- connect arguments

    Returns:
    list of loaded groups
    """
    try:
        backend = get_data_backend(type, args)
        id_list = backend.list_groups()
        return list(filter(
            lambda group: isinstance(group, DataGroup),
            map(lambda id: backend.load_group(id), id_list),
        ))
    except Exception as e:
        _logger.error(f'Exception in loading groups with type {type}'
                      f'and the arguments {args}: {e!r}')
        return []

def reload_group(group: DataGroup) -> Optional[DataGroup]:
    """
    Reload a data group

    Arguments:
    - group -- current data group

    Returns:
    Reloaded data group, ``None`` if the reloading failed

    Throws:
    - If input ``group`` is not a ``DataGroup`` instance, raise a type error
    """
    if not isinstance(group, DataGroup):
        raise TypeError(f'Invalid data group: {type(group)}')
    try:
        backend = get_data_backend_by_info(group.backend)
        return backend.load_group(group.id)
    except Exception as e:
        _logger.error(f'Exception in reloading group {group.id}: {e!r}')
        return None

def save_group(group: DataGroup,
               backend_type: Optional[str] = None,
               backend_args: Optional[Dict[str, Any]] = None) -> bool:
    """
    Save a data group

    Arguments:
    - group -- the data group to save
    - backend_type -- type of backend, ``None`` if to use backend info stored
                      in the data group
    - backend_args -- connect arguments

    Returns:
    Whether the saving succeeded

    Throws:
    - If input ``group`` is not a ``DataGroup`` instance, raise a type error
    """
    if not isinstance(group, DataGroup):
        raise TypeError(f'Invalid data group: {type(group)}')
    try:
        if backend_type is None:
            backend = get_data_backend_by_info(group.backend)
        else:
            backend = get_data_backend(backend_type, backend_args)
            group.backend = {
                'type': backend.backend_type,
                'arguments': backend.arguments,
            }
        return backend.save_group(group)
    except Exception as e:
        _logger.error(f'Exception in saving group {group.id}: {e!r}')
        return False

def extract_group_to_folder(group: DataGroup, folder: str) -> tuple[int, int]:
    """Extract data ``group`` content to files in given ``folder``

    Output record data into seperate csv files, and output charts into
    seperate png images.
    Returns tuple of counts of record files and chart files
    """
    if not isinstance(group, DataGroup):
        raise TypeError(f'Invalid data group {type(group)}')
    folder = str(folder)
    os.makedirs(folder, exist_ok=True)
    record_count = 0
    chart_count = 0
    for r_name in group.records:
        record = group.record(r_name)
        if isinstance(record, DataRecord):
            record.table.to_csv(
                os.path.join(folder, f'{record.name}.csv',),
                sep=',', index=False)
            record_count = record_count + 1
            for c_name in record.charts:
                chart = record.chart(c_name)
                if isinstance(chart, DataChart):
                    chart.write(os.path.join(
                        folder, f'{record.name}.{chart.title}.png'))
                    chart_count = chart_count + 1
    return (record_count, chart_count)
