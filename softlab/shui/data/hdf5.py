"""
Data backend by HDF5
"""
from typing import (
    Any,
    Dict,
    Optional,
    Sequence,
)
import numpy as np
import pandas as pd
import h5py
import uuid
import json
from datetime import datetime
from softlab.shui.data.base import (
    DataGroup,
    DataRecord,
    DataChart,
)
from softlab.shui.backend import catch_error
from softlab.shui.data.backend import DataBackend

class HDF5DataBackend(DataBackend):
    """Data backend using HDF5. A subclass of DataBackend"""

    def __init__(self):
        super().__init__(type='hdf5')
        self.file = None

    @catch_error(failed_return=False, action='connect')
    def connect_impl(self, args: Dict[str, Any]) -> bool:
        """
        Arguments:
        - args -- hdf5 backend arguments
            - path -- absolute path to the hdf5 data file.
        """
        path = args['path'] if 'path' in args else 'data.hdf5'
        self.file = h5py.File(path, 'a')
        print(f'Connected to HDF5 backend, data is stored in {path}.')
        return True

    @catch_error(failed_return=False, action='disconnect')
    def disconnect_impl(self) -> bool:
        self.file.close()
        self.file = None
        return True

    @catch_error(failed_return=[], action='list groups')
    def list_impl(self) -> Sequence[str]:
        return list(self.file.keys())

    @catch_error(failed_return=None, action='load group')
    def load_impl(self, id: str) -> Optional[DataGroup]:
        hdf5_data_grp = self.file[id]
        data_group = self.load_data_group_from_hdf5_group(hdf5_data_grp)
        data_group.backend = {
            'type': 'hdf5',
            'arguments': {'path': self.file.filename},
        }
        self.mark_success()
        return data_group

    @catch_error(failed_return=False, action='save group')
    def save_impl(self, data_grp: DataGroup) -> bool:
        """
        Save DataGroup object in hdf5 backend.

        The hierarchy of the data saved in hdf5 is:
        - ``/data_group/data_record/table/column`` or
        - ``/data_group/data_record/charts/chart``
        """
        if str(data_grp.id) in self.file:
            del self.file[str(data_grp.id)]

        hdf5_data_grp = self.file.create_group(str(data_grp.id))

        hdf5_data_grp.attrs['id'] = str(data_grp.id)
        hdf5_data_grp.attrs['name'] = str(data_grp.name)
        hdf5_data_grp.attrs['timestamp'] = data_grp.timestamp.isoformat()
        for key, value in data_grp.meta.items():
            hdf5_data_grp.attrs[key] = str(value)

        hdf5_data_grp.attrs['backend'] = json.dumps({
            'type': 'hdf5', 'arguments': {'path': self.file.filename},
        })

        # save data records
        data_record_names = data_grp.records
        for name in data_record_names:
            data_record = data_grp.record(name)
            self.save_record(hdf5_data_grp, data_record)
        self.mark_success()
        return True

    @staticmethod
    def save_record(hdf5_data_grp: h5py.Group, data_record: DataRecord) -> None:
        """
        Save a DataRecord object as a hdf5 group.

        Each column of the object's table is saved as a hdf5 dataset.
        """
        if data_record.name in hdf5_data_grp:
            del hdf5_data_grp[data_record.name]

        record_grp_to_save = hdf5_data_grp.create_group(data_record.name)
        record_grp_to_save.attrs['name'] = data_record.name

        record_columns_to_save = record_grp_to_save.create_group('columns')
        for col_info in data_record.columns:
            col_df = data_record.column(col_info['name'])
            dst = record_columns_to_save.create_dataset(
                col_info['name'],
                data=col_df.values
                )
            for attr in ['name', 'label', 'unit', 'dependent']:
                dst.attrs[attr] = col_info[attr]

        record_charts_to_save = record_grp_to_save.create_group('charts')
        for chart_title in data_record.charts:
            chart = data_record.chart(chart_title)
            dst = record_charts_to_save.create_dataset(
                chart.title,
                data=chart.figure,
            )

    @staticmethod
    def load_data_group_from_hdf5_group(hdf5_data_grp: h5py.Group):
        """
        Load DataGroup object in hdf5 backend.

        The hierarchy of the data saved in hdf5 is:
        - ``/data_group/data_record/table/column`` or
        - ``/data_group/data_record/charts/chart``
        """
        # load metadata
        meta = hdf5_data_grp.attrs
        group_name = meta.pop('name', 'group')
        group_id = uuid.UUID(meta.pop('id', uuid.uuid4()))
        timestamp = meta.pop('timestamp', datetime.now())
        backend = meta.pop('backend', '{}')
        target_data_group = DataGroup(group_name, group_id, meta)
        target_data_group.timestamp = timestamp
        target_data_group.backend = json.loads(backend)

        # load data record
        for name in hdf5_data_grp.keys():
            hdf5_data_record = hdf5_data_grp[name]

            # load data record table
            hdf5_data_record_columns = hdf5_data_record['columns']
            col_names = list(hdf5_data_record_columns.keys())
            col_metadata = [
                {
                    attr: hdf5_data_record_columns[col_n].attrs[attr]
                    for attr in ['name', 'label', 'unit', 'dependent']
                } for col_n in col_names
            ]
            data_record = DataRecord(name=name, columns=col_metadata)
            table = pd.DataFrame({
                col_name: hdf5_data_record_columns[col_name][:, 0]
                for col_name in col_names
            })
            data_record.add_rows(table)

            # load data record charts
            hdf5_data_record_charts = hdf5_data_record['charts']
            for hdf5_data_chart_title in hdf5_data_record_charts.keys():
                hdf5_chart_dst = hdf5_data_record_charts[hdf5_data_chart_title]
                chart = DataChart(title=hdf5_data_chart_title)
                chart.figure = hdf5_chart_dst[...]
                data_record.add_chart(chart)

            target_data_group.add_record(data_record)

        return target_data_group

if __name__ == '__main__':
    backend = HDF5DataBackend()
    print('type: ', backend.backend_type)
    print('status: ', backend.status)
    print('connected: ', backend.connected)
    backend.connect()
    print('status: ', backend.status)
    print('connected: ', backend.connected)
    dg1 = DataGroup('example')
    print('data group id: ', dg1.id)

    columns = [
        {'name': 'x',
         'label': 'x',
         'dependent': False},
        {'name': 'y',
         'label': 'y',
         'dependent': True}
    ]
    data = np.array([[1, 1],
                     [2, 4],
                     [3, 9],
                     [4, 16]])
    dr1 = DataRecord('dr1', columns, data)
    dc = DataChart('test')
    dc.figure = np.random.randint(155, size=(10, 10))
    dr1.add_chart(dc)
    dg1.add_record(dr1)
    backend.save_group(dg1)
    print(backend.list_groups())

    dg1_ = backend.load_group(str(dg1.id))
    if dg1_ is None:
        print(backend.errors)
    print(f'Backend info: {dg1_.backend}')
    dr1_ = dg1_.record('dr1')
    print(dr1_.snapshot())
    print(dg1_.record('dr1').charts)
    print(dg1_.record('dr1').chart('test').figure)
