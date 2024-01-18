"""
Data backend by SQLITE3
"""
from typing import (
    Any,
    Dict,
    Optional,
    Sequence,
)
import numpy as np
import pandas as pd
import uuid
import json
import sqlite3
import io
from softlab.shui.data.base import (
    DataGroup,
    DataRecord,
    DataChart,
)
from softlab.shui.backend import catch_error
from softlab.shui.data.backend import DataBackend

class Sqlite3DataBackend(DataBackend):
    """Data backend using sqlite3"""

    def __init__(self):
        super().__init__('sqlite3')
        self.conn = None
        self.cur = None
        self._path = 'data.db'

    @catch_error(failed_return=False, action='connect')
    def connect_impl(self, args: Dict[str, Any]) -> bool:
        """
        Arguments:
        - args --  sqlite3 backend arguments
            - path -- absolute path to the sqlite database file
        """
        path = args['path'] if 'path' in args else 'data.db'

        def adapt_array(arr):
            """
            Adapt numpy.ndarray to sqlite binary.
            """
            out = io.BytesIO()
            np.save(out, arr)
            out.seek(0)
            return sqlite3.Binary(out.read())

        def convert_array(blob):
            """
            Convert sqlite binary to numpy.ndarray
            """
            out = io.BytesIO(blob)
            out.seek(0)
            return np.load(out)

        def adapt_dataframe(df):
            """
            Adapt pandas.DaraFrame to sqlite binary.
            """
            out = io.BytesIO()
            df.to_csv(out, index=False)
            out.seek(0)
            return sqlite3.Binary(out.read())

        def convert_dataframe(blob):
            """
            Convert sqlite binary to pandas.DataFrame
            """
            out = io.BytesIO(blob)
            out.seek(0)
            return pd.read_csv(out)

        self.conn = sqlite3.connect(
            path, detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None)
        sqlite3.register_adapter(np.ndarray, adapt_array)
        sqlite3.register_converter('array', convert_array)
        sqlite3.register_adapter(pd.DataFrame, adapt_dataframe)
        sqlite3.register_converter('dataframe', convert_dataframe)
        sqlite3.register_adapter(dict, lambda d: json.dumps(d))
        sqlite3.register_converter('dict', lambda d: json.loads(d))
        sqlite3.register_adapter(list, lambda d: json.dumps(d))
        sqlite3.register_converter('list', lambda d: json.loads(d))

        self.cur = self.conn.cursor()

        # Data-definition Language
        self.conn.execute("begin transaction;")
        self.cur.execute(
                'create table if not exists datagroup ('
                    'id text, '
                    'experiment text, '
                    'sample text, '
                    'run_id integer, '
                    'timestamp timestamp, '
                    'backend dict, '
                    'primary key (id))'
                )
        self.cur.execute(
                'create table if not exists datarecord ('
                     'group_id text, '
                     'name text, '
                     'columns list, '
                     'df dataframe, '
                     'primary key (group_id, name), '
                     'foreign key (group_id) references datagroup(id))'
                )
        self.cur.execute(
                'create table if not exists datachart ('
                     'group_id text, '
                     'record_name text, '
                     'title text, '
                     'figure array, '
                     'primary key (group_id, record_name, title), '
                     'foreign key (group_id, record_name) '
                     'references datarecord(group_id, name))'
                )
        self.conn.execute("commit;")

        print(f'Connected to sqlite3 backend, data is stored in {path}.')
        self._path = path
        return True

    @catch_error(failed_return=False, action='disconnect')
    def disconnect_impl(self) -> bool:
        self.cur.close()
        self.conn.close()
        return True

    @catch_error(failed_return=[], action='list groups')
    def list_impl(self) -> Sequence[str]:
        res = self.conn.execute(
                    'select id '
                    'from datagroup '
                    'order by timestamp'
                    ).fetchall()
        return res

    @catch_error(failed_return=None, action='load group')
    def load_impl(self, id: str) -> Optional[DataGroup]:
        self.cur.execute(
            'select * '
            'from datagroup '
            'where id=:id',
            {'id': id}
            )
        metadata = self.cur.fetchone()
        if metadata is None:
            print(f'Group {id} does not exist.')
            return

        target_data_group = DataGroup(
            metadata[1], uuid.UUID(metadata[0]),
            {'sample': metadata[2], 'run_id': metadata[3]},
        )
        target_data_group.timestamp = metadata[4]
        target_data_group.backend = {
            'type': self.backend_type,
            'arguments': {'path': self._path}
        }

        # recover data records in the data group
        self.cur.execute(
            'select name, columns, df '
            'from datarecord '
            'where group_id=:id',
            {'id': id},
        )
        records_data = self.cur.fetchall()
        for record_name, columns, df in records_data:
            data_record = DataRecord(name=record_name, columns=columns)
            data_record.add_rows(df)
            charts_data = self.cur.execute(
                'select title, figure '
                'from datachart '
                'where group_id=:group_id and '
                'record_name=:record_name',
                {'group_id': id, 'record_name': record_name}
            ).fetchall()
            for title, figure in charts_data:
                chart = DataChart(title=title)
                chart.figure = figure
                data_record.add_chart(chart)
            target_data_group.add_record(data_record)
        self.mark_success()
        return target_data_group

    @catch_error(failed_return=False, action='save group')
    def save_impl(self, group: DataGroup) -> bool:
        """
        Save DataGroup object in hdf5 backend.
        """
        self.conn.execute("begin transaction;")
        self.cur.execute(
            'delete from datagroup '
            'where id=:id',
            {'id': str(group.id)}
        )
        self.cur.execute(
            'insert into datagroup values(?, ?, ?, ?, ?, ?)',
            (str(group.id), group.name, group.meta.get('sample', 'demo'),
             group.meta.get('run_id', 0), group.timestamp, group.backend)
        )

        self.cur.execute(
            'delete from datarecord '
            'where group_id=:id',
            {'id': str(group.id)}
        )

        record_names = group.records
        for record_name in record_names:
            record = group.record(record_name)
            self.cur.execute(
                'insert into datarecord values(?, ?, ?, ?)',
                (str(group.id), record_name, record.columns, record.table)
            )

            self.cur.execute(
                'delete from datachart '
                'where group_id=:id and record_name=:name',
                {'id': str(group.id), 'name': record_name}
            )
            for chart_title in record.charts:
                chart = record.chart(chart_title)
                self.cur.execute(
                    'insert into datachart values(?, ?, ?, ?)',
                    (str(group.id), record_name, chart_title, chart.figure)
                )

        self.conn.execute("commit;")
        self.mark_success()
        return True
