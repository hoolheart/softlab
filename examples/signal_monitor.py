"""A synthetic paired-signal count client using the bounded execution seam.

The source is intentionally consumptive: each channel is read once per cycle.
This example owns its configuration and its derived difference policy.
"""
from softlab.huo.process import count, run_count
from softlab.shui.data import DataGroup, DataRecord
from softlab.tu.station import Parameter


def acquire(points=3):
    """Return terminal evidence, record, differences, and channel read trace."""
    state = {'tick': 0, 'reads': []}
    group = DataGroup('signal_monitor')
    record = DataRecord('signal', [
        {'name': 'timestamp', 'unit': 's', 'dependent': False},
        {'name': 'signal', 'unit': 'V', 'dependent': True},
        {'name': 'reference', 'unit': 'V', 'dependent': True},
    ])
    group.add_record(record)

    def advance():
        state['tick'] += 1

    def read(channel, offset):
        state['reads'].append((channel, state['tick']))
        return state['tick'] + offset

    getters = [
        Parameter('signal', settable=False,
                  before_get=lambda _: read('signal', 5)),
        Parameter('reference', settable=False,
                  before_get=lambda _: read('reference', 2)),
    ]
    process = count('signal', group=group, record=record, getters=getters,
                    times=points, hook_before_get=advance)
    result = run_count(process, points, lambda: len(record),
                       lambda: state['tick'])
    differences = (record.table['signal'] - record.table['reference']).tolist()
    return result, record, differences, list(state['reads'])
