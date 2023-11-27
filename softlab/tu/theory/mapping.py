"""Interface of any mapping"""

from typing import (
    Any,
    Tuple,
    Callable,
    Dict,
)
import numpy as np
from softlab.jin.validator import (
    ValSequence,
    ValInt,
)

_shape_validator = ValSequence(ValInt(min=1))

class Mapping:
    """
    Mapping is general interface for any function from ndarray to ndarray

    Any mapping is a callable object performing given function.
    The given function should take only one ndarray as input, and output
    only one ndarray.
    The input shape and output shape are given at initialization and
    protected during calling.
    Optional metadata can be also given at initialization.
    """
    def __init__(self,
                 in_shape: Tuple[int],
                 out_shape: Tuple[int],
                 func: Callable[[np.ndarray], np.ndarray],
                 metadata: Dict[str, Any] = {}) -> None:
        _shape_validator.validate(in_shape, 'Mapping in shape')
        _shape_validator.validate(out_shape, 'Mapping out shape')
        if not isinstance(func, Callable):
            raise TypeError(f'Invalid function type {type(func)}')
        self._in = tuple(in_shape)
        self._out = tuple(out_shape)
        self._func = func
        self._meta: Dict[str, Any] = {}
        if isinstance(metadata, Dict):
            for key in metadata:
                if isinstance(key, str) and len(key) > 0:
                    self._meta[key] = metadata[key]

    @property
    def in_shape(self) -> Tuple[int]:
        """Shape of input ndarray"""
        return self._in

    @property
    def out_shape(self) -> Tuple[int]:
        """Shape of output ndarray"""
        return self._out

    @property
    def metadata(self) -> Dict[str, Any]:
        """Optional metadata"""
        return self._meta

    def __call__(self, *args: np.ndarray) -> np.ndarray:
        if len(args) != 1:
            raise RuntimeError('Mapping takes only one input')
        if not isinstance(args[0], np.ndarray):
            raise TypeError('Input to mapping must be numpy ndarray')
        if args[0].shape != self._in:
            raise ValueError(
                f'Shape of input {args[0].shape} dismatches {self._in}')
        rst = self._func(args[0])
        if not isinstance(rst, np.ndarray) or rst.shape != self._out:
            raise RuntimeError('Result type or shape is invalid')
        return rst

if __name__ == '__main__':
    def _test_mapping(x: np.ndarray) -> np.ndarray:
        return x**2

    m = Mapping((2,1), (2,1), _test_mapping,
                {'func': _test_mapping.__name__, 1: 6})
    print(f'Input shape {m.in_shape}')
    print(f'Output shape {m.out_shape}')
    print(f'Metadata {m.metadata}')
    for d in [
        np.ones((2, 1)),
        np.random.uniform(0.0, 1.0, (2, 1)),
        np.random.normal(0.0, 1.0, (1, 2)),
    ]:
        print(f'Data shape {d.shape}')
        try:
            rst = m(d)
            print('Input')
            print(d)
            print('Output')
            print(rst)
        except:
            print('Failed')
