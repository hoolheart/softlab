"""
Basic definitions for data backend
"""
from typing import (
    Optional,
    Sequence,
)
from abc import abstractmethod
from softlab.shui.data.base import DataGroup
from softlab.shui.backend import DatabaseBackend

class DataBackend(DatabaseBackend):
    """
    Abstract definition of data backend, derived from ``DatabaseBackend``

    Additional actions for user:
    - list_groups -- list ID sequence of all groups in backend
    - load_group -- load a group with given ID
    - save_group -- save a group to the backend

    Methods need be implemented by derived class:
    - connect_impl -- perform actual connection
    - disconnect_impl -- perform actual disconnection
    - list_impl -- list ID sequence of all groups actually
    - load_impl -- load a group with given ID actually
    - save_impl -- save a group to the backend actually
    """

    def __init__(self, type: str) -> None:
        super().__init__(type)

    def list_groups(self) -> Sequence[str]:
        """List all IDs of groups in the backend"""
        if self.connected:
            return self.list_impl()
        else:
            raise ConnectionError('The backend has not connected')

    def load_group(self, id: str) -> Optional[DataGroup]:
        """Load a group with the given ID"""
        if self.connected:
            return self.load_impl(id)
        else:
            raise ConnectionError('The backend has not connected')

    def save_group(self, group: DataGroup) -> bool:
        """Save a group into the backend"""
        if self.connected:
            return self.save_impl(group)
        else:
            raise ConnectionError('The backend has not connected')

    @abstractmethod
    def list_impl(self) -> Sequence[str]:
        """Actual listing of group IDs, implemented in derived class"""
        raise NotImplementedError(
            'This method must be implemented in derived class')

    @abstractmethod
    def load_impl(self, id: str) -> Optional[DataGroup]:
        """Actual loading of a group, implemented in derived class"""
        raise NotImplementedError(
            'This method must be implemented in derived class')

    @abstractmethod
    def save_impl(self, group: DataGroup) -> bool:
        """Actual saving of a group, implemented in derived class"""
        raise NotImplementedError(
            'This method must be implemented in derived class')
