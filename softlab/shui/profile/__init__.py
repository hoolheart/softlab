"""
``profile`` module is used to manage working profiles, including
profile data structure and management.
"""

from softlab.shui.profile.base import (
    ProfileItem,
    Profile,
)

from softlab.shui.profile.manage import (
    ProfileInfo,
    ProfileManage,
)

from softlab.shui.profile.backend import ProfileBackend
from softlab.shui.profile.json_profile import JsonProfileBackend
from softlab.shui.profile.memory import MemoryProfileBackend
from softlab.shui.profile.io import (
    get_profile_backend,
    get_profile_backend_by_info,
)
