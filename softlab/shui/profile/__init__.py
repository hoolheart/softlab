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

from softlab.shui.profile.backend import (
    ProfileBackend,
    JsonProfileBackend,
    MemoryProfileBackend,
    get_profile_backend,
    get_profile_backend_by_info,
)
