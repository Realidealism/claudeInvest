"""The project's single timezone.

Taiwan is UTC+8 year round with no daylight saving, so the offset itself is
never going to change -- the reason to share one definition is that a
timestamp written without it is indistinguishable from one written with it
until the machine's clock is somewhere else.

Sixteen modules currently define this constant privately. New code should
import from here.
"""

from datetime import datetime, timedelta, timezone

TPE_TZ = timezone(timedelta(hours=8), name="Asia/Taipei")


def now() -> datetime:
    """Timezone-aware current time in Taipei."""
    return datetime.now(TPE_TZ)
