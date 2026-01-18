# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from wit_world.imports.collector import emit as wit_emit, emit_watermark as wit_emit_watermark
else:
    try:
        from wit_world.imports.collector import emit as wit_emit, emit_watermark as wit_emit_watermark
    except (ImportError, AttributeError):
        wit_emit = None
        wit_emit_watermark = None


def emit(data: bytes, channel: int = 0) -> None:
    if wit_emit is None:
        raise RuntimeError("WIT Collector binding is not available")
    
    try:
        wit_emit(channel, data)
    except Exception:
        pass


def emit_watermark(watermark: int, channel: int = 0) -> None:
    if wit_emit_watermark is None:
        raise RuntimeError("WIT Collector binding is not available")
    
    try:
        wit_emit_watermark(channel, watermark)
    except Exception:
        pass


__all__ = ['emit', 'emit_watermark']

