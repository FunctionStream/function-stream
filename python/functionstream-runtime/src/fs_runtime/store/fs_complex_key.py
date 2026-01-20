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
    from fs_api.store.complexkey import ComplexKey as ApiComplexKey
    from wit_world.imports.kv import ComplexKey as WitComplexKey
else:
    from fs_api.store.complexkey import ComplexKey as ApiComplexKey
    try:
        from wit_world.imports.kv import ComplexKey as WitComplexKey
    except (ImportError, AttributeError):
        WitComplexKey = None


def api_to_wit(api_key: 'ApiComplexKey') -> 'WitComplexKey':
    if WitComplexKey is not None:
        return WitComplexKey(
            key_group=api_key.key_group,
            key=api_key.key,
            namespace=api_key.namespace,
            user_key=api_key.user_key,
        )
    else:
        return {
            'key_group': api_key.key_group,
            'key': api_key.key,
            'namespace': api_key.namespace,
            'user_key': api_key.user_key,
        }


def wit_to_api(wit_key: 'WitComplexKey') -> 'ApiComplexKey':
    from fs_api.store.complexkey import ComplexKey
    
    if isinstance(wit_key, dict):
        return ComplexKey(
            key_group=wit_key.get('key_group', b''),
            key=wit_key.get('key', b''),
            namespace=wit_key.get('namespace', b''),
            user_key=wit_key.get('user_key', b''),
        )
    else:
        return ComplexKey(
            key_group=wit_key.key_group,
            key=wit_key.key,
            namespace=wit_key.namespace,
            user_key=wit_key.user_key,
        )


__all__ = ['api_to_wit', 'wit_to_api']

