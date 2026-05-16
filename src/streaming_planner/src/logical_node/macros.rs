// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[macro_export]
macro_rules! multifield_partial_ord {
    ($ty:ty, $($field:tt), *) => {
        impl PartialOrd for $ty {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                $(
                    let cmp = self.$field.partial_cmp(&other.$field)?;
                    if cmp != std::cmp::Ordering::Equal {
                        return Some(cmp);
                    }
                )*
                Some(std::cmp::Ordering::Equal)
            }
        }
    };
}
