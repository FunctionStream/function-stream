#!/bin/bash
# Copyright 2024 Function Stream Org.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -xe

cd "$(git rev-parse --show-toplevel)"

LICENSE_CHECKER="bin/license-header-checker"

if [ ! -f "$LICENSE_CHECKER" ]; then
  echo "license-checker not found, building it..."
  export BINDIR=bin && curl -s https://raw.githubusercontent.com/lluissm/license-header-checker/master/install.sh | bash
fi

$LICENSE_CHECKER -a -r -i bin,admin/client,common/run.go,common/signal.go,fs/runtime/external/model,clients ./license-checker/license-header.txt . go
$LICENSE_CHECKER -a -r ./license-checker/license-header.txt . proto
$LICENSE_CHECKER -a -r -i bin,admin/client,.chglog ./license-checker/license-header-sh.txt . sh yaml yml
$LICENSE_CHECKER -a -r -i bin,admin/client,.chglog,CHANGELOG.md ./license-checker/license-header-md.txt . md

if [[ -z $(git status -s) ]]; then
  echo "No license header issues found"
else
  echo "$(git status)"
  echo "License header issues found"
  exit 1
fi
