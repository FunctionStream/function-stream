
<a name="v0.5.0"></a>
## [v0.5.0](https://github.com/FunctionStream/function-stream/compare/v0.4.0...v0.5.0) (2024-06-13)

### Features

* add data schema support ([#182](https://github.com/FunctionStream/function-stream/issues/182)) - _by Zike Yang_
* improve log ([#181](https://github.com/FunctionStream/function-stream/issues/181)) - _by Zike Yang_
* improve tube configuration ([#180](https://github.com/FunctionStream/function-stream/issues/180)) - _by Zike Yang_
* support tls ([#179](https://github.com/FunctionStream/function-stream/issues/179)) - _by Zike Yang_
* function store ([#178](https://github.com/FunctionStream/function-stream/issues/178)) - _by Zike Yang_

### Documentation

* add changelog for v0.4.0 ([#177](https://github.com/FunctionStream/function-stream/issues/177)) - _by Zike Yang_


<a name="v0.4.0"></a>
## [v0.4.0](https://github.com/FunctionStream/function-stream/compare/v0.3.0...v0.4.0) (2024-05-09)

### Features

* add contube config validation ([#174](https://github.com/FunctionStream/function-stream/issues/174)) - _by Zike Yang_
* support pluggable state store ([#173](https://github.com/FunctionStream/function-stream/issues/173)) - _by Zike Yang_
* improve function configuration ([#170](https://github.com/FunctionStream/function-stream/issues/170)) - _by Zike Yang_
* improve configuration ([#169](https://github.com/FunctionStream/function-stream/issues/169)) - _by Zike Yang_
* refactor fs http service ([#168](https://github.com/FunctionStream/function-stream/issues/168)) - _by Zike Yang_

### Build

* add change log ([#160](https://github.com/FunctionStream/function-stream/issues/160)) - _by Zike Yang_
* **deps:** bump google.golang.org/protobuf from 1.32.0 to 1.33.0 ([#162](https://github.com/FunctionStream/function-stream/issues/162)) - _by dependabot[bot]_

### Bug Fixes

* prevent panic by closing channel in NewSourceTube goroutine ([#156](https://github.com/FunctionStream/function-stream/issues/156)) - _by wy-os_
* **tube:** move the getOrCreatChan outside of the goroutine ([#161](https://github.com/FunctionStream/function-stream/issues/161)) - _by wy-os_

### Tests

* fix duplicated close on the server ([#163](https://github.com/FunctionStream/function-stream/issues/163)) - _by Zike Yang_


<a name="v0.3.0"></a>
## [v0.3.0](https://github.com/FunctionStream/function-stream/compare/v0.2.0...v0.3.0) (2024-03-13)

### Features

* state store ([#153](https://github.com/FunctionStream/function-stream/issues/153)) - _by Zike Yang_
* add http source tube ([#149](https://github.com/FunctionStream/function-stream/issues/149)) - _by Zike Yang_
* add sink, source and runtime config to function config ([#136](https://github.com/FunctionStream/function-stream/issues/136)) - _by Zike Yang_
* add grpc runtime ([#135](https://github.com/FunctionStream/function-stream/issues/135)) - _by Zike Yang_

### Tests

* add tests for chan utils ([#140](https://github.com/FunctionStream/function-stream/issues/140)) - _by wy-os_
* fix flaky test for grpc_func ([#142](https://github.com/FunctionStream/function-stream/issues/142)) - _by Zike Yang_

### Bug Fixes

* fix deadlock issue in grpc_func and add cors support ([#158](https://github.com/FunctionStream/function-stream/issues/158)) - _by Zike Yang_
* cli doesn't respect replica when creating function ([#145](https://github.com/FunctionStream/function-stream/issues/145)) - _by Zike Yang_
* fix race condition issues in function manager ([#141](https://github.com/FunctionStream/function-stream/issues/141)) - _by Zike Yang_
* fix context value setting for the function instance ([#139](https://github.com/FunctionStream/function-stream/issues/139)) - _by wy-os_

### Code Refactoring

* improve grpc function protocol ([#147](https://github.com/FunctionStream/function-stream/issues/147)) - _by Zike Yang_
* improve logging ([#146](https://github.com/FunctionStream/function-stream/issues/146)) - _by Zike Yang_

### Miscellaneous

* rename module ([#137](https://github.com/FunctionStream/function-stream/issues/137)) - _by Zike Yang_


<a name="v0.2.0"></a>
## [v0.2.0](https://github.com/FunctionStream/function-stream/compare/v0.1.0...v0.2.0) (2024-02-17)

### Features

* add directory structure to readme and improve structure ([#132](https://github.com/FunctionStream/function-stream/issues/132)) - _by Zike Yang_
* support basic function operations using CLI tool ([#128](https://github.com/FunctionStream/function-stream/issues/128)) - _by Zike Yang_
* support pluggable queue ([#125](https://github.com/FunctionStream/function-stream/issues/125)) - _by Zike Yang_
* support delete function ([#3](https://github.com/FunctionStream/function-stream/issues/3)) - _by Zike Yang_
* add integration test and CI ([#1](https://github.com/FunctionStream/function-stream/issues/1)) - _by Zike Yang_
* support loading wasm file - _by Zike Yang_

### License

* update license header ([#130](https://github.com/FunctionStream/function-stream/issues/130)) - _by Zike Yang_

### Build

* add license checker ([#7](https://github.com/FunctionStream/function-stream/issues/7)) - _by Zike Yang_

### Bug Fixes

* fix mem queue bench doesn't show result ([#129](https://github.com/FunctionStream/function-stream/issues/129)) - _by Zike Yang_

### Performance Improvements

* improve performance ([#124](https://github.com/FunctionStream/function-stream/issues/124)) - _by Zike Yang_
* add bench perf ([#6](https://github.com/FunctionStream/function-stream/issues/6)) - _by Zike Yang_

### Code Refactoring

* use tube term instead of queue ([#134](https://github.com/FunctionStream/function-stream/issues/134)) - _by Zike Yang_
* abstract contube-go impl ([#131](https://github.com/FunctionStream/function-stream/issues/131)) - _by Zike Yang_

### Documentation

* fix readme format ([#133](https://github.com/FunctionStream/function-stream/issues/133)) - _by Zike Yang_


<a name="v0.1.0"></a>
## v0.1.0 (2021-06-28)

