
<a name="v0.3.0"></a>
## [v0.3.0](https://github.com/FunctionStream/function-stream/compare/v0.2.0...v0.3.0) (2024-03-13)

### Features

* state store ([#153](https://github.com/FunctionStream/function-stream/issues/153))
* add http source tube ([#149](https://github.com/FunctionStream/function-stream/issues/149))
* add sink, source and runtime config to function config ([#136](https://github.com/FunctionStream/function-stream/issues/136))
* add grpc runtime ([#135](https://github.com/FunctionStream/function-stream/issues/135))

### Tests

* add tests for chan utils ([#140](https://github.com/FunctionStream/function-stream/issues/140))
* fix flaky test for grpc_func ([#142](https://github.com/FunctionStream/function-stream/issues/142))

### Bug Fixes

* fix deadlock issue in grpc_func and add cors support ([#158](https://github.com/FunctionStream/function-stream/issues/158))
* cli doesn't respect replica when creating function ([#145](https://github.com/FunctionStream/function-stream/issues/145))
* fix race condition issues in function manager ([#141](https://github.com/FunctionStream/function-stream/issues/141))
* fix context value setting for the function instance ([#139](https://github.com/FunctionStream/function-stream/issues/139))

### Code Refactoring

* improve grpc function protocol ([#147](https://github.com/FunctionStream/function-stream/issues/147))
* improve logging ([#146](https://github.com/FunctionStream/function-stream/issues/146))

### Miscellaneous

* rename module ([#137](https://github.com/FunctionStream/function-stream/issues/137))


<a name="v0.2.0"></a>
## [v0.2.0](https://github.com/FunctionStream/function-stream/compare/v0.1.0...v0.2.0) (2024-02-17)

### Features

* add directory structure to readme and improve structure ([#132](https://github.com/FunctionStream/function-stream/issues/132))
* support basic function operations using CLI tool ([#128](https://github.com/FunctionStream/function-stream/issues/128))
* support pluggable queue ([#125](https://github.com/FunctionStream/function-stream/issues/125))
* support delete function ([#3](https://github.com/FunctionStream/function-stream/issues/3))
* add integration test and CI ([#1](https://github.com/FunctionStream/function-stream/issues/1))
* support loading wasm file

### License

* update license header ([#130](https://github.com/FunctionStream/function-stream/issues/130))

### Build

* add license checker ([#7](https://github.com/FunctionStream/function-stream/issues/7))

### Bug Fixes

* fix mem queue bench doesn't show result ([#129](https://github.com/FunctionStream/function-stream/issues/129))

### Performance Improvements

* improve performance ([#124](https://github.com/FunctionStream/function-stream/issues/124))
* add bench perf ([#6](https://github.com/FunctionStream/function-stream/issues/6))

### Code Refactoring

* use tube term instead of queue ([#134](https://github.com/FunctionStream/function-stream/issues/134))
* abstract contube-go impl ([#131](https://github.com/FunctionStream/function-stream/issues/131))

### Documentation

* fix readme format ([#133](https://github.com/FunctionStream/function-stream/issues/133))


<a name="v0.1.0"></a>
## v0.1.0 (2021-06-28)

