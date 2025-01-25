package api

import "fmt"

var ErrFunctionAlreadyExists = fmt.Errorf("function already exists")
var ErrFunctionNotFound = fmt.Errorf("function not found")
var ErrStateNotFound = fmt.Errorf("state not found")

var ErrPackageAlreadyExists = fmt.Errorf("package already exists")
var ErrPackageNotFound = fmt.Errorf("package not found")
