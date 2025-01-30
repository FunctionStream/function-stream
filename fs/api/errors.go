package api

import "fmt"

var ErrStateNotFound = fmt.Errorf("state not found")

var ErrResourceAlreadyExists = fmt.Errorf("resource already exists")
var ErrResourceNotFound = fmt.Errorf("resource not found")
