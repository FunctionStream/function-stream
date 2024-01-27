package lib

import (
	"github.com/functionstream/functionstream/common/model"
	"sync"
)

type FunctionManager struct {
	functions     map[string]*FunctionInstance
	functionsLock sync.Mutex
}

func NewFunctionManager() *FunctionManager {
	return &FunctionManager{
		functions: make(map[string]*FunctionInstance),
	}
}

func (fm *FunctionManager) StartFunction(f model.Function) {
	fm.functionsLock.Lock()
	instance := NewFunctionInstance(f)
	fm.functions[f.Name] = instance
	go instance.Run()
	fm.functionsLock.Unlock()
}
