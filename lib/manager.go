package lib

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/common/model"
	"sync"
)

type FunctionManager struct {
	functions     map[string]*FunctionInstance
	functionsLock sync.Mutex
	pc            pulsar.Client
}

func NewFunctionManager() (*FunctionManager, error) {
	pc, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: common.GetConfig().PulsarURL,
	})
	if err != nil {
		return nil, err
	}
	return &FunctionManager{
		functions: make(map[string]*FunctionInstance),
		pc:        pc,
	}, nil
}

func (fm *FunctionManager) StartFunction(f model.Function) {
	fm.functionsLock.Lock()
	instance := NewFunctionInstance(f, fm.pc)
	fm.functions[f.Name] = instance
	go instance.Run()
	instance.WaitForReady()
	fm.functionsLock.Unlock()
}
