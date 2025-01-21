package fs

import (
	"context"
	"fmt"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/model"
	"github.com/go-playground/validator/v10"
	"regexp"
)

type ManagerImpl struct {
	runtimeMap  map[string]api.RuntimeAdapter
	instanceMap map[string]api.Instance
	pkgLoader   api.PackageLoader
	es          api.EventStorage
	stateStore  api.StateStore
	validate    *validator.Validate
}

type ManagerConfig struct {
	RuntimeMap    map[string]api.RuntimeAdapter
	PackageLoader api.PackageLoader
	EventStorage  api.EventStorage
	StateStore    api.StateStore
}

func NewManager(cfg ManagerConfig) (api.Manager, error) {
	validate := validator.New()
	err := validate.RegisterValidation("alphanumdash", func(fl validator.FieldLevel) bool {
		value := fl.Field().String()
		// Allow alphanumeric, dash, dot, asterisk, and slash
		matched, _ := regexp.MatchString(`^[a-zA-Z0-9\-.*/]+$`, value)
		return matched
	})
	if err != nil {
		return nil, err
	}
	return &ManagerImpl{
		runtimeMap:  cfg.RuntimeMap,
		pkgLoader:   cfg.PackageLoader,
		es:          cfg.EventStorage,
		stateStore:  cfg.StateStore,
		instanceMap: make(map[string]api.Instance),
		validate:    validate,
	}, nil
}

func (m *ManagerImpl) validateFunctionModel(f *model.Function) error {
	if err := m.validate.Struct(f); err != nil {
		var errMessages []string
		for _, err := range err.(validator.ValidationErrors) {
			errMessages = append(errMessages, fmt.Sprintf("%s: %s", err.Field(), err.Tag()))
		}
		return fmt.Errorf("validation errors: %s", errMessages)
	}
	return nil
}

func validateFunctionPackage(f *model.Function, p *model.Package) error {
	if p.Name != f.Package {
		return fmt.Errorf("package name %s does not match function package name %s", p.Name, f.Package)
	}
	if _, ok := p.Modules[f.Module]; !ok {
		return fmt.Errorf("module %s not found in package %s", f.Module, f.Package)
	}
	return nil
}

type instance struct {
	ctx context.Context
	f   *model.Function
	p   *model.Package
	es  api.EventStorage
	ss  api.StateStore
}

func (i *instance) EventStorage() api.EventStorage {
	//TODO implement me
	panic("implement me")
}

func (i *instance) Context() context.Context {
	return i.ctx
}

func (i *instance) Function() *model.Function {
	return i.f
}

func (i *instance) Package() *model.Package {
	return i.p
}

func (i *instance) StateStore() api.StateStore {
	return i.ss
}

func (m *ManagerImpl) Deploy(ctx context.Context, f *model.Function) error {
	err := m.validateFunctionModel(f)
	if err != nil {
		return err
	}
	p, err := m.pkgLoader.LoadPackage(ctx, f.Package)
	if err != nil {
		return err
	}

	err = validateFunctionPackage(f, p)
	if err != nil {
		return err
	}

	runtime, ok := m.runtimeMap[p.Type]
	if !ok {
		return fmt.Errorf("runtime %s not found", p.Type)
	}

	ins := &instance{
		ctx: ctx,
		f:   f,
		p:   p,
		es:  m.es,
		ss:  m.stateStore,
	}
	m.instanceMap[f.Name] = ins

	return runtime.DeployFunction(ctx, ins)
}

func (m *ManagerImpl) Delete(ctx context.Context, name string) error {
	ins, ok := m.instanceMap[name]
	if !ok {
		return fmt.Errorf("function %s not found", name)
	}

	runtime, ok := m.runtimeMap[ins.Package().Type]
	if !ok {
		return fmt.Errorf("runtime %s not found", ins.Function().Package)
	}

	if err := runtime.DeleteFunction(ctx, name); err != nil {
		return err
	}

	delete(m.instanceMap, name)
	return nil
}
