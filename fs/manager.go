package fs

import (
	"context"
	"fmt"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/model"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
	"regexp"
	"sync"
)

type ManagerImpl struct {
	runtimeMap  map[string]api.RuntimeAdapter
	mu          sync.Mutex
	instanceMap map[string]api.Instance
	pkgLoader   api.PackageStorage
	es          api.EventStorage
	stateStore  api.StateStore
	validate    *validator.Validate
	log         *zap.Logger
}

func (m *ManagerImpl) Create(ctx context.Context, r *model.Function) error {
	if err := m.Deploy(ctx, r); err != nil {
		return err
	}
	m.log.Info("created function", zap.String("name", r.Name))
	return nil
}

func (m *ManagerImpl) Read(ctx context.Context, name string) (*model.Function, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ins, ok := m.instanceMap[name]
	if !ok {
		return nil, api.ErrResourceNotFound
	}
	return ins.Function(), nil
}

func (m *ManagerImpl) Upsert(ctx context.Context, r *model.Function) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.instanceMap[r.Name]
	if ok {
		if err := m.Delete(ctx, r.Name); err != nil {
			return fmt.Errorf("failed to restart function %s: %w", r.Name, err)
		}
	}
	m.log.Info("updated function", zap.String("name", r.Name))
	return m.Deploy(ctx, r)
}

func (m *ManagerImpl) List(ctx context.Context) ([]*model.Function, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var functions []*model.Function
	for _, ins := range m.instanceMap {
		functions = append(functions, ins.Function())
	}
	return functions, nil
}

type ManagerConfig struct {
	RuntimeMap    map[string]api.RuntimeAdapter
	PackageLoader api.PackageStorage
	EventStorage  api.EventStorage
	StateStore    api.StateStore
}

func NewManager(cfg ManagerConfig, log *zap.Logger) (api.Manager, error) {
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
		log:         log,
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
	return i.es
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
	m.mu.Lock()
	_, ok := m.instanceMap[f.Name]
	if ok {
		m.mu.Unlock()
		return api.ErrResourceAlreadyExists
	}

	err := m.validateFunctionModel(f)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	p, err := m.pkgLoader.Read(ctx, f.Package)
	if err != nil {
		m.mu.Unlock()
		return err
	}

	err = validateFunctionPackage(f, p)
	if err != nil {
		m.mu.Unlock()
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
	m.mu.Unlock()

	return runtime.DeployFunction(ctx, ins)
}

func (m *ManagerImpl) Delete(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ins, ok := m.instanceMap[name]
	if !ok {
		return api.ErrResourceNotFound
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
