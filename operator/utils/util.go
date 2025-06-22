package utils

import (
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
)

// HandleReconcileError handles errors in reconcile loops, logging conflicts as info and returning nil error for them.
func HandleReconcileError(log logr.Logger, err error, conflictMsg string) (ctrl.Result, error) {
	if errors.IsConflict(err) {
		log.V(1).Info(conflictMsg, "error", err)
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, err
}
