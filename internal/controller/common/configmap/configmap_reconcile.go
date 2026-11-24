package configmap

import (
	"context"

	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/reconciler"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/util/maps"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Reconcile creates the expected ConfigMap, owned by owner, or brings the
// existing one's labels, annotations and data in line with it.
func Reconcile(
	ctx context.Context,
	cli client.Client,
	expected corev1.ConfigMap,
	owner client.Object,
) (corev1.ConfigMap, error) {
	reconciled, err := reconciler.Reconcile(ctx, reconciler.Params{
		Client:   cli,
		Owner:    owner,
		Expected: &expected,
		NeedUpdate: func(existed client.Object) bool {
			return needUpdate(&expected, existed.(*corev1.ConfigMap))
		},
		Update: func(existed client.Object) {
			update(&expected, existed.(*corev1.ConfigMap))
		},
	})
	if err != nil || reconciled == nil {
		return expected, err
	}
	return *reconciled.(*corev1.ConfigMap), nil
}

func needUpdate(expected, existed *corev1.ConfigMap) bool {
	return !maps.IsSubset(expected.Labels, existed.Labels) ||
		!maps.IsSubset(expected.Annotations, existed.Annotations) ||
		!equality.Semantic.DeepEqual(expected.Data, existed.Data)
}

func update(expected, existed *corev1.ConfigMap) {
	existed.Labels = maps.Merge(existed.Labels, expected.Labels)
	existed.Annotations = maps.Merge(existed.Annotations, expected.Annotations)
	existed.Data = expected.Data
}
