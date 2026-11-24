package configmap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNeedUpdate(t *testing.T) {
	expected := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "sentinel"}},
		Data:       map[string]string{"redis-sentinel-additional.conf": "tls-replication yes\n"},
	}

	tests := []struct {
		name    string
		existed *corev1.ConfigMap
		want    bool
	}{
		{
			name: "identical",
			existed: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "sentinel"}},
				Data:       map[string]string{"redis-sentinel-additional.conf": "tls-replication yes\n"},
			},
			want: false,
		},
		{
			name: "extra labels set by someone else are kept",
			existed: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "sentinel", "owner": "team"}},
				Data:       map[string]string{"redis-sentinel-additional.conf": "tls-replication yes\n"},
			},
			want: false,
		},
		{
			name: "edited data is restored",
			existed: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "sentinel"}},
				Data:       map[string]string{"redis-sentinel-additional.conf": "tls-replication no\n"},
			},
			want: true,
		},
		{
			name: "missing label",
			existed: &corev1.ConfigMap{
				Data: map[string]string{"redis-sentinel-additional.conf": "tls-replication yes\n"},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, needUpdate(expected, tt.existed))
		})
	}
}

func TestUpdate(t *testing.T) {
	expected := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "sentinel"}},
		Data:       map[string]string{"redis-sentinel-additional.conf": "tls-replication yes\n"},
	}
	existed := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"owner": "team"}},
		Data:       map[string]string{"redis-sentinel-additional.conf": "tls-replication no\n", "stale": "x"},
	}

	update(expected, existed)

	assert.Equal(t, map[string]string{"app": "sentinel", "owner": "team"}, existed.Labels)
	assert.Equal(t, expected.Data, existed.Data)
	assert.False(t, needUpdate(expected, existed))
}
