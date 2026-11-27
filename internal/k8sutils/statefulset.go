package k8sutils

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/consts"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/envs"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/features"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/util"
	"github.com/banzaicloud/k8s-objectmatcher/patch"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type StatefulSet interface {
	IsStatefulSetReady(ctx context.Context, namespace, name string) bool
	GetStatefulSetReplicas(ctx context.Context, namespace, name string) int32
}

type StatefulSetService struct {
	kubeClient kubernetes.Interface
}

func NewStatefulSetService(kubeClient kubernetes.Interface) *StatefulSetService {
	return &StatefulSetService{
		kubeClient: kubeClient,
	}
}

func (s *StatefulSetService) IsStatefulSetReady(ctx context.Context, namespace, name string) bool {
	var (
		partition = 0
		replicas  = 1
	)

	sts, err := s.kubeClient.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to get statefulset")
		return false
	}

	if sts.Spec.UpdateStrategy.RollingUpdate != nil && sts.Spec.UpdateStrategy.RollingUpdate.Partition != nil {
		partition = int(*sts.Spec.UpdateStrategy.RollingUpdate.Partition)
	}
	if sts.Spec.Replicas != nil {
		replicas = int(*sts.Spec.Replicas)
	}

	if expectedUpdateReplicas := replicas - partition; sts.Status.UpdatedReplicas < int32(expectedUpdateReplicas) {
		log.FromContext(ctx).V(1).Info("StatefulSet is not ready", "Status.UpdatedReplicas", sts.Status.UpdatedReplicas, "ExpectedUpdateReplicas", expectedUpdateReplicas)
		return false
	}
	if partition == 0 && sts.Status.CurrentRevision != sts.Status.UpdateRevision {
		log.FromContext(ctx).V(1).Info("StatefulSet is not ready", "Status.CurrentRevision", sts.Status.CurrentRevision, "Status.UpdateRevision", sts.Status.UpdateRevision)
		return false
	}
	if sts.Status.ObservedGeneration != sts.Generation {
		log.FromContext(ctx).V(1).Info("StatefulSet is not ready", "Status.ObservedGeneration", sts.Status.ObservedGeneration, "Generation", sts.Generation)
		return false
	}
	if int(sts.Status.ReadyReplicas) != replicas {
		log.FromContext(ctx).V(1).Info("StatefulSet is not ready", "Status.ReadyReplicas", sts.Status.ReadyReplicas, "Replicas", replicas)
		return false
	}
	return true
}

func (s *StatefulSetService) GetStatefulSetReplicas(ctx context.Context, namespace, name string) int32 {
	sts, err := s.kubeClient.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return 0
	}
	if sts.Spec.Replicas == nil {
		return 0
	}
	return *sts.Spec.Replicas
}

const (
	redisExporterContainer = "redis-exporter"
)

// statefulSetParameters will define statefulsets input params
type statefulSetParameters struct {
	Replicas                             *int32
	ClusterMode                          bool
	ClusterVersion                       *string
	NodeConfVolume                       bool
	NodeSelector                         map[string]string
	TopologySpreadConstraints            []corev1.TopologySpreadConstraint
	PodSecurityContext                   *corev1.PodSecurityContext
	PriorityClassName                    string
	Affinity                             *corev1.Affinity
	Tolerations                          *[]corev1.Toleration
	EnableMetrics                        bool
	PersistentVolumeClaim                corev1.PersistentVolumeClaim
	NodeConfPersistentVolumeClaim        corev1.PersistentVolumeClaim
	ImagePullSecrets                     *[]corev1.LocalObjectReference
	ExternalConfig                       *string
	ServiceAccountName                   *string
	UpdateStrategy                       appsv1.StatefulSetUpdateStrategy
	PersistentVolumeClaimRetentionPolicy *appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy
	RecreateStatefulSet                  bool
	RecreateStatefulsetStrategy          *metav1.DeletionPropagation
	TerminationGracePeriodSeconds        *int64
	IgnoreAnnotations                    []string
	HostNetwork                          bool
	MinReadySeconds                      int32
	PodManagementPolicy                  *string
}

// containerParameters will define container input params
type containerParameters struct {
	Image                        string
	ImagePullPolicy              corev1.PullPolicy
	Resources                    *corev1.ResourceRequirements
	MaxMemoryPercentOfLimit      *int
	SecurityContext              *corev1.SecurityContext
	RedisExporterImage           string
	RedisExporterImagePullPolicy corev1.PullPolicy
	RedisExporterResources       *corev1.ResourceRequirements
	RedisExporterEnv             *[]corev1.EnvVar
	RedisExporterPort            *int
	RedisExporterSecurityContext *corev1.SecurityContext
	Role                         string
	EnabledPassword              *bool
	SecretName                   *string
	SecretKey                    *string
	PersistenceEnabled           *bool
	TLSConfig                    *commonapi.TLSConfig
	ACLConfig                    *commonapi.ACLConfig
	ReadinessProbe               *corev1.Probe
	LivenessProbe                *corev1.Probe
	AdditionalEnvVariable        *[]corev1.EnvVar
	AdditionalVolume             []corev1.Volume
	AdditionalMountPath          []corev1.VolumeMount
	EnvVars                      *[]corev1.EnvVar
	Port                         *int
	HostPort                     *int
	// Sentinel-driven preStop settings. These are only populated for the
	// "replication" role when an embedded Sentinel is enabled. When
	// SentinelService is empty the replication preStop hook is not installed.
	SentinelService    string
	SentinelMasterName string
	SentinelPort       int
	// SentinelPasswordSecret is the embedded Sentinel's own password
	// (spec.sentinel.redisSecret). The preStop hook authenticates its
	// Sentinel calls with it, not with the Redis password.
	SentinelPasswordSecret *commonapi.ExistingPasswordSecret
	// PreStopWaitSeconds is not Sentinel-specific: it is populated for the
	// "replication" role and for every RedisCluster leader and follower, and
	// bounds the demotion wait in whichever preStop hook is installed.
	PreStopWaitSeconds int
}

type initContainerParameters struct {
	Enabled               *bool
	Image                 string
	ImagePullPolicy       corev1.PullPolicy
	Resources             *corev1.ResourceRequirements
	Role                  string
	Command               []string
	Arguments             []string
	PersistenceEnabled    *bool
	AdditionalEnvVariable *[]corev1.EnvVar
	AdditionalVolume      []corev1.Volume
	AdditionalMountPath   []corev1.VolumeMount
	SecurityContext       *corev1.SecurityContext
}

// CreateOrUpdateStateFul method will create or update Redis service
func CreateOrUpdateStateFul(ctx context.Context, cl kubernetes.Interface, namespace string, stsMeta metav1.ObjectMeta, params statefulSetParameters, ownerDef metav1.OwnerReference, initcontainerParams initContainerParameters, containerParams containerParameters, sidecars *[]commonapi.Sidecar) error {
	storedStateful, err := GetStatefulSet(ctx, cl, namespace, stsMeta.Name)
	statefulSetDef := generateStatefulSetsDef(stsMeta, params, ownerDef, initcontainerParams, containerParams, getSidecars(sidecars))
	if err != nil {
		if err := patch.DefaultAnnotator.SetLastAppliedAnnotation(statefulSetDef); err != nil { //nolint:gocritic
			log.FromContext(ctx).Error(err, "Unable to patch redis statefulset with comparison object")
			return err
		}
		if apierrors.IsNotFound(err) {
			return createStatefulSet(ctx, cl, namespace, statefulSetDef)
		}
		return err
	}
	return patchStatefulSet(ctx, storedStateful, statefulSetDef, namespace, params.RecreateStatefulSet, params.RecreateStatefulsetStrategy, cl)
}

// patchStatefulSet patches the Redis StatefulSet by applying changes while maintaining atomicity.
func patchStatefulSet(ctx context.Context, storedStateful, newStateful *appsv1.StatefulSet, namespace string, recreateStatefulSet bool, deletePropagation *metav1.DeletionPropagation, cl kubernetes.Interface) error {
	// Sync system-managed fields to ensure atomic update.
	syncManagedFields(storedStateful, newStateful)

	vctModified := false
	if hasVolumeClaimTemplates(newStateful, storedStateful) {
		originalCap := storedStateful.Annotations["storageCapacity"]
		if err := HandlePVCResizing(ctx, storedStateful, newStateful, cl); err != nil {
			return err
		}
		// NOTE: this way of detecting changes is hacky because we rely on
		// HandlePVCResizing updating the storedStateful.Annotations as a side
		// effect.  Also, the code will not detect when other VCT fields change
		vctModified = storedStateful.Annotations["storageCapacity"] != originalCap
		if !recreateStatefulSet {
			// Since VolumeClaimTemplate fields are immutable, revert to the stored configuration
			// if we are not recreating the StatefulSet.
			if newStateful.Annotations == nil {
				newStateful.Annotations = make(map[string]string)
			}
			newStateful.Annotations["storageCapacity"] = storedStateful.Annotations["storageCapacity"]
			newStateful.Spec.VolumeClaimTemplates = storedStateful.Spec.VolumeClaimTemplates
			if vctModified {
				log.FromContext(ctx).V(1).Info("VolumeClaimTemplate change is being ignored because the field is immutable. Consider enabling recreating the statefulset option.")
			}
		}
	}

	// Since PodManagementPolicy is immutable, revert to the stored value if we
	// are not recreating the StatefulSet, otherwise the API server would reject
	// the update.
	if !recreateStatefulSet && newStateful.Spec.PodManagementPolicy != storedStateful.Spec.PodManagementPolicy {
		if newStateful.Spec.PodManagementPolicy != "" {
			log.FromContext(ctx).V(1).Info("PodManagementPolicy change is being ignored because the field is immutable. Consider enabling recreating the statefulset option.")
		}
		newStateful.Spec.PodManagementPolicy = storedStateful.Spec.PodManagementPolicy
	}

	// Calculate the patch between the stored and new objects, ignoring immutable or unnecessary fields.
	patchResult, err := patch.DefaultPatchMaker.Calculate(storedStateful, newStateful,
		patch.IgnoreStatusFields(),
		patch.IgnoreVolumeClaimTemplateTypeMetaAndStatus(),
		patch.IgnoreField("kind"),
		patch.IgnoreField("apiVersion"),
	)
	if err != nil {
		log.FromContext(ctx).Error(err, "Unable to calculate patch for redis statefulset")
		return err
	}

	if patchResult.IsEmpty() && !vctModified {
		log.FromContext(ctx).V(1).Info("Reconciliation complete, no changes required.")
		return nil
	}

	log.FromContext(ctx).V(1).Info("Changes detected in statefulset, updating...", "patch", string(patchResult.Patch), "VCT modified", vctModified)

	// Merge missing annotations from the stored object into the new object.
	mergeAnnotations(storedStateful, newStateful)

	// Set the last applied annotation for future patch comparisons.
	if err := patch.DefaultAnnotator.SetLastAppliedAnnotation(newStateful); err != nil {
		log.FromContext(ctx).Error(err, "Failed to set last applied annotation for redis statefulset")
		return err
	}

	return updateStatefulSet(ctx, cl, namespace, newStateful, recreateStatefulSet, deletePropagation)
}

// syncManagedFields syncs system-managed fields from the stored object to the new object.
func syncManagedFields(stored, new *appsv1.StatefulSet) {
	new.ResourceVersion = stored.ResourceVersion
	new.CreationTimestamp = stored.CreationTimestamp
	new.ManagedFields = stored.ManagedFields
}

// storageHasVolumeClaimTemplate checks if the Storage has a meaningful VolumeClaimTemplate defined
// (i.e., not just the zero value). This distinguishes between storage configured with only
// volumeMount (e.g. emptyDir) vs. storage that actually requests a PersistentVolumeClaim.
func storageHasVolumeClaimTemplate(storage *commonapi.Storage) bool {
	if storage == nil {
		return false
	}
	vct := storage.VolumeClaimTemplate
	return len(vct.Spec.AccessModes) > 0 || vct.Spec.Resources.Requests != nil || vct.Spec.StorageClassName != nil || vct.Spec.VolumeName != ""
}

// hasVolumeClaimTemplates checks if the StatefulSet has VolumeClaimTemplates and if their counts match.
func hasVolumeClaimTemplates(new, stored *appsv1.StatefulSet) bool {
	return len(new.Spec.VolumeClaimTemplates) >= 1 && len(new.Spec.VolumeClaimTemplates) == len(stored.Spec.VolumeClaimTemplates)
}

// mergeAnnotations merges annotations from the stored object into the new object if missing.
func mergeAnnotations(stored, new *appsv1.StatefulSet) {
	if new.Annotations == nil {
		new.Annotations = make(map[string]string)
	}
	for key, value := range stored.Annotations {
		if _, exists := new.Annotations[key]; !exists {
			new.Annotations[key] = value
		}
	}
}

// generateStatefulSetsDef generates the statefulsets definition of Redis
func generateStatefulSetsDef(stsMeta metav1.ObjectMeta, params statefulSetParameters, ownerDef metav1.OwnerReference, initcontainerParams initContainerParameters, containerParams containerParameters, sidecars []commonapi.Sidecar) *appsv1.StatefulSet {
	// Generate stable selector labels (only core labels that won't change)
	selectorLabels := extractStatefulSetSelectorLabels(stsMeta.GetLabels())

	statefulset := &appsv1.StatefulSet{
		TypeMeta:   generateMetaInformation("StatefulSet", "apps/v1"),
		ObjectMeta: stsMeta,
		Spec: appsv1.StatefulSetSpec{
			Selector:                             LabelSelectors(selectorLabels),
			ServiceName:                          fmt.Sprintf("%s-headless", stsMeta.Name),
			Replicas:                             params.Replicas,
			UpdateStrategy:                       params.UpdateStrategy,
			PersistentVolumeClaimRetentionPolicy: params.PersistentVolumeClaimRetentionPolicy,
			MinReadySeconds:                      params.MinReadySeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      stsMeta.GetLabels(),
					Annotations: generateStatefulSetsAnots(stsMeta, params.IgnoreAnnotations),
				},
				Spec: corev1.PodSpec{
					Containers: generateContainerDef(
						stsMeta.GetName(),
						containerParams,
						params.ClusterMode,
						params.NodeConfVolume,
						params.EnableMetrics,
						params.ExternalConfig,
						params.ClusterVersion,
						containerParams.AdditionalMountPath,
						sidecars,
					),
					NodeSelector:                  params.NodeSelector,
					TopologySpreadConstraints:     params.TopologySpreadConstraints,
					SecurityContext:               params.PodSecurityContext,
					PriorityClassName:             params.PriorityClassName,
					Affinity:                      params.Affinity,
					TerminationGracePeriodSeconds: params.TerminationGracePeriodSeconds,
					HostNetwork:                   params.HostNetwork,
					Volumes:                       []corev1.Volume{generateConfigVolume(common.VolumeNameConfig)},
				},
			},
		},
	}

	statefulset.Spec.Template.Spec.InitContainers = generateInitContainerDef(containerParams.Role, stsMeta.GetName(), initcontainerParams, params.ExternalConfig, initcontainerParams.AdditionalMountPath, containerParams, params.ClusterVersion)

	if params.PodManagementPolicy != nil {
		statefulset.Spec.PodManagementPolicy = appsv1.PodManagementPolicyType(*params.PodManagementPolicy)
	}

	if params.Tolerations != nil {
		statefulset.Spec.Template.Spec.Tolerations = *params.Tolerations
	}
	if params.ImagePullSecrets != nil {
		statefulset.Spec.Template.Spec.ImagePullSecrets = *params.ImagePullSecrets
	}
	if containerParams.PersistenceEnabled != nil && params.ClusterMode && params.NodeConfVolume {
		statefulset.Spec.VolumeClaimTemplates = append(statefulset.Spec.VolumeClaimTemplates, createPVCTemplate("node-conf", stsMeta, params.NodeConfPersistentVolumeClaim))
	}
	if containerParams.PersistenceEnabled != nil && *containerParams.PersistenceEnabled {
		pvcTplName := util.CoalesceEnv1(common.EnvOperatorSTSPVCTemplateName, stsMeta.GetName())
		statefulset.Spec.VolumeClaimTemplates = append(statefulset.Spec.VolumeClaimTemplates, createPVCTemplate(pvcTplName, stsMeta, params.PersistentVolumeClaim))
	}
	if params.ExternalConfig != nil {
		statefulset.Spec.Template.Spec.Volumes = append(statefulset.Spec.Template.Spec.Volumes, getExternalConfig(*params.ExternalConfig)...)
	}
	if containerParams.AdditionalVolume != nil {
		statefulset.Spec.Template.Spec.Volumes = append(statefulset.Spec.Template.Spec.Volumes, containerParams.AdditionalVolume...)
	}

	if containerParams.TLSConfig != nil {
		statefulset.Spec.Template.Spec.Volumes = append(statefulset.Spec.Template.Spec.Volumes,
			corev1.Volume{
				Name: "tls-certs",
				VolumeSource: corev1.VolumeSource{
					Secret: &containerParams.TLSConfig.Secret,
				},
			})
	}

	if containerParams.ACLConfig != nil {
		if containerParams.ACLConfig.Secret != nil {
			statefulset.Spec.Template.Spec.Volumes = append(statefulset.Spec.Template.Spec.Volumes,
				corev1.Volume{
					Name: "acl-secret",
					VolumeSource: corev1.VolumeSource{
						Secret: containerParams.ACLConfig.Secret,
					},
				})
		} else if containerParams.ACLConfig.PersistentVolumeClaim != nil {
			statefulset.Spec.Template.Spec.Volumes = append(statefulset.Spec.Template.Spec.Volumes,
				corev1.Volume{
					Name: "acl-pvc",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: *containerParams.ACLConfig.PersistentVolumeClaim,
						},
					},
				})
		}
	}

	if params.ServiceAccountName != nil {
		statefulset.Spec.Template.Spec.ServiceAccountName = *params.ServiceAccountName
	}

	AddOwnerRefToObject(statefulset, ownerDef)
	return statefulset
}

func generateConfigVolume(volumeName string) corev1.Volume {
	return corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	}
}

func generateConfigVolumeMount(volumeName string) corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      volumeName,
		MountPath: "/etc/redis",
	}
}

// getExternalConfig will return the redis external configuration
func getExternalConfig(configMapName string) []corev1.Volume {
	return []corev1.Volume{
		{
			Name: "external-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: configMapName,
					},
				},
			},
		},
	}
}

// createPVCTemplate will create the persistent volume claim template
func createPVCTemplate(volumeName string, stsMeta metav1.ObjectMeta, storageSpec corev1.PersistentVolumeClaim) corev1.PersistentVolumeClaim {
	pvcTemplate := storageSpec
	pvcTemplate.CreationTimestamp = metav1.Time{}
	pvcTemplate.Name = volumeName
	pvcTemplate.Labels = stsMeta.GetLabels()
	// We want the same annotation as the StatefulSet here
	pvcTemplate.Annotations = generateStatefulSetsAnots(stsMeta, nil)
	if storageSpec.Spec.AccessModes == nil {
		pvcTemplate.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	} else {
		pvcTemplate.Spec.AccessModes = storageSpec.Spec.AccessModes
	}
	pvcVolumeMode := corev1.PersistentVolumeFilesystem
	if storageSpec.Spec.VolumeMode != nil {
		pvcVolumeMode = *storageSpec.Spec.VolumeMode
	}
	pvcTemplate.Spec.VolumeMode = &pvcVolumeMode
	pvcTemplate.Spec.Resources = storageSpec.Spec.Resources
	pvcTemplate.Spec.Selector = storageSpec.Spec.Selector
	return pvcTemplate
}

// generateContainerDef generates container definition for Redis
func generateContainerDef(name string, containerParams containerParameters, clusterMode, nodeConfVolume, enableMetrics bool, externalConfig, clusterVersion *string, mountpath []corev1.VolumeMount, sidecars []commonapi.Sidecar) []corev1.Container {
	sentinelCntr := containerParams.Role == "sentinel"
	enableTLS := containerParams.TLSConfig != nil
	containerDefinition := []corev1.Container{
		{
			Name:            name,
			Image:           containerParams.Image,
			ImagePullPolicy: containerParams.ImagePullPolicy,
			SecurityContext: containerParams.SecurityContext,
			Env: getEnvironmentVariables(
				containerParams.Role,
				containerParams.EnabledPassword,
				containerParams.SecretName,
				containerParams.SecretKey,
				containerParams.PersistenceEnabled,
				containerParams.TLSConfig,
				containerParams.ACLConfig,
				containerParams.EnvVars,
				containerParams.Port,
				clusterVersion,
				containerParams.Resources,
				containerParams.MaxMemoryPercentOfLimit,
			),
			ReadinessProbe: GetProbeInfo(containerParams.ReadinessProbe, sentinelCntr, enableTLS),
			LivenessProbe:  GetProbeInfo(containerParams.LivenessProbe, sentinelCntr, enableTLS),
			VolumeMounts:   getVolumeMount(name, containerParams.PersistenceEnabled, clusterMode, nodeConfVolume, externalConfig, mountpath, containerParams.TLSConfig, containerParams.ACLConfig),
		},
	}

	if features.Enabled(features.GenerateConfigInInitContainer) {
		if sentinelCntr {
			containerDefinition[0].Command = []string{"redis-sentinel"}
			containerDefinition[0].Args = []string{"/etc/redis/sentinel.conf"}
		} else {
			containerDefinition[0].Command = []string{"redis-server"}
			containerDefinition[0].Args = []string{"/etc/redis/redis.conf"}
		}
	}

	preStopCfg := PreStopConfig{
		Role:               containerParams.Role,
		EnableTLS:          enableTLS,
		SentinelService:    containerParams.SentinelService,
		SentinelMasterName: containerParams.SentinelMasterName,
		SentinelPort:       containerParams.SentinelPort,
		WaitSeconds:        containerParams.PreStopWaitSeconds,
	}
	if preStopCmd := GeneratePreStopCommand(preStopCfg); preStopCmd != "" {
		if secret := containerParams.SentinelPasswordSecret; preStopCfg.Role == "replication" && secret != nil && secret.Name != nil && secret.Key != nil {
			containerDefinition[0].Env = append(containerDefinition[0].Env, corev1.EnvVar{
				Name: sentinelCLIAuthEnv,
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: *secret.Name},
						Key:                  *secret.Key,
					},
				},
			})
		}
		containerDefinition[0].Lifecycle = &corev1.Lifecycle{
			PreStop: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					Command: []string{"sh", "-c", preStopCmd},
				},
			},
		}
	}

	if containerParams.HostPort != nil {
		containerDefinition[0].Ports = []corev1.ContainerPort{
			{
				HostPort:      int32(*containerParams.HostPort),
				ContainerPort: int32(*containerParams.Port),
			},
		}
	}

	if containerParams.Resources != nil {
		containerDefinition[0].Resources = *containerParams.Resources
	}
	if enableMetrics {
		containerDefinition = append(containerDefinition, enableRedisMonitoring(containerParams))
	}
	for _, sidecar := range sidecars {
		container := corev1.Container{
			Name:            sidecar.Name,
			Image:           sidecar.Image,
			ImagePullPolicy: sidecar.ImagePullPolicy,
			SecurityContext: sidecar.SecurityContext,
		}
		if sidecar.Command != nil {
			container.Command = sidecar.Command
		}
		if sidecar.Ports != nil {
			container.Ports = append(container.Ports, *sidecar.Ports...)
		}
		if sidecar.Volumes != nil {
			container.VolumeMounts = *sidecar.Volumes
		}
		if sidecar.Resources != nil {
			container.Resources = *sidecar.Resources
		}
		if sidecar.EnvVars != nil {
			container.Env = *sidecar.EnvVars
		}
		containerDefinition = append(containerDefinition, container)
	}

	if containerParams.AdditionalEnvVariable != nil {
		containerDefinition[0].Env = append(containerDefinition[0].Env, *containerParams.AdditionalEnvVariable...)
	}

	return containerDefinition
}

// redisCLIAuthSanitizer strips CR/LF from REDISCLI_AUTH before redis-cli reads
// it. Password secrets frequently carry a trailing newline (echo | base64,
// kubectl create secret --from-file), and every other consumer of the password
// sees it trimmed: getRedisPassword applies strings.TrimSpace and requirepass
// is read from a line-based config file. REDISCLI_AUTH is sourced raw from the
// secretKeyRef, so without this the server holds the trimmed password while
// redis-cli sends the untrimmed one and every AUTH fails with WRONGPASS.
// The -n guard keeps the variable unset on password-less deployments, where
// exporting an empty value would make redis-cli send AUTH with an empty password.
const redisCLIAuthSanitizer = `if [ -n "${REDISCLI_AUTH:-}" ]; then REDISCLI_AUTH="$(printf %s "$REDISCLI_AUTH" | tr -d '\r\n')"; export REDISCLI_AUTH; fi`

// sentinelCLIAuthEnv carries spec.sentinel.redisSecret into the Redis pods of
// a RedisReplication with an embedded Sentinel. The Sentinel's requirepass is
// that password, not the Redis one in REDISCLI_AUTH, so the preStop hook must
// switch to it for its Sentinel calls.
const sentinelCLIAuthEnv = "SENTINEL_REDISCLI_AUTH"

// sentinelCLIFunc defines sentinel_cli, which runs redis-cli against the
// Sentinel with the Sentinel password instead of the Redis one. It runs in a
// subshell so the Redis password stays in REDISCLI_AUTH for the local calls.
// Without a Sentinel password REDISCLI_AUTH is unset rather than emptied: an
// empty value still makes redis-cli send AUTH, which a password-less Sentinel
// rejects. The password is trimmed of CR/LF like REDISCLI_AUTH (see
// redisCLIAuthSanitizer).
const sentinelCLIFunc = `sentinel_cli() (
    unset REDISCLI_AUTH
    if [ -n "${` + sentinelCLIAuthEnv + `:-}" ]; then
        REDISCLI_AUTH="$(printf %s "$` + sentinelCLIAuthEnv + `" | tr -d '\r\n')"
        export REDISCLI_AUTH
    fi
    exec redis-cli "$@"
)`

// PreStopConfig holds the inputs needed to render a container preStop hook.
type PreStopConfig struct {
	Role      string
	EnableTLS bool
	// SentinelService, SentinelMasterName and SentinelPort describe the
	// Sentinel that manages failover for the "replication" role. They must be
	// sourced from the actual (embedded) Sentinel config rather than derived in
	// shell. When SentinelService is empty the replication hook is disabled, so
	// non-Sentinel replication deployments never get a hook that would block on
	// a non-existent service.
	SentinelService    string
	SentinelMasterName string
	SentinelPort       int
	// WaitSeconds bounds how long a hook waits for the local node to be demoted
	// to a slave -- both the cluster and the replication hook honour it. It is
	// kept below terminationGracePeriodSeconds so the hook returns before the
	// kubelet sends SIGKILL.
	WaitSeconds int
}

// GeneratePreStopCommand generates the preStop script based on the Redis role.
// "cluster" triggers a CLUSTER FAILOVER to the best slave; "replication"
// triggers a Sentinel failover, but only when a Sentinel service is configured.
// All other roles (and Sentinel-less replication) return an empty string.
//
// Authentication is taken from the REDISCLI_AUTH environment variable that the
// operator sets on the pod, so the password is never passed on the command line.
func GeneratePreStopCommand(cfg PreStopConfig) string {
	tlsArgs := GenerateTLSArgs(cfg.EnableTLS)

	switch cfg.Role {
	case "cluster":
		return generateClusterPreStop(tlsArgs, cfg)
	case "replication":
		// Without a Sentinel managing failover there is nothing to fail over
		// to; installing the hook would make every master termination block on
		// a redis-cli call to a service that does not exist.
		if cfg.SentinelService == "" {
			return ""
		}
		return generateReplicationPreStop(tlsArgs, cfg)
	default:
		return ""
	}
}

// preStopWaitSeconds bounds the demotion wait so the preStop hook returns with
// headroom before terminationGracePeriodSeconds elapses, leaving the kubelet
// time to deliver SIGTERM and let Redis shut down cleanly instead of being
// SIGKILLed mid-failover.
func preStopWaitSeconds(gracePeriodSeconds *int64) int {
	const (
		defaultGracePeriodSeconds = 30
		headroomSeconds           = 10
	)
	grace := int64(defaultGracePeriodSeconds)
	if gracePeriodSeconds != nil && *gracePeriodSeconds > 0 {
		grace = *gracePeriodSeconds
	}
	return int(max(grace-headroomSeconds, 1))
}

// GenerateTLSArgs constructs TLS arguments for redis-cli. Authentication is
// supplied via the REDISCLI_AUTH environment variable, never on the command line.
func GenerateTLSArgs(enableTLS bool) string {
	tlsArgs := ""

	if enableTLS {
		tlsArgs = " --tls --cert \"${REDIS_TLS_CERT}\" --key \"${REDIS_TLS_CERT_KEY}\"${REDIS_TLS_CA_CERT:+ --cacert \"${REDIS_TLS_CA_CERT}\"}"
	}
	return tlsArgs
}

// demotionWaitLoop renders the shell loop that blocks until the local node
// reports role:slave, i.e. until a failover triggered above has actually
// completed. The cluster hook uses it; the replication hook ends with the same
// loop against its hook-wide deadline (replicationPreStopScript), so a
// hardening change here belongs there too.
//
// The loop is bounded by a wall-clock deadline rather than an iteration count.
// An iteration costs one redis-cli round trip plus a sleep, so counting
// iterations bounds the wait from below, not above, and a slow local Redis
// could push the hook past terminationGracePeriodSeconds. Re-checking a
// deadline bounds it from above, which is what the hook actually promises.
//
// The individual redis-cli calls deliberately carry no per-call timeout. Do not
// add `redis-cli -t`: that option only exists from Redis 7.4 onwards, and this
// hook is rendered identically for every Redis version the operator supports --
// including the default 7.0.x images. On anything older, redis-cli rejects it
// with "Unrecognized option" and exits 1, so NEW_ROLE would be empty on every
// iteration and the loop would burn the whole budget even when the demotion
// succeeded immediately: worse than the bug this hook exists to fix.
//
// The cost is that a local Redis wedged on a fork or an fsync stall can still
// overrun the deadline by one round trip. That matches the pre-existing
// behaviour, and the deadline keeps the common case honest.
//
// indent is the leading whitespace of the block the loop is spliced into, so
// the rendered script stays readable in `kubectl get sts -o yaml`.
func demotionWaitLoop(tlsArgs string, waitSeconds int, indent string) string {
	loop := fmt.Sprintf(`DEADLINE=$(( $(date +%%s) + %d ))
while [ "$(date +%%s)" -lt "$DEADLINE" ]; do
    NEW_ROLE=$(redis-cli -h $(hostname) -p ${REDIS_PORT} %s info replication | awk -F: '/role:slave/ {print "slave"}')
    if [ "$NEW_ROLE" = "slave" ]; then
        break
    fi
    sleep 1
done`, waitSeconds, tlsArgs)

	lines := strings.Split(loop, "\n")
	for i, line := range lines {
		if line != "" {
			lines[i] = indent + line
		}
	}
	// The caller supplies the indentation of the first line.
	return strings.TrimPrefix(strings.Join(lines, "\n"), indent)
}

// generateClusterPreStop generates the preStop script for Redis cluster mode.
// It identifies the master node and triggers a failover to the best available
// slave before shutdown.
//
// CLUSTER FAILOVER only starts the handoff and returns before it completes, so
// the hook then polls the local node until it has been demoted to a slave. This
// keeps the kubelet from SIGKILLing the old master mid-failover, which would
// abort the coordinated handoff and fall back to slower failure detection. The
// wait is bounded by cfg.WaitSeconds so the hook still returns before the grace
// period expires.
//
// The wait is entered only when the replica accepts the failover. Redis
// declines a manual failover in routine situations -- a replica whose link to
// the master is down, a leader whose slots were already resharded away, or an
// election the replica cannot win -- and in all of them the local node is never
// demoted. Waiting anyway would burn the whole budget on every such
// termination and leave only the headroom for the SIGTERM shutdown, which is
// where a master with save points writes its final RDB.
func generateClusterPreStop(tlsArgs string, cfg PreStopConfig) string {
	waitSeconds := max(cfg.WaitSeconds, 1)
	return fmt.Sprintf(`#!/bin/sh
%s
ROLE=$(redis-cli -h $(hostname) -p ${REDIS_PORT} %s info replication | awk -F: '/role:master/ {print "master"}')

if [ "$ROLE" = "master" ]; then
    BEST_SLAVE=$(redis-cli -h $(hostname) -p ${REDIS_PORT} %s info replication | awk -F: '
        BEGIN { maxOffset = -1; bestSlave = "" }
        /slave[0-9]+:ip/ {
            split($2, a, ",");
            split(a[1], ip_arr, "=");
            split(a[4], offset_arr, "=");
            ip = ip_arr[2];
            offset = offset_arr[2] + 0;
            if (offset > maxOffset) {
                maxOffset = offset;
                bestSlave = ip;
            }
        }
        END { print bestSlave }
    ')

    if [ -n "$BEST_SLAVE" ]; then
        FAILOVER_RESULT=$(redis-cli -h "$BEST_SLAVE" -p ${REDIS_PORT} %s cluster failover 2>&1)
        case "$FAILOVER_RESULT" in
            OK*) ;;
            *)
                # Exit 0: a declined failover is routine, and a non-zero preStop
                # hook only buys a FailedPreStopHook event on every such pod.
                echo "preStop: cluster failover declined: $FAILOVER_RESULT" >&2
                exit 0
                ;;
        esac

        %s
    fi
fi`, redisCLIAuthSanitizer, tlsArgs, tlsArgs, tlsArgs, demotionWaitLoop(tlsArgs, waitSeconds, "        "))
}

// replicationPreStopScript is the template of the replication preStop hook; see
// generateReplicationPreStop. The @NAME@ placeholders are substituted there.
//
// Shell notes:
//   - No `redis-cli -t` per-call timeout, for the reason given on
//     demotionWaitLoop. A hung call can overrun DEADLINE by one round trip;
//     the write pause cannot, because it is armed to expire at DEADLINE.
//   - INFO replies end lines with CRLF. `$0 + 0` drops the CR from offsets.
//   - IPv6 replica addresses contain colons, so offsets are matched by name,
//     not by splitting INFO lines on ':'.
//   - Fractional sleep is not POSIX; images whose sleep rejects it fall back
//     to whole seconds instead of spinning.
const replicationPreStopScript = `#!/bin/sh
@AUTH_SANITIZER@
# One deadline for the whole hook, below terminationGracePeriodSeconds.
DEADLINE=$(( $(date +%s) + @WAIT_SECONDS@ ))
CATCHUP_DEADLINE=$(( $(date +%s) + @CATCHUP_SECONDS@ ))

local_cli() { redis-cli -h "$(hostname)" -p "${REDIS_PORT}"@TLS_ARGS@ "$@"; }
@SENTINEL_CLI_FUNC@
sentinel_call() { sentinel_cli -h "@SENTINEL_HOST@" -p @SENTINEL_PORT@@TLS_ARGS@ "$@"; }
short_sleep() { sleep 0.2 2>/dev/null || sleep 1; }

ROLE=$(local_cli info replication | awk -F: '/role:master/ {print "master"}')
if [ "$ROLE" != "master" ]; then
    exit 0
fi

SELF_NAME=$(hostname)
SELF_ADDRS=$(hostname -i 2>/dev/null)

# is_self HOST: whether a master address reported by Sentinel is this pod.
# Sentinel reports the pod IP, or the pod FQDN when hostnames are announced.
is_self() {
    case "$1" in
        "") return 1 ;;
        "$SELF_NAME"|"$SELF_NAME".*) return 0 ;;
    esac
    for addr in $SELF_ADDRS; do
        if [ "$1" = "$addr" ]; then
            return 0
        fi
    done
    return 1
}

# sentinel_master prints "HOST PORT" of the master Sentinel names, or nothing.
sentinel_master() {
    sentinel_call SENTINEL get-master-addr-by-name "@MASTER_NAME@" 2>/dev/null | tr -d '\r' |
        awk 'NR == 1 { host = $0 } NR == 2 { port = $0 } END { if (host != "" && port ~ /^[0-9]+$/) print host, port }'
}

# Waiting for "the master to be someone else" below only means something if
# Sentinel names this pod now; and if it names another pod, SENTINEL FAILOVER
# would move that pod instead of this one.
CURRENT_MASTER=$(sentinel_master)
if ! is_self "${CURRENT_MASTER% *}"; then
    echo "preStop: sentinel does not report this pod as the master (${CURRENT_MASTER:-no answer}), skipping the failover" >&2
    exit 0
fi

# Fence: from here until this node is a replica, writes wait instead of being
# acknowledged by a master that is about to be replaced. The pause is armed to
# expire at DEADLINE, so even a killed hook cannot leave the node paused past
# it, and the EXIT trap lifts it on every earlier exit.
PAUSED=0
unpause() {
    if [ "$PAUSED" = 1 ]; then
        PAUSED=0
        UNPAUSE_RESULT=$(local_cli CLIENT UNPAUSE 2>&1)
        if [ "$UNPAUSE_RESULT" != "OK" ]; then
            echo "preStop: CLIENT UNPAUSE failed, the pause expires at the hook deadline: $UNPAUSE_RESULT" >&2
        fi
    fi
}
trap unpause EXIT
trap 'exit 1' HUP INT TERM

PAUSE_MS=$(( (DEADLINE - $(date +%s)) * 1000 ))
PAUSE_RESULT=$(local_cli CLIENT PAUSE "$PAUSE_MS" WRITE 2>&1)
if [ "$PAUSE_RESULT" = "OK" ]; then
    PAUSED=1
else
    # Redis older than 6.2 has no CLIENT PAUSE WRITE. Fail over without the
    # fence; the prompt REPLICAOF below still shortens the window in which the
    # old master accepts writes.
    echo "preStop: could not pause writes, failing over without fencing: $PAUSE_RESULT" >&2
fi

if [ "$PAUSED" = 1 ]; then
    # With writes paused master_repl_offset stops moving, so every replica
    # whose acknowledged offset reaches it holds every acknowledged write.
    # Replicas acknowledge once a second, so a healthy one gets there quickly.
    while :; do
        CATCHUP=$(local_cli info replication | awk '
            /^slave[0-9]+:/ {
                n++
                offset[n] = -1
                if (match($0, /offset=[0-9]+/)) offset[n] = substr($0, RSTART + 7, RLENGTH - 7) + 0
            }
            /^master_repl_offset:/ { sub(/^master_repl_offset:/, ""); master = $0 + 0; seen = 1 }
            END {
                if (!seen) exit
                if (n == 0) { print "none"; exit }
                for (i = 1; i <= n; i++) if (offset[i] != master) { print "behind"; exit }
                print "yes"
            }')
        case "$CATCHUP" in
            yes|none) break ;;
        esac
        if [ "$(date +%s)" -ge "$CATCHUP_DEADLINE" ]; then
            break
        fi
        short_sleep
    done
    case "$CATCHUP" in
        yes) ;;
        none)
            echo "preStop: no replica is connected, failing over to whatever Sentinel can promote" >&2
            ;;
        *)
            # Fail over anyway rather than skip. Writes stay paused, so the
            # writes the replica lacks are only those it lacks now. Skipping
            # unpauses: the master keeps acknowledging writes until SIGTERM
            # stops it, and once down-after-milliseconds passes Sentinel
            # promotes the same lagging replica, losing those writes too,
            # after a longer outage. Redis 7's shutdown does wait for lagging
            # replicas, but only for the grace period that is left, over the
            # link that has just failed to catch up.
            echo "preStop: replicas did not catch up within @CATCHUP_SECONDS@s, failing over anyway (${CATCHUP:-INFO unreadable})" >&2
            ;;
    esac
fi

# Only stdout holds the reply. redis-cli prints warnings such as "AUTH failed"
# on stderr and runs the command anyway; captured with the reply, a warning
# hid the OK and the failover looked declined.
FAILOVER_RESULT=$(sentinel_call SENTINEL FAILOVER "@MASTER_NAME@")
case "$FAILOVER_RESULT" in
    OK*) ;;
    *)
        # Exit 0: see generateClusterPreStop.
        echo "preStop: sentinel failover declined: ${FAILOVER_RESULT:-no reply}" >&2
        exit 0
        ;;
esac

# Sentinel demotes the old master only on its next check of it, seconds after
# promoting the replica. Demote this node as soon as Sentinel names the new
# master instead, so it never accepts a write the new master does not have.
NEW_MASTER=""
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    CANDIDATE=$(sentinel_master)
    if [ -n "$CANDIDATE" ] && ! is_self "${CANDIDATE% *}"; then
        NEW_MASTER=$CANDIDATE
        break
    fi
    short_sleep
done
if [ -z "$NEW_MASTER" ]; then
    echo "preStop: sentinel did not report a new master before the deadline" >&2
    exit 0
fi

REPLICAOF_RESULT=$(local_cli REPLICAOF "${NEW_MASTER% *}" "${NEW_MASTER##* }" 2>&1)
case "$REPLICAOF_RESULT" in
    OK*) ;;
    *)
        echo "preStop: REPLICAOF $NEW_MASTER failed, leaving the demotion to sentinel: $REPLICAOF_RESULT" >&2
        ;;
esac
# Paused writes are now re-run against a replica and fail with READONLY.
unpause

while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    NEW_ROLE=$(local_cli info replication | awk -F: '/role:slave/ {print "slave"}')
    if [ "$NEW_ROLE" = "slave" ]; then
        break
    fi
    sleep 1
done`

// generateReplicationPreStop generates the preStop script for Redis replication
// mode. When this pod is the master Sentinel manages, it hands the master role
// to a replica without losing acknowledged writes:
//
//  1. CLIENT PAUSE WRITE fences the local master.
//  2. It waits until every connected replica's acknowledged offset equals
//     master_repl_offset, for at most half of the budget.
//  3. SENTINEL FAILOVER promotes a replica.
//  4. It polls SENTINEL get-master-addr-by-name until Sentinel names another pod.
//  5. REPLICAOF that pod, then CLIENT UNPAUSE: the paused writes fail with
//     READONLY instead of landing on the old master.
//  6. It waits for the local node to report role:slave.
//
// Without the fence the old master keeps acknowledging writes until Sentinel
// demotes it on its next check, about 10 s later, and those writes are dropped
// when it resyncs from the new master.
//
// Everything is bounded by one deadline, cfg.WaitSeconds from the start, which
// is below terminationGracePeriodSeconds. The pause is armed to expire at that
// deadline and an EXIT trap lifts it on every earlier exit, so the node is never
// left paused.
//
// The Sentinel service, master group name and port are injected from the actual
// (embedded) Sentinel configuration rather than derived in shell, so they stay
// correct regardless of the resource name or topology. Every call goes through
// one of two wrappers, so none can miss its flags or credentials:
//
//   - local_cli talks to this Redis with the TLS arguments and REDISCLI_AUTH,
//     sanitized first (the Redis password).
//   - sentinel_call talks to the embedded Sentinel through sentinel_cli, with
//     the Sentinel's own password (spec.sentinel.redisSecret) instead of the
//     Redis one: with distinct passwords Sentinel answers WRONGPASS and the
//     hook never fails over. With TLS the embedded Sentinel only listens on
//     TLS and requires a client certificate, so these calls carry the same
//     TLS arguments as the local ones.
//
// As in the cluster hook, nothing waits for a failover Sentinel declined:
// SENTINEL FAILOVER replies -NOGOODSLAVE when no replica is eligible and
// -INPROGRESS when one is already running.
func generateReplicationPreStop(tlsArgs string, cfg PreStopConfig) string {
	sentinelPort := cfg.SentinelPort
	if sentinelPort == 0 {
		sentinelPort = 26379
	}
	waitSeconds := max(cfg.WaitSeconds, 1)
	// The rest of the budget is left for Sentinel to promote a replica and for
	// the local demotion; both take about a second when nothing is wrong.
	catchupSeconds := max(waitSeconds/2, 1)
	return strings.NewReplacer(
		"@AUTH_SANITIZER@", redisCLIAuthSanitizer,
		"@SENTINEL_CLI_FUNC@", sentinelCLIFunc,
		"@WAIT_SECONDS@", strconv.Itoa(waitSeconds),
		"@CATCHUP_SECONDS@", strconv.Itoa(catchupSeconds),
		"@TLS_ARGS@", tlsArgs,
		"@SENTINEL_HOST@", cfg.SentinelService,
		"@SENTINEL_PORT@", strconv.Itoa(sentinelPort),
		"@MASTER_NAME@", cfg.SentinelMasterName,
	).Replace(replicationPreStopScript)
}

func generateInitContainerDef(role, name string, initcontainerParams initContainerParameters, externalConfig *string, mountpath []corev1.VolumeMount, containerParams containerParameters, clusterVersion *string) []corev1.Container {
	containers := []corev1.Container{}

	if features.Enabled(features.GenerateConfigInInitContainer) {
		// give all container env vars to init container
		envVars := append(
			ptr.Deref(containerParams.EnvVars, []corev1.EnvVar{}),
			ptr.Deref(containerParams.AdditionalEnvVariable, []corev1.EnvVar{})...,
		)

		VolumeMounts := []corev1.VolumeMount{
			generateConfigVolumeMount(common.VolumeNameConfig),
		}
		if externalConfig != nil {
			VolumeMounts = append(VolumeMounts, externalConfigMount)
		}
		if aclMount := generateACLVolumeMount(containerParams.ACLConfig, true); aclMount != nil {
			VolumeMounts = append(VolumeMounts, *aclMount)
		}
		if containerParams.PersistenceEnabled != nil && *containerParams.PersistenceEnabled {
			const aofDataRoot = "/aof-data"
			VolumeMounts = append(VolumeMounts, corev1.VolumeMount{
				Name:      util.CoalesceEnv1(common.EnvOperatorSTSPVCTemplateName, name),
				MountPath: path.Join(aofDataRoot, "data"),
				ReadOnly:  true,
			})
			envVars = append(envVars, corev1.EnvVar{Name: consts.ENV_KEY_AOF_DATA_ROOT, Value: aofDataRoot})
		}

		container := corev1.Container{
			Name:            "init-config",
			Image:           envs.GetInitContainerImage(),
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         []string{"/operator", "agent"},
			SecurityContext: initcontainerParams.SecurityContext,
			Env: getEnvironmentVariables(
				containerParams.Role,
				containerParams.EnabledPassword,
				containerParams.SecretName,
				containerParams.SecretKey,
				containerParams.PersistenceEnabled,
				containerParams.TLSConfig,
				containerParams.ACLConfig,
				&envVars,
				containerParams.Port,
				clusterVersion,
				containerParams.Resources,
				containerParams.MaxMemoryPercentOfLimit,
			),
			VolumeMounts: VolumeMounts,
		}
		// Set init-config resources to match main container if present
		if containerParams.Resources != nil {
			container.Resources = *containerParams.Resources
		}
		if role == "sentinel" {
			container.Args = []string{"bootstrap", "--sentinel"}
		} else {
			container.Args = []string{"bootstrap"}
		}
		containers = append(containers, container)
	}

	if initcontainerParams.Enabled != nil && *initcontainerParams.Enabled {
		containers = append(containers, corev1.Container{
			Name:            "init" + name,
			Image:           initcontainerParams.Image,
			ImagePullPolicy: initcontainerParams.ImagePullPolicy,
			Command:         initcontainerParams.Command,
			Args:            initcontainerParams.Arguments,
			VolumeMounts:    getVolumeMount(name, initcontainerParams.PersistenceEnabled, false, false, nil, mountpath, nil, nil),
			SecurityContext: initcontainerParams.SecurityContext,
			Resources:       ptr.Deref(initcontainerParams.Resources, corev1.ResourceRequirements{}),
			Env:             ptr.Deref(initcontainerParams.AdditionalEnvVariable, []corev1.EnvVar{}),
		})
	}
	return containers
}

func GenerateTLSEnvironmentVariables(tlsconfig *commonapi.TLSConfig) []corev1.EnvVar {
	var envVars []corev1.EnvVar
	root := "/tls/"
	caCert, tlsCert, tlsCertKey := getTLSSecretKeys(tlsconfig)
	hasExplicitCA := tlsconfig != nil && tlsconfig.CaCertFile != ""

	envVars = append(envVars, corev1.EnvVar{
		Name:  "TLS_MODE",
		Value: "true",
	})
	if hasExplicitCA {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "REDIS_TLS_CA_CERT",
			Value: path.Join(root, caCert),
		})
	}
	envVars = append(envVars, corev1.EnvVar{
		Name:  "REDIS_TLS_CERT",
		Value: path.Join(root, tlsCert),
	})
	envVars = append(envVars, corev1.EnvVar{
		Name:  "REDIS_TLS_CERT_KEY",
		Value: path.Join(root, tlsCertKey),
	})
	return envVars
}

// GenerateSentinelTLSEnvironmentVariables returns the TLS environment of a
// Sentinel container whose config is rendered by the sentinel image's
// entrypoint (entrypoint-sentinel.sh). That entrypoint reads the CA path from
// REDIS_TLS_CA_KEY, not REDIS_TLS_CA_CERT, and always writes it to
// tls-ca-cert-file, so the CA is set even without an explicit tlsConfig.CaCertFile:
// Sentinel cannot verify Redis or its peers over TLS without one. The same
// path is also exported as REDIS_TLS_CA_CERT, which the GetProbeInfo probes read.
func GenerateSentinelTLSEnvironmentVariables(tlsconfig *commonapi.TLSConfig) []corev1.EnvVar {
	root := "/tls/"
	caCert, tlsCert, tlsCertKey := getTLSSecretKeys(tlsconfig)
	return []corev1.EnvVar{
		{Name: "TLS_MODE", Value: "true"},
		{Name: "REDIS_TLS_CA_KEY", Value: path.Join(root, caCert)},
		{Name: "REDIS_TLS_CA_CERT", Value: path.Join(root, caCert)},
		{Name: "REDIS_TLS_CERT", Value: path.Join(root, tlsCert)},
		{Name: "REDIS_TLS_CERT_KEY", Value: path.Join(root, tlsCertKey)},
	}
}

// enableRedisMonitoring will add Redis Exporter as sidecar container
func enableRedisMonitoring(params containerParameters) corev1.Container {
	exporterDefinition := corev1.Container{
		Name:            redisExporterContainer,
		Image:           params.RedisExporterImage,
		ImagePullPolicy: params.RedisExporterImagePullPolicy,
		Env:             getExporterEnvironmentVariables(params),
		VolumeMounts:    getVolumeMount("", nil, false, false, nil, params.AdditionalMountPath, params.TLSConfig, params.ACLConfig), // We need/want the tls-certs but we DON'T need the PVC (if one is available)
		Ports: []corev1.ContainerPort{
			{
				Name:          common.RedisExporterPortName,
				ContainerPort: int32(*util.Coalesce(params.RedisExporterPort, ptr.To(common.RedisExporterPort))),
				Protocol:      corev1.ProtocolTCP,
			},
		},
		SecurityContext: params.RedisExporterSecurityContext,
	}
	if params.RedisExporterResources != nil {
		exporterDefinition.Resources = *params.RedisExporterResources
	}
	return exporterDefinition
}

func getExporterEnvironmentVariables(params containerParameters) []corev1.EnvVar {
	var envVars []corev1.EnvVar
	redisHost := "redis://localhost:"
	if params.TLSConfig != nil {
		caCert, tlsCert, tlsKey := getTLSSecretKeys(params.TLSConfig)
		envVars = append(envVars, corev1.EnvVar{
			Name:  "REDIS_EXPORTER_TLS_CLIENT_KEY_FILE",
			Value: path.Join("/tls/", tlsKey),
		})
		envVars = append(envVars, corev1.EnvVar{
			Name:  "REDIS_EXPORTER_TLS_CLIENT_CERT_FILE",
			Value: path.Join("/tls/", tlsCert),
		})
		if params.TLSConfig.CaCertFile != "" {
			envVars = append(envVars, corev1.EnvVar{
				Name:  "REDIS_EXPORTER_TLS_CA_CERT_FILE",
				Value: path.Join("/tls/", caCert),
			})
		}
		envVars = append(envVars, corev1.EnvVar{
			Name:  "REDIS_EXPORTER_SKIP_TLS_VERIFICATION",
			Value: "true",
		})
		redisHost = "rediss://localhost:"
	}
	if params.RedisExporterPort != nil {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "REDIS_EXPORTER_WEB_LISTEN_ADDRESS",
			Value: fmt.Sprintf(":%d", *params.RedisExporterPort),
		})
	}
	if params.Port != nil {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "REDIS_ADDR",
			Value: redisHost + strconv.Itoa(*params.Port),
		})
	}
	if params.EnabledPassword != nil && *params.EnabledPassword {
		envVars = append(envVars, corev1.EnvVar{
			Name: "REDIS_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: *params.SecretName,
					},
					Key: *params.SecretKey,
				},
			},
		})
	}
	if params.RedisExporterEnv != nil {
		envVars = append(envVars, *params.RedisExporterEnv...)
	}

	sort.SliceStable(envVars, func(i, j int) bool {
		return envVars[i].Name < envVars[j].Name
	})
	return envVars
}

var externalConfigMount = corev1.VolumeMount{
	Name:      "external-config",
	MountPath: "/etc/redis/external.conf.d",
}

// getVolumeMount gives information about persistence mount
func getVolumeMount(name string, persistenceEnabled *bool, clusterMode bool, nodeConfVolume bool, externalConfig *string, mountpath []corev1.VolumeMount, tlsConfig *commonapi.TLSConfig, aclConfig *commonapi.ACLConfig) []corev1.VolumeMount {
	var VolumeMounts []corev1.VolumeMount

	if persistenceEnabled != nil && clusterMode && nodeConfVolume {
		VolumeMounts = append(VolumeMounts, corev1.VolumeMount{
			Name:      "node-conf",
			MountPath: "/node-conf",
		})
	}

	if persistenceEnabled != nil && *persistenceEnabled {
		VolumeMounts = append(VolumeMounts, corev1.VolumeMount{
			Name:      util.CoalesceEnv1(common.EnvOperatorSTSPVCTemplateName, name),
			MountPath: "/data",
		})
	}

	if tlsConfig != nil {
		VolumeMounts = append(VolumeMounts, corev1.VolumeMount{
			Name:      "tls-certs",
			ReadOnly:  true,
			MountPath: "/tls",
		})
	}

	if aclMount := generateACLVolumeMount(aclConfig, false); aclMount != nil {
		VolumeMounts = append(VolumeMounts, *aclMount)
	}

	if externalConfig != nil {
		VolumeMounts = append(VolumeMounts, externalConfigMount)
	}

	if features.Enabled(features.GenerateConfigInInitContainer) {
		VolumeMounts = append(VolumeMounts, generateConfigVolumeMount(common.VolumeNameConfig))
	}

	VolumeMounts = append(VolumeMounts, mountpath...)

	return VolumeMounts
}

func generateACLVolumeMount(aclConfig *commonapi.ACLConfig, readOnly bool) *corev1.VolumeMount {
	switch {
	case aclConfig == nil:
		return nil
	case aclConfig.Secret != nil:
		return &corev1.VolumeMount{
			Name:      "acl-secret",
			MountPath: "/etc/redis/user.acl",
			SubPath:   "user.acl",
			ReadOnly:  readOnly,
		}
	case aclConfig.PersistentVolumeClaim != nil:
		return &corev1.VolumeMount{
			Name:      "acl-pvc",
			MountPath: "/data/redis",
			ReadOnly:  readOnly,
		}
	default:
		return nil
	}
}

// GetProbeInfo generate probe for Redis StatefulSet
// The `ping` command will exit successfully even if the node is loading,
// so we need to verify that the Redis `ping` command returns "PONG".
// The rendered command reads REDIS_PORT or SENTINEL_PORT, REDISCLI_AUTH and,
// with TLS, REDIS_TLS_CERT, REDIS_TLS_CERT_KEY and REDIS_TLS_CA_CERT from the
// container environment.
func GetProbeInfo(probe *corev1.Probe, sentinel, enableTLS bool) *corev1.Probe {
	if probe == nil {
		probe = &corev1.Probe{}
	}
	if probe.Exec == nil && probe.HTTPGet == nil && probe.TCPSocket == nil && probe.GRPC == nil {
		redisHealthCheck := []string{
			"redis-cli",
			"-h", "$(hostname)",
		}
		if sentinel {
			redisHealthCheck = append(redisHealthCheck, "-p", "${SENTINEL_PORT}")
		} else {
			redisHealthCheck = append(redisHealthCheck, "-p", "${REDIS_PORT}")
		}
		if enableTLS {
			redisHealthCheck = append(redisHealthCheck, "--tls", "--cert", "${REDIS_TLS_CERT}", "--key", "${REDIS_TLS_CERT_KEY}", "${REDIS_TLS_CA_CERT:+--cacert}", "${REDIS_TLS_CA_CERT}")
		}
		redisHealthCheck = append(redisHealthCheck, "ping")

		redisHealthCheckSubshell := strings.Join(redisHealthCheck, " ")

		healthCheckScript := redisCLIAuthSanitizer + "\n" + "RESP=\"$(" + redisHealthCheckSubshell + ")\"\n" + "[ \"$RESP\" = \"PONG\" ]"

		// `-e` causes the shell to exit immediately if a (nontested) command fails
		probe.ProbeHandler = corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"sh", "-ec", healthCheckScript},
			},
		}
	}
	return probe
}

// getEnvironmentVariables returns all the required Environment Variables
func getEnvironmentVariables(role string, enabledPassword *bool, secretName *string,
	secretKey *string, persistenceEnabled *bool, tlsConfig *commonapi.TLSConfig,
	aclConfig *commonapi.ACLConfig, envVar *[]corev1.EnvVar, port *int, clusterVersion *string,
	resources *corev1.ResourceRequirements, maxMemoryPercentOfLimit *int,
) []corev1.EnvVar {
	envVars := []corev1.EnvVar{
		{Name: "SERVER_MODE", Value: role},
		{Name: "SETUP_MODE", Value: role},
	}

	if clusterVersion != nil {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "REDIS_MAJOR_VERSION",
			Value: *clusterVersion,
		})
	}

	if resources != nil && resources.Limits != nil && maxMemoryPercentOfLimit != nil && *maxMemoryPercentOfLimit > 0 {
		if memLimit := resources.Limits.Memory().Value(); memLimit > 0 {
			maxMem := int(float64(memLimit) * float64(*maxMemoryPercentOfLimit) / 100)
			envVars = append(envVars, corev1.EnvVar{
				Name:  consts.ENV_KEY_REDIS_MAX_MEMORY,
				Value: strconv.FormatInt(int64(maxMem), 10),
			})
		}
	}

	var redisHost string
	if role == "sentinel" {
		redisHost = "redis://localhost:" + strconv.Itoa(common.SentinelPort)
		if port != nil {
			envVars = append(envVars, corev1.EnvVar{
				Name: "SENTINEL_PORT", Value: strconv.Itoa(*port),
			})
		}
	} else {
		redisHost = "redis://localhost:" + strconv.Itoa(common.RedisPort)
		if port != nil {
			envVars = append(envVars, corev1.EnvVar{
				Name: "REDIS_PORT", Value: strconv.Itoa(*port),
			})
		}
	}

	if tlsConfig != nil {
		envVars = append(envVars, GenerateTLSEnvironmentVariables(tlsConfig)...)
	}

	if aclConfig != nil {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "ACL_MODE",
			Value: "true",
		})
		aclFilePath := "/etc/redis/user.acl"
		if aclConfig.PersistentVolumeClaim != nil {
			aclFilePath = "/data/redis/user.acl"
		}
		envVars = append(envVars, corev1.EnvVar{
			Name:  "ACL_FILE_PATH",
			Value: aclFilePath,
		})
	}

	envVars = append(envVars, corev1.EnvVar{
		Name:  "REDIS_ADDR",
		Value: redisHost,
	})

	if enabledPassword != nil && *enabledPassword {
		envVars = append(envVars, corev1.EnvVar{
			Name: "REDIS_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: *secretName,
					},
					Key: *secretKey,
				},
			},
		}, corev1.EnvVar{
			Name: "REDISCLI_AUTH",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: *secretName,
					},
					Key: *secretKey,
				},
			},
		})
	}
	if persistenceEnabled != nil && *persistenceEnabled {
		envVars = append(envVars, corev1.EnvVar{Name: "PERSISTENCE_ENABLED", Value: "true"})
	}

	if envVar != nil {
		envVars = append(envVars, *envVar...)
	}

	sort.SliceStable(envVars, func(i, j int) bool {
		return envVars[i].Name < envVars[j].Name
	})
	return envVars
}

// createStatefulSet is a method to create statefulset in Kubernetes
func createStatefulSet(ctx context.Context, cl kubernetes.Interface, namespace string, stateful *appsv1.StatefulSet) error {
	_, err := cl.AppsV1().StatefulSets(namespace).Create(ctx, stateful, metav1.CreateOptions{})
	if err != nil {
		log.FromContext(ctx).Error(err, "Redis stateful creation failed")
		return err
	}
	log.FromContext(ctx).V(1).Info("Redis stateful successfully created")
	return nil
}

// updateStatefulSet is a method to update statefulset in Kubernetes
func updateStatefulSet(ctx context.Context, cl kubernetes.Interface, namespace string, stateful *appsv1.StatefulSet, recreateStateFulSet bool, deletePropagation *metav1.DeletionPropagation) error {
	_, err := cl.AppsV1().StatefulSets(namespace).Update(ctx, stateful, metav1.UpdateOptions{})
	if recreateStateFulSet {
		sErr, ok := err.(*apierrors.StatusError)
		if ok && sErr.ErrStatus.Code == 422 && sErr.ErrStatus.Reason == metav1.StatusReasonInvalid {
			failMsg := make([]string, len(sErr.ErrStatus.Details.Causes))
			for messageCount, cause := range sErr.ErrStatus.Details.Causes {
				failMsg[messageCount] = cause.Message
			}
			log.FromContext(ctx).V(1).Info("recreating StatefulSet because the update operation wasn't possible", "reason", strings.Join(failMsg, ", "))
			if err := cl.AppsV1().StatefulSets(namespace).Delete(ctx, stateful.GetName(), metav1.DeleteOptions{PropagationPolicy: deletePropagation}); err != nil { //nolint:gocritic
				return errors.Wrap(err, "failed to delete StatefulSet to avoid forbidden action")
			}
			return nil // rely on the controller to recreate the StatefulSet
		}
	}
	if err != nil {
		log.FromContext(ctx).Error(err, "Redis statefulset update failed")
		return err
	}
	log.FromContext(ctx).V(1).Info("Redis statefulset successfully updated ")
	return nil
}

// GetStateFulSet is a method to get statefulset in Kubernetes
func GetStatefulSet(ctx context.Context, cl kubernetes.Interface, namespace string, name string) (*appsv1.StatefulSet, error) {
	getOpts := metav1.GetOptions{
		TypeMeta: generateMetaInformation("StatefulSet", "apps/v1"),
	}
	statefulInfo, err := cl.AppsV1().StatefulSets(namespace).Get(ctx, name, getOpts)
	if err != nil {
		log.FromContext(ctx).V(1).Info("Redis statefulset get action failed")
		return nil, err
	}
	log.FromContext(ctx).V(1).Info("Redis statefulset get action was successful")
	return statefulInfo, nil
}

func getSidecars(sidecars *[]commonapi.Sidecar) []commonapi.Sidecar {
	if sidecars == nil {
		return []commonapi.Sidecar{}
	}
	return *sidecars
}

// getDeletionPropagationStrategy returns the deletion propagation strategy based on the annotation
func getDeletionPropagationStrategy(annotations map[string]string) *metav1.DeletionPropagation {
	if annotations == nil {
		return nil
	}

	if strategy, exists := annotations[common.AnnotationKeyRecreateStatefulsetStrategy]; exists {
		var propagation metav1.DeletionPropagation

		switch strings.ToLower(strategy) {
		case "orphan":
			propagation = metav1.DeletePropagationOrphan
		case "background":
			propagation = metav1.DeletePropagationBackground
		default:
			propagation = metav1.DeletePropagationForeground
		}

		return &propagation
	}

	return nil
}
