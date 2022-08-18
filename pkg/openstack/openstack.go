package openstack

import (
	"fmt"
	"strings"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/blockstorage/noauth"
	"github.com/gophercloud/gophercloud/openstack/blockstorage/v3/attachments"
	"github.com/gophercloud/gophercloud/openstack/blockstorage/v3/snapshots"
	"github.com/gophercloud/gophercloud/openstack/blockstorage/v3/volumes"
	"k8s.io/klog/v2"
)

const (
	cinderAuthStrategy    = "cinderAuthStrategy"
	CinderVolumeType      = "cinderVolumeType"
	CinderBackendDriver   = "cinderBackendDriver"
	CinderEndpoint        = "cinderEndpoint"
	OsUserName            = "osUserName"
	OsProject             = "osProject"
	OsDomainName          = "osDomainName"
	OsRegion              = "osRegion"
	OsIdentityEndpoint    = "osIdentityEndpoint"
	OsCinderEndpointType  = "osCinderEndpointType"
	OsUserPassword        = "osUserPassword"
	CephClientUser        = "cephClientUser"
	CephClientKey         = "cephClientKey"
	NodeVolumeAttachLimit = "nodeVolumeAttachLimit"
	ConnectionProtocol    = "connectionProtocol"
	Local                 = "local"
	iSCSI                 = "iscsi"
	RBD                   = "rbd"
)

type IOpenstack interface {
	CreateVolume(name, zone, volType, snapshotID, sourceVolID string, size int) (*volumes.Volume, error)
	DeleteVolume(volumeID string) error
	AttachVolume(volumeID, mountPoint, hostName string) error
	DetachVolume(volumeID string) error
	GetVolumeByName(volumeName string) ([]volumes.Volume, error)
	GetVolumeByID(volumeID string) (*volumes.Volume, error)
	GetAvailability() (string, error)
	ListVolume(maxLimit int32, marker string) ([]volumes.Volume, string, error)
	CreateSnapShot(name string, sourceVolumeID string) (*snapshots.Snapshot, error)
	DeleteSnapshot(snapshotID string) error
	ListSnapshot(filter map[string]string) ([]snapshots.Snapshot, string, error)
	GetSnapshotByID(snapshotID string) (*snapshots.Snapshot, error)
	ExpandVolume(volumeID string, status string, size int) error
	CreateAttachment(volumeID string) (*attachments.Attachment, error)
	GetAttachmentByVolumeId(volID string) (*attachments.Attachment, error)
	DeleteAttachmentByVolumeId(volID string) error
	GetAttachmentConnentionInfo(volumeID string) (map[string]interface{}, error)
	CheckBlockStorageAPI() error
}

type Openstack struct {
	BlockStorageClient *gophercloud.ServiceClient
	EsOpts             gophercloud.EndpointOpts

	VolumeType    string
	BackendDriver string
}

func CreateOpenstackClient(secrets map[string]string) (IOpenstack, error) {
	klog.InfoS("xxxxxxxxxxxxxxxxxxxx", "openstack secret", secrets)
	var err error
	opts := gophercloud.AuthOptions{
		IdentityEndpoint: secrets[OsIdentityEndpoint],
		Username:         secrets[OsUserName],
		Password:         secrets[OsUserPassword],
		DomainName:       secrets[OsDomainName],
		TenantName:       secrets[OsProject],
		AllowReauth:      true,
	}
	provider, err := openstack.AuthenticatedClient(opts)
	if err != nil {
		klog.Error(fmt.Sprintf("Request openstack provider client failed, %v", err))
		return nil, err
	}

	if err != nil {
		klog.Error(fmt.Sprintf("Get openstack provider client failed, %v", err))
	}

	var blockStorageClient *gophercloud.ServiceClient
	if strings.ToLower(secrets[cinderAuthStrategy]) == "keystone" {
		esOpts := gophercloud.EndpointOpts{
			Region:       secrets[OsRegion],
			Availability: gophercloud.Availability(secrets[OsCinderEndpointType]),
		}
		blockStorageClient, err = openstack.NewBlockStorageV3(provider, esOpts)
		if err != nil {
			klog.Error(fmt.Sprintf("Get keystone openstack client failed, %v", err))
			return nil, err
		}
	} else if strings.ToLower(secrets[cinderAuthStrategy]) == "noauth" {
		blockStorageClient, err = noauth.NewBlockStorageNoAuthV3(provider, noauth.EndpointOpts{CinderEndpoint: secrets["cinderEndpoint"]})
		if err != nil {
			klog.Error(fmt.Sprintf("Get noauth openstack client failed, %v", err))
			return nil, err
		}
	}
	if err != nil {
		klog.Error(fmt.Sprintf("The parameter 'nodeVolumeAttachLimit' must be a integer."))
	}
	OsInstance := &Openstack{
		BlockStorageClient: blockStorageClient,
		EsOpts: gophercloud.EndpointOpts{
			Region:       secrets[OsRegion],
			Availability: gophercloud.Availability(secrets[OsCinderEndpointType]),
		},
		VolumeType: secrets[CinderVolumeType],
	}
	return OsInstance, nil
}
