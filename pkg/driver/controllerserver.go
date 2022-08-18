package driver

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/gophercloud/gophercloud/openstack/blockstorage/v3/snapshots"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"

	"github.com/kungze/cinder-metal-csi/pkg/openstack"
)

type ControllerServer struct {
	driver *Driver
}

func (c *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.NotFound, "CreateVolume: The volume name not exists")
	}
	size := RoundOffBytes(req.GetCapacityRange().GetRequiredBytes())
	if size < 1 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume: The volume size must greater than 1GB")
	}
	volumeType := req.GetParameters()["cinderVolumeType"]
	if volumeType == "" {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume: The StorageClass 'cinderVolumeType' is required.")
	}
	cloud, err := openstack.CreateOpenstackClient(req.GetSecrets())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("CreateVolume: Failed to initial openstack cinder client: %v", err))
	}
	// Verify that a volume with the same name exists
	vol, err := cloud.GetVolumeByName(name)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("CreateVolume: Encounter error while try to query cinder volume: %v", err))
	}
	if len(vol) == 1 {
		if size != vol[0].Size {
			return nil, status.Errorf(codes.AlreadyExists, "CreateVolume: Volume already exists with same name and different capacity")
		}
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      vol[0].ID,
				CapacityBytes: int64(vol[0].Size * 1024 * 1024 * 1024),
			},
		}, nil
	} else if len(vol) > 1 {
		return nil, status.Error(codes.Internal, "CreateVolume: Multiple volumes reported by cinder with same name")
	}

	snapshotID := req.GetVolumeContentSource().GetSnapshot().GetSnapshotId()
	if snapshotID != "" {
		_, err := cloud.GetSnapshotByID(snapshotID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to retrieve the snapshot %s: %v", snapshotID, err)
		}
	}

	sourceVolID := req.GetVolumeContentSource().GetVolume().GetVolumeId()
	if sourceVolID != "" {
		_, err := cloud.GetVolumeByID(sourceVolID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to retrieve the source volume %s: %v", sourceVolID, err)
		}
	}
	region := req.GetParameters()["osRegion"]
	if region == "" {
		region, err = cloud.GetAvailability()
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("CreateVolume: Get volume Availability zone failed, %v", err))
		}
	}
	volume, err := cloud.CreateVolume(name, region, volumeType, snapshotID, sourceVolID, size)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("CreateVolume: Request openstack create volume failed, %v", err))
	}
	klog.Infof("CreateVolume: Create the volume %s success!", volume.ID)
	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volume.ID,
			CapacityBytes: int64(volume.Size) * 1024 * 1024 * 1024,
		},
	}, nil
}

func (c *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	var err error
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.NotFound, "DeleteVolume: The volume ID not exists!!")
	}
	cloud, err := openstack.CreateOpenstackClient(req.GetSecrets())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("DeleteVolume: Failed to initial openstack cinder client: %v", err))
	}
	err = cloud.DeleteVolume(volumeID)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("DeleteVolume: Delete the volume %s failed: %v", volumeID, err))
	}
	klog.Infof("DeleteVolume: Delete the volume %s success", volumeID)
	return &csi.DeleteVolumeResponse{}, nil
}

const (
	RBD   = "rbd"
	ISCSI = "iscsi"
)

func convertConnectionInfoToContext(connInfo map[string]interface{}, req *csi.ControllerPublishVolumeRequest) (*connectionContext, error) {
	connContext := connectionContext{}
	data := connInfo["data"].(map[string]interface{})
	connContext.volumeID = fmt.Sprint(data["volume_id"])
	connProto := fmt.Sprint(data["driver_volume_type"])
	switch strings.ToLower(connProto) {
	case RBD:
		var monAddrs []string
		hosts := data["hosts"].([]string)
		ports := data["ports"].([]string)
		for index, host := range hosts {
			monAddrs = append(monAddrs, host+":"+ports[index])
		}
		keyring := req.GetSecrets()["keyring"]
		if keyring == "" {
			keyring = fmt.Sprint(data["keyring"])
		}
		connContext.monHosts = strings.Join(monAddrs, ",")
		connContext.keyring = keyring
	case ISCSI:

	default:
		return nil, fmt.Errorf("Connecton protocol: %s don't support.", connProto)
	}
	return &connContext, nil
}

type connectionContext struct {
	volumeID         string
	name             string
	authEnabled      string
	authUsername     string
	authMethod       string
	authPassword     string
	driverVolumeType string
	targetDiscovered string
	targetPortals    string
	targetIqns       string
	targetLuns       string
	targetPortal     string
	targetIqn        string
	targetLun        string
	monHosts         string
	clusterName      string
	keyring          string
}

func (c *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.NotFound, "ControllerPublishVolume: The volume ID not exists!!")
	}
	cloud, err := openstack.CreateOpenstackClient(req.GetSecrets())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("ControllerPublishVolume: Failed to initial openstack cinder client: %v", err))
	}
	_, err = cloud.CreateAttachment(volumeID)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("ControllerPublishVolume: Failed to create attachment: %v", err))
	}
	return &csi.ControllerPublishVolumeResponse{
		PublishContext: req.GetSecrets(),
	}, nil
}

func (c *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.NotFound, "ControllerPublishVolume: The volume ID not exists!!")
	}
	cloud, err := openstack.CreateOpenstackClient(req.GetSecrets())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("ControllerUnpublishVolume: Failed to initial openstack cinder client: %v", err))
	}
	err = cloud.DeleteAttachmentByVolumeId(volumeID)
	if err != nil {
		return nil, err
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (c *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ValidateVolumeCapabilities is not yet implemented")
}

func (c *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ListVolumes is not yet implemented")
}

func (c *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "GetCapacity is not yet implemented")
}

func (c *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: c.driver.cap,
	}, nil
}

func (c *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	sourceVolumeId := req.GetSourceVolumeId()
	if sourceVolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "CreateSnapshot: The source volume id is required fields")
	}
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "CreateSnapshot: The create snapshot name is none")
	}
	cloud, err := openstack.CreateOpenstackClient(req.GetSecrets())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("CreateVolume: Failed to initial openstack cinder client: %v", err))
	}
	// Verify a snapshot with the provided name doesn't already exist for this tenant
	filter := map[string]string{}
	filter["name"] = name
	snapshot, _, err := cloud.ListSnapshot(filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("CreateSnapshot: Get the snapshots info failed, %v", err))
	}
	var snap *snapshots.Snapshot
	if len(snapshot) == 1 {
		snap = &snapshot[0]
		if snap.VolumeID != sourceVolumeId {
			return nil, status.Error(codes.AlreadyExists, "CreateSnapshot: snapshot with given name already exists, with different source volume ID")
		}
	} else if len(snapshot) > 1 {
		return nil, status.Error(codes.Internal, "CreateSnapshot: Multiple snapshots reported by Cinder with same name")
	} else {
		properties := map[string]string{}
		for _, mKey := range []string{"csi.storage.k8s.io/volumesnapshot/name", "csi.storage.k8s.io/volumesnapshot/namespace", "csi.storage.k8s.io/volumesnapshotcontent/name"} {
			if v, ok := req.Parameters[mKey]; ok {
				properties[mKey] = v
			}
		}
		snap, err = cloud.CreateSnapShot(name, sourceVolumeId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "CreateSnapshot: Create snapshot volume %s failed, %v", name, err)
		}
	}
	klog.Infof("CreateSnapshot: Create snapshot volume %s success", snap.ID)
	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snap.ID,
			SizeBytes:      int64(snap.Size) * 1024 * 1024 * 1024,
			CreationTime:   timestamppb.New(snap.CreatedAt),
			ReadyToUse:     true,
			SourceVolumeId: snap.VolumeID,
		},
	}, nil
}

func (c *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	snapshotID := req.GetSnapshotId()
	if snapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "DeleteSnapshot: The snapshot volume ID is required fields")
	}
	cloud, err := openstack.CreateOpenstackClient(req.GetSecrets())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("CreateVolume: Failed to initial openstack cinder client: %v", err))
	}
	err = cloud.DeleteSnapshot(snapshotID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "DeleteSnapshot: Delete snapshot volume id %s is failed, %v", snapshotID, err)
	}
	klog.Infof("DeleteSnapshot: Delete snapshot volume %s success", snapshotID)
	return &csi.DeleteSnapshotResponse{}, nil
}

func (c *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	cloud, err := openstack.CreateOpenstackClient(req.GetSecrets())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("CreateVolume: Failed to initial openstack cinder client: %v", err))
	}
	snapshotID := req.GetSnapshotId()
	if snapshotID != "" {
		snapshot, err := cloud.GetSnapshotByID(snapshotID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, fmt.Sprintf("ListSnapshots: Get snapshot volume info failed, %v", err))
		}
		var vEntries []*csi.ListSnapshotsResponse_Entry
		entry := &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				SnapshotId:     snapshot.ID,
				SizeBytes:      int64(snapshot.Size) * 1024 * 1024 * 1024,
				SourceVolumeId: snapshot.VolumeID,
				CreationTime:   timestamppb.New(snapshot.CreatedAt),
			},
		}
		vEntries = append(vEntries, entry)
		return &csi.ListSnapshotsResponse{
			Entries: vEntries,
		}, nil
	}
	filter := map[string]string{}
	volumeID := req.GetSourceVolumeId()
	if volumeID != "" {
		filter["volumeID"] = volumeID
	} else if req.GetMaxEntries() > 0 && req.GetStartingToken() != "" {
		filter["limit"] = strconv.Itoa(int(req.MaxEntries))
		filter["marker"] = req.StartingToken
	}
	filter["status"] = "available"
	snapshot, nextToken, err := cloud.ListSnapshot(filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("ListSnapshots: Get snapshot volume list failed, %v", err))
	}
	var vEntries []*csi.ListSnapshotsResponse_Entry
	for _, snap := range snapshot {
		entry := &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				SnapshotId:     snap.ID,
				SizeBytes:      int64(snap.Size) * 1024 * 1024 * 1024,
				SourceVolumeId: snap.VolumeID,
				CreationTime:   timestamppb.New(snap.CreatedAt),
			},
		}
		vEntries = append(vEntries, entry)
	}
	return &csi.ListSnapshotsResponse{
		Entries:   vEntries,
		NextToken: nextToken,
	}, nil
}

func (c *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "ControllerExpandVolume: The volume id must provider")
	}
	sizeBytes := req.GetCapacityRange().GetRequiredBytes()
	if sizeBytes < 0 {
		return nil, status.Error(codes.InvalidArgument, "ControllerExpandVolume: The volume size must greater than 0")
	}
	maxSize := req.GetCapacityRange().GetLimitBytes()
	if maxSize > 0 && maxSize < sizeBytes {
		return nil, status.Error(codes.OutOfRange, "ControllerExpandVolume: The volume size exceeds the limit specified")
	}
	cloud, err := openstack.CreateOpenstackClient(req.GetSecrets())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("CreateVolume: Failed to initial openstack cinder client: %v", err))
	}
	vol, err := cloud.GetVolumeByID(volumeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("ControllerExpandVolume: Get volume %s info failed, %v", volumeID, err))
	}
	size := RoundOffBytes(sizeBytes)
	if size < vol.Size {
		klog.Infof(fmt.Sprintf("ControllerExpandVolume: Request extend cinder volume size %d must greater exists volume size %d", size, vol.Size))
		return &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         int64(vol.Size) * 1024 * 1024,
			NodeExpansionRequired: false,
		}, nil
	}
	err = cloud.ExpandVolume(volumeID, vol.Status, size)
	if err != nil {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("ControllerExpandVolume: Request expand volume %s failed, %v", vol.ID, err))
	}
	klog.Infof("ControllerExpandVolume: Expand the volume %s success", volumeID)
	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         int64(size) * 1024 * 1024 * 1024,
		NodeExpansionRequired: true,
	}, nil
}

func (c *ControllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerGetVolume is not yet implemented")
}

func NewControllerServer(d *Driver) csi.ControllerServer {
	return &ControllerServer{
		driver: d,
	}
}
