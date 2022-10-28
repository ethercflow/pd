// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"context"
	"reflect"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/storage"
)

func TestTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)

	// Add stores 1, 2, 3
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	// Add regions 1 with leader in stores 1
	tc.AddLeaderRegion(1, 1, 2, 3)

	sl, err := schedule.CreateScheduler(TransferLeaderType, schedule.NewOperatorController(ctx, nil, nil), storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(TransferLeaderType, []string{"1"}))
	re.NoError(err)
	re.True(sl.IsScheduleAllowed(tc))
	ops, _ := sl.Schedule(tc, false)
	testutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{2, 3})
	re.False(ops[0].Step(0).(operator.TransferLeader).IsFinish(tc.MockRegionInfo(1, 1, []uint64{2, 3}, []uint64{}, &metapb.RegionEpoch{ConfVer: 0, Version: 0})))
	re.True(ops[0].Step(0).(operator.TransferLeader).IsFinish(tc.MockRegionInfo(1, 2, []uint64{1, 3}, []uint64{}, &metapb.RegionEpoch{ConfVer: 0, Version: 0})))
}

func TestTransferLeaderWithUnhealthyPeer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	sl, err := schedule.CreateScheduler(TransferLeaderType, schedule.NewOperatorController(ctx, nil, nil), storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(TransferLeaderType, []string{"1"}))
	re.NoError(err)

	// Add stores 1, 2, 3
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	// Add region 1, which has 3 peers. 1 is leader. 2 is healthy or pending, 3 is healthy or down.
	tc.AddLeaderRegion(1, 1, 2, 3)
	region := tc.MockRegionInfo(1, 1, []uint64{2, 3}, nil, nil)
	withDownPeer := core.WithDownPeers([]*pdpb.PeerStats{{
		Peer:        region.GetPeers()[2],
		DownSeconds: 1000,
	}})
	withPendingPeer := core.WithPendingPeers([]*metapb.Peer{region.GetPeers()[1]})

	// only pending
	tc.PutRegion(region.Clone(withPendingPeer))
	ops, _ := sl.Schedule(tc, false)
	testutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{3})
	ops, _ = sl.Schedule(tc, false)
	re.Nil(ops)
	// only down
	tc.PutRegion(region.Clone(withDownPeer))
	sl.UpdateConfig([]string{"1"})
	ops, _ = sl.Schedule(tc, false)
	testutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{2})
	// pending + down
	tc.PutRegion(region.Clone(withPendingPeer, withDownPeer))
	ops, _ = sl.Schedule(tc, false)
	re.Empty(ops)
}

func TestTransferLeaderConfigClone(t *testing.T) {
	re := require.New(t)

	emptyConf := &transferLeaderSchedulerConfig{Regions: make([]uint64, 0)}
	con2 := emptyConf.Clone()
	re.Empty(emptyConf.getRegions())
	re.NoError(con2.BuildWithArgs([]string{"1"}))
	re.NotEmpty(con2.getRegions())
	re.Empty(emptyConf.getRegions())

	con3 := con2.Clone()
	con3.Regions = []uint64{1, 2, 3}
	re.Empty(emptyConf.getRegions())
	re.False(len(con3.getRegions()) == len(con2.getRegions()))

	con4 := con3.Clone()
	re.True(reflect.DeepEqual(con4.getRegions(), con3.getRegions()))
	con4.Regions[0] = 4
	re.False(con4.Regions[0] == con3.Regions[0])
}
