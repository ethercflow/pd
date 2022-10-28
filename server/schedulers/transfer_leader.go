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
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/plan"
	"github.com/tikv/pd/server/storage/endpoint"
)

const (
	// TransferLeaderName is transfer leader scheduler name.
	TransferLeaderName = "transfer-leader-scheduler"
	// TransferLeaderType is transfer leader scheduler type.
	TransferLeaderType = "transfer-leader"
	// TransferLeaderBatchSize is the number of operators to to transfer
	// leaders by one scheduling
	TransferLeaderBatchSize = 3
)

func init() {
	schedule.RegisterSliceDecoderBuilder(TransferLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 1 {
				return errs.ErrSchedulerConfig.FastGenByArgs("id")
			}
			conf, ok := v.(*transferLeaderSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			regionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
			}
			for _, id := range conf.Regions {
				if id == regionID {
					return errs.ErrSchedulerConfig.FastGen("dup id")
				}
			}
			conf.Regions = append(conf.Regions, regionID)
			return nil
		}
	})

	schedule.RegisterScheduler(TransferLeaderType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &transferLeaderSchedulerConfig{Regions: make([]uint64, 0), storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.cluster = opController.GetCluster()
		return newTransferLeaderScheduler(opController, conf), nil
	})
}

type transferLeaderSchedulerConfig struct {
	mu      syncutil.RWMutex
	storage endpoint.ConfigStorage
	Regions []uint64
	cluster schedule.Cluster

	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
}

func (conf *transferLeaderSchedulerConfig) getRegions() []uint64 {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return conf.Regions
}

func (conf *transferLeaderSchedulerConfig) BuildWithArgs(args []string) error {
	if len(args) != 1 {
		return errs.ErrSchedulerConfig.FastGenByArgs("id")
	}
	regionID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
	}
	conf.mu.RLock()
	for _, id := range conf.Regions {
		if id == regionID {
			conf.mu.RUnlock()
			return errs.ErrSchedulerConfig.FastGen("dup id")
		}
	}
	conf.Regions = append(conf.Regions, regionID)
	conf.mu.RUnlock()
	return nil
}

func (conf *transferLeaderSchedulerConfig) Clone() *transferLeaderSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	regions := make([]uint64, len(conf.Regions))
	copy(regions, conf.Regions)
	return &transferLeaderSchedulerConfig{
		Regions: regions,
	}
}

func (conf *transferLeaderSchedulerConfig) Persist() error {
	name := conf.getSchedulerName()
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	data, err := schedule.EncodeConfig(conf)
	failpoint.Inject("persistFail", func() {
		err = errors.New("fail to persist")
	})
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(name, data)
}

func (conf *transferLeaderSchedulerConfig) removeRegionID(id uint64) {
	conf.mu.Lock()
	defer conf.mu.Unlock()
	for i, other := range conf.Regions {
		if other == id {
			conf.Regions = append(conf.Regions[:i], conf.Regions[i+1:]...)
			break
		}
	}
}

func (conf *transferLeaderSchedulerConfig) getSchedulerName() string {
	return TransferLeaderName
}

type transferLeaderScheduler struct {
	*BaseScheduler
	conf    *transferLeaderSchedulerConfig
	reigons chan *core.RegionInfo
}

// newTransferLeaderScheduler creates an admin scheduler that transfers leader of a region.
func newTransferLeaderScheduler(opController *schedule.OperatorController, conf *transferLeaderSchedulerConfig) schedule.Scheduler {
	s:= &transferLeaderScheduler{
		BaseScheduler: NewBaseScheduler(opController),
		conf:          conf,
		regions: make(chan *core.RegionInfo, 1000)
	}
	

	return s
}

func (s *transferLeaderScheduler) GetName() string {
	return TransferLeaderName
}

func (s *transferLeaderScheduler) GetType() string {
	return TransferLeaderType
}

func (s *transferLeaderScheduler) EncodeConfig() ([]byte, error) {
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	return schedule.EncodeConfig(s.conf)
}

func (s *transferLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
	}
	return allowed
}

func (s *transferLeaderScheduler) Schedule(cluster schedule.Cluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	return scheduleTransferLeaderBatch(s.GetName(), s.GetType(), cluster, s.conf, TransferLeaderBatchSize), nil
}

func (s *transferLeaderScheduler) UpdateConfig(args []string) error {
	err := s.conf.BuildWithArgs(args)
	if err != nil {
		return err
	}
	err = s.conf.Persist()
	if err != nil {
		regionID, _ := strconv.ParseUint(args[0], 10, 64)
		s.conf.removeRegionID(regionID)
	}
	return err
}

type transferLeaderSchedulerConf interface {
	getRegions() []uint64
	removeRegionID(uint64)
	Persist() error
}

func scheduleTransferLeaderBatch(name, typ string, cluster schedule.Cluster, conf transferLeaderSchedulerConf, batchSize int) []*operator.Operator {
	var ops []*operator.Operator
	for i := 0; i < batchSize; i++ {
		once := scheduleTransferLeaderOnce(name, typ, cluster, conf)
		// no more regions
		if len(once) == 0 {
			break
		}
		ops = uniqueAppendOperator(ops, once...)
		// the batch has been fulfilled
		if len(ops) > batchSize {
			break
		}
	}
	return ops
}

func scheduleTransferLeaderOnce(name, typ string, cluster schedule.Cluster, conf transferLeaderSchedulerConf) []*operator.Operator {
	regionIDs := conf.getRegions()
	ops := make([]*operator.Operator, 0, len(regionIDs))
	for _, id := range regionIDs {
		region := cluster.GetRegion(id)
		if region != nil {
			var filters []filter.Filter
			unhealthyPeerStores := make(map[uint64]struct{})
			for _, peer := range region.GetDownPeers() {
				unhealthyPeerStores[peer.GetPeer().GetStoreId()] = struct{}{}
			}
			for _, peer := range region.GetPendingPeers() {
				unhealthyPeerStores[peer.GetStoreId()] = struct{}{}
			}
			filters = append(filters, filter.NewExcludedFilter(name, nil, unhealthyPeerStores), &filter.StoreStateFilter{ActionScope: name, TransferLeader: true})
			candidates := filter.NewCandidates(cluster.GetFollowerStores(region)).
				FilterTarget(cluster.GetOpts(), nil,
					filters...)
			// Compatible with old TiKV transfer leader logic.
			target := candidates.RandomPick()
			targets := candidates.PickAll()
			// `targets` MUST contains `target`, so only needs to check if `target` is nil here.
			if target == nil {
				schedulerCounter.WithLabelValues(name, "no-target-store").Inc()
				continue
			}
			targetIDs := make([]uint64, 0, len(targets))
			for _, t := range targets {
				targetIDs = append(targetIDs, t.GetID())
			}
			op, err := operator.CreateTransferLeaderOperator(typ, cluster, region, region.GetLeader().GetStoreId(), target.GetID(), targetIDs, operator.OpLeader)
			if err != nil {
				log.Debug("fail to create transfer leader operator", errs.ZapError(err))
				continue
			}
			op.SetPriorityLevel(core.Urgent)
			op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(name, "new-operator"))
			ops = append(ops, op)
			conf.removeRegionID(id)
		}
	}
	conf.Persist()

	return ops
}
