// Copyright 2017 TiKV Project Authors.
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

package schedule

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/placement"
	"go.uber.org/zap"
)

const regionScatterName = "region-scatter"

var (
	gcInterval = time.Minute
	gcTTL      = time.Minute * 3
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	scatterSkipEmptyRegionCounter   = scatterCounter.WithLabelValues("skip", "empty-region")
	scatterSkipNoRegionCounter      = scatterCounter.WithLabelValues("skip", "no-region")
	scatterSkipNoLeaderCounter      = scatterCounter.WithLabelValues("skip", "no-leader")
	scatterSkipHotRegionCounter     = scatterCounter.WithLabelValues("skip", "hot")
	scatterSkipNotReplicatedCounter = scatterCounter.WithLabelValues("skip", "not-replicated")
	scatterUnnecessaryCounter       = scatterCounter.WithLabelValues("unnecessary", "")
	scatterFailCounter              = scatterCounter.WithLabelValues("fail", "")
	scatterSuccessCounter           = scatterCounter.WithLabelValues("success", "")
)

type selectedStores struct {
	mu                syncutil.RWMutex
	groupDistribution *cache.TTLString // value type: map[uint64]uint64, group -> StoreID -> count
}

func newSelectedStores(ctx context.Context) *selectedStores {
	return &selectedStores{
		groupDistribution: cache.NewStringTTL(ctx, gcInterval, gcTTL),
	}
}

// Put plus count by storeID and group
func (s *selectedStores) Put(id uint64, group string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	distribution, ok := s.getDistributionByGroupLocked(group)
	if !ok {
		distribution = map[uint64]uint64{}
		distribution[id] = 0
	}
	distribution[id]++
	s.groupDistribution.Put(group, distribution)
}

// Get the count by storeID and group
func (s *selectedStores) Get(id uint64, group string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	distribution, ok := s.getDistributionByGroupLocked(group)
	if !ok {
		return 0
	}
	count, ok := distribution[id]
	if !ok {
		return 0
	}
	return count
}

// GetGroupDistribution get distribution group by `group`
func (s *selectedStores) GetGroupDistribution(group string) (map[uint64]uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getDistributionByGroupLocked(group)
}

// TotalCountByStore counts the total count by store
func (s *selectedStores) TotalCountByStore(storeID uint64) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	groups := s.groupDistribution.GetAllID()
	totalCount := uint64(0)
	for _, group := range groups {
		storeDistribution, ok := s.getDistributionByGroupLocked(group)
		if !ok {
			continue
		}
		count, ok := storeDistribution[storeID]
		if !ok {
			continue
		}
		totalCount += count
	}
	return totalCount
}

// getDistributionByGroupLocked should be called with lock
func (s *selectedStores) getDistributionByGroupLocked(group string) (map[uint64]uint64, bool) {
	if result, ok := s.groupDistribution.Get(group); ok {
		return result.(map[uint64]uint64), true
	}
	return nil, false
}

// RegionScatterer scatters regions.
type RegionScatterer struct {
	ctx            context.Context
	name           string
	cluster        Cluster
	ordinaryEngine engineContext
	specialEngines sync.Map
	opController   *OperatorController
}

// NewRegionScatterer creates a region scatterer.
// RegionScatter is used for the `Lightning`, it will scatter the specified regions before import data.
func NewRegionScatterer(ctx context.Context, cluster Cluster, opController *OperatorController) *RegionScatterer {
	return &RegionScatterer{
		ctx:          ctx,
		name:         regionScatterName,
		cluster:      cluster,
		opController: opController,
		ordinaryEngine: newEngineContext(ctx, func() filter.Filter {
			return filter.NewEngineFilter(regionScatterName, filter.NotSpecialEngines)
		}),
	}
}

type filterFunc func() filter.Filter

type engineContext struct {
	filterFuncs     []filterFunc
	selectedPeer    *selectedStores
	selectedLeader  *selectedStores
	selectedWitness *selectedStores
}

func newEngineContext(ctx context.Context, filterFuncs ...filterFunc) engineContext {
	filterFuncs = append(filterFuncs, func() filter.Filter {
		return &filter.StoreStateFilter{ActionScope: regionScatterName, MoveRegion: true, ScatterRegion: true}
	})
	return engineContext{
		filterFuncs:     filterFuncs,
		selectedPeer:    newSelectedStores(ctx),
		selectedLeader:  newSelectedStores(ctx),
		selectedWitness: newSelectedStores(ctx),
	}
}

const maxSleepDuration = time.Minute
const initialSleepDuration = 100 * time.Millisecond
const maxRetryLimit = 30

// ScatterRegionsByRange directly scatter regions by ScatterRegions
func (r *RegionScatterer) ScatterRegionsByRange(startKey, endKey []byte, group string, retryLimit int) (int, map[uint64]error, error) {
	regions := r.cluster.ScanRegions(startKey, endKey, -1)
	if len(regions) < 1 {
		scatterSkipEmptyRegionCounter.Inc()
		return 0, nil, errors.New("empty region")
	}
	failures := make(map[uint64]error, len(regions))
	regionMap := make(map[uint64]*core.RegionInfo, len(regions))
	for _, region := range regions {
		regionMap[region.GetID()] = region
	}
	// If there existed any region failed to relocated after retry, add it into unProcessedRegions
	opsCount, err := r.scatterRegions(regionMap, failures, group, retryLimit)
	if err != nil {
		return 0, nil, err
	}
	return opsCount, failures, nil
}

// ScatterRegionsByID directly scatter regions by ScatterRegions
func (r *RegionScatterer) ScatterRegionsByID(regionsID []uint64, group string, retryLimit int) (int, map[uint64]error, error) {
	if len(regionsID) < 1 {
		scatterSkipEmptyRegionCounter.Inc()
		return 0, nil, errors.New("empty region")
	}
	failures := make(map[uint64]error, len(regionsID))
	regions := make([]*core.RegionInfo, 0, len(regionsID))
	for _, id := range regionsID {
		region := r.cluster.GetRegion(id)
		if region == nil {
			scatterSkipNoRegionCounter.Inc()
			log.Warn("failed to find region during scatter", zap.Uint64("region-id", id))
			failures[id] = errors.New(fmt.Sprintf("failed to find region %v", id))
			continue
		}
		regions = append(regions, region)
	}
	regionMap := make(map[uint64]*core.RegionInfo, len(regions))
	for _, region := range regions {
		regionMap[region.GetID()] = region
	}
	// If there existed any region failed to relocated after retry, add it into unProcessedRegions
	opsCount, err := r.scatterRegions(regionMap, failures, group, retryLimit)
	if err != nil {
		return 0, nil, err
	}
	return opsCount, failures, nil
}

// scatterRegions relocates the regions. If the group is defined, the regions' leader with the same group would be scattered
// in a group level instead of cluster level.
// RetryTimes indicates the retry times if any of the regions failed to relocate during scattering. There will be
// time.Sleep between each retry.
// Failures indicates the regions which are failed to be relocated, the key of the failures indicates the regionID
// and the value of the failures indicates the failure error.
func (r *RegionScatterer) scatterRegions(regions map[uint64]*core.RegionInfo, failures map[uint64]error, group string, retryLimit int) (int, error) {
	if len(regions) < 1 {
		scatterSkipEmptyRegionCounter.Inc()
		return 0, errors.New("empty region")
	}
	if retryLimit > maxRetryLimit {
		retryLimit = maxRetryLimit
	}
	opsCount := 0
	for currentRetry := 0; currentRetry <= retryLimit; currentRetry++ {
		for _, region := range regions {
			op, err := r.Scatter(region, group)
			failpoint.Inject("scatterFail", func() {
				if region.GetID() == 1 {
					err = errors.New("mock error")
				}
			})
			if err != nil {
				failures[region.GetID()] = err
				continue
			}
			delete(regions, region.GetID())
			opsCount++
			if op != nil {
				if ok := r.opController.AddOperator(op); !ok {
					// If there existed any operator failed to be added into Operator Controller, add its regions into unProcessedRegions
					failures[op.RegionID()] = fmt.Errorf("region %v failed to add operator", op.RegionID())
					continue
				}
				failpoint.Inject("scatterHbStreamsDrain", func() {
					r.opController.hbStreams.Drain(1)
					r.opController.RemoveOperator(op)
				})
			}
			delete(failures, region.GetID())
		}
		// all regions have been relocated, break the loop.
		if len(regions) < 1 {
			break
		}
		// Wait for a while if there are some regions failed to be relocated
		time.Sleep(typeutil.MinDuration(maxSleepDuration, time.Duration(math.Pow(2, float64(currentRetry)))*initialSleepDuration))
	}
	return opsCount, nil
}

// Scatter relocates the region. If the group is defined, the regions' leader with the same group would be scattered
// in a group level instead of cluster level.
func (r *RegionScatterer) Scatter(region *core.RegionInfo, group string) (*operator.Operator, error) {
	if !filter.IsRegionReplicated(r.cluster, region) {
		r.cluster.AddSuspectRegions(region.GetID())
		scatterSkipNotReplicatedCounter.Inc()
		log.Warn("region not replicated during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d is not fully replicated", region.GetID())
	}

	if region.GetLeader() == nil {
		scatterSkipNoLeaderCounter.Inc()
		log.Warn("region no leader during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d has no leader", region.GetID())
	}

	if r.cluster.IsRegionHot(region) {
		scatterSkipHotRegionCounter.Inc()
		log.Warn("region too hot during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d is hot", region.GetID())
	}

	return r.scatterRegion(region, group), nil
}

func (r *RegionScatterer) scatterRegion(region *core.RegionInfo, group string) *operator.Operator {
	engineFilter := filter.NewEngineFilter(r.name, filter.NotSpecialEngines)
	ordinaryPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers()))
	specialPeers := make(map[string]map[uint64]*metapb.Peer)
	oldFit := r.cluster.GetRuleManager().FitRegion(r.cluster, region)
	// Group peers by the engine of their stores
	for _, peer := range region.GetPeers() {
		store := r.cluster.GetStore(peer.GetStoreId())
		if store == nil {
			return nil
		}
		if engineFilter.Target(r.cluster.GetOpts(), store).IsOK() {
			ordinaryPeers[peer.GetStoreId()] = peer
		} else {
			engine := store.GetLabelValue(core.EngineKey)
			if _, ok := specialPeers[engine]; !ok {
				specialPeers[engine] = make(map[uint64]*metapb.Peer)
			}
			specialPeers[engine][peer.GetStoreId()] = peer
		}
	}

	sortByWitness := func(arr []*metapb.Peer) {
		sort.Slice(arr, func(i, j int) bool {
			return arr[i].GetIsWitness() && !arr[j].GetIsWitness()
		})
	}

	ordinaryPeersArr := make([]*metapb.Peer, 0, len(region.GetPeers()))
	for _, peer := range ordinaryPeers {
		log.Error("before sort", zap.String("peer", peer.String()))
		ordinaryPeersArr = append(ordinaryPeersArr, peer)
	}
	log.Error("before sort", zap.Int("len(oridianryPeersArr)", len(ordinaryPeersArr)))
	sortByWitness(ordinaryPeersArr)
	for _, peer := range ordinaryPeersArr {
		log.Error("after sort", zap.String("peer", peer.String()))
	}

	targetPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers())) // StoreID -> Peer
	selectedStores := make(map[uint64]struct{}, len(region.GetPeers()))  // selected StoreID set
	leaderCandidateStores := make([]uint64, 0, len(region.GetPeers()))   // StoreID allowed to become Leader
	shouldnotleader := make([]uint64, 0, len(region.GetPeers()))
	scatterWithSameEngine := func(peers map[uint64]*metapb.Peer, peersArr []*metapb.Peer, context engineContext) { // peers: StoreID -> Peer
		for _, peer := range peersArr {
			if _, ok := selectedStores[peer.GetStoreId()]; ok {
				if allowLeader(oldFit, peer) {
					leaderCandidateStores = append(leaderCandidateStores, peer.GetStoreId())
				}
				// It is both sourcePeer and targetPeer itself, no need to select.
				continue
			}
			for {
				log.Error("before selectCandidates", zap.String("peer", peer.String()))
				candidates := r.selectCandidates(region, oldFit, peer.GetStoreId(), peer.GetIsWitness(), selectedStores, context)
				newPeer := r.selectStore(group, region, peer, peer.GetStoreId(), candidates, context)
				targetPeers[newPeer.GetStoreId()] = newPeer
				selectedStores[newPeer.GetStoreId()] = struct{}{}
				// If the selected peer is a peer other than origin peer in this region,
				// it is considered that the selected peer select itself.
				// This origin peer re-selects.
				existPeer, ok := peers[newPeer.GetStoreId()]
				if !ok || peer.GetStoreId() == newPeer.GetStoreId() {
					selectedStores[peer.GetStoreId()] = struct{}{}
					if allowLeader(oldFit, peer) {
						leaderCandidateStores = append(leaderCandidateStores, newPeer.GetStoreId())
					}
					break
				}
				if ok && (!existPeer.GetIsWitness() && peer.GetIsWitness()) {
					if region.GetLeader().GetId() != existPeer.GetId() {
						// let old voter to witness
						newPeer.Id = existPeer.GetId()
						// let old witness to non-witness
						otherNewPeer := &metapb.Peer{
							StoreId:   peer.GetStoreId(),
							Role:      peer.GetRole(),
							IsWitness: false,
							Id:        peer.GetId(),
						}
						targetPeers[peer.GetStoreId()] = otherNewPeer
						shouldnotleader = append(shouldnotleader, newPeer.GetStoreId())
						leaderCandidateStores = append(leaderCandidateStores, peer.GetStoreId())
						log.Error("in scatterRegion", zap.Uint64("region", region.GetID()),
							zap.String("peer", peer.String()),
							zap.String("new_peer", newPeer.String()),
							zap.String("other_new_peer", otherNewPeer.String()))
						break
					} else {
						delete(targetPeers, newPeer.GetStoreId())
					}
				}
			}
		}
	}

	scatterWithSameEngine(ordinaryPeers, ordinaryPeersArr, r.ordinaryEngine)
	deleteSlice := func(original []uint64, toDelete []uint64) []uint64 {
		if len(toDelete) == 0 {
			return original
		}
		result := make([]uint64, 0)
		i, j := 0, 0
		for i < len(original) {
			if original[i] != toDelete[j] {
				result = append(result, original[i])
				i++
			} else {
				// match found, start sub loop
				subI, subJ := i, j
				for subJ < len(toDelete) && subI < len(original) && original[subI] == toDelete[subJ] {
					subI++
					subJ++
				}
				if subJ == len(toDelete) {
					i = subI
					j = 0
				} else {
					result = append(result, original[i])
					i++
					j = 0
				}
			}
		}
		return result
	}

	for _, s := range shouldnotleader {
		log.Error("shouldnot leader sid", zap.Uint64("store_id", s))
	}
	for _, s := range leaderCandidateStores {
		log.Error("before delete sid", zap.Uint64("store_id", s))
	}
	leaderCandidateStores = deleteSlice(leaderCandidateStores, shouldnotleader)
	for _, s := range leaderCandidateStores {
		log.Error("after delete sid", zap.Uint64("store_id", s))
	}

	// FIXME: target leader only considers the ordinary stores, maybe we need to consider the
	// special engine stores if the engine supports to become a leader. But now there is only
	// one engine, tiflash, which does not support the leader, so don't consider it for now.
	targetLeader := r.selectAvailableLeaderStore(group, region, leaderCandidateStores, r.ordinaryEngine)
	if targetLeader == 0 {
		scatterSkipNoLeaderCounter.Inc()
		return nil
	}

	for engine, peers := range specialPeers {
		ctx, ok := r.specialEngines.Load(engine)
		if !ok {
			ctx = newEngineContext(r.ctx, func() filter.Filter {
				return filter.NewEngineFilter(r.name, placement.LabelConstraint{Key: core.EngineKey, Op: placement.In, Values: []string{engine}})
			})
			r.specialEngines.Store(engine, ctx)
		}

		peersArr := make([]*metapb.Peer, 0, len(peers))
		for _, peer := range peers {
			peersArr = append(peersArr, peer)
		}
		scatterWithSameEngine(peers, peersArr, ctx.(engineContext))
	}

	if isSameDistribution(region, targetPeers, targetLeader) {
		scatterUnnecessaryCounter.Inc()
		r.Put(targetPeers, targetLeader, group)
		return nil
	}

	for _, p := range ordinaryPeers {
		log.Error("in scatterRegion orignal", zap.Uint64("region", region.GetID()), zap.Uint64("peer_id", p.GetId()), zap.Uint64("peer_store_id", p.GetStoreId()),
			zap.Bool("is_leader", region.GetLeader().GetId() == p.GetId()), zap.Bool("is_witness", p.GetIsWitness()))
	}
	for _, p := range targetPeers {
		log.Error("in scatterRegion target", zap.Uint64("region", region.GetID()), zap.Uint64("peer_id", p.GetId()), zap.Uint64("peer_store_id", p.GetStoreId()),
			zap.Bool("is_leader", region.GetLeader().GetId() == p.GetId()), zap.Bool("is_witness", p.GetIsWitness()))
	}

	op, err := operator.CreateScatterRegionOperator("scatter-region", r.cluster, region, targetPeers, targetLeader)
	if err != nil {
		scatterFailCounter.Inc()
		for _, peer := range region.GetPeers() {
			targetPeers[peer.GetStoreId()] = peer
		}
		r.Put(targetPeers, region.GetLeader().GetStoreId(), group)
		log.Debug("fail to create scatter region operator", errs.ZapError(err))
		return nil
	}
	if op != nil {
		scatterSuccessCounter.Inc()
		r.Put(targetPeers, targetLeader, group)
		op.SetPriorityLevel(core.High)
	}
	return op
}

func allowLeader(fit *placement.RegionFit, peer *metapb.Peer) bool {
	switch peer.GetRole() {
	case metapb.PeerRole_Learner, metapb.PeerRole_DemotingVoter:
		return false
	}
	if peer.IsWitness {
		return false
	}

	rule := fit.GetRuleFit(peer.GetId()).Rule
	if rule.IsWitness {
		return false
	}
	switch rule.Role {
	case placement.Voter, placement.Leader:
		return true
	}
	return false
}

func isSameDistribution(region *core.RegionInfo, targetPeers map[uint64]*metapb.Peer, targetLeader uint64) bool {
	peers := region.GetPeers()
	for _, peer := range peers {
		if other_peer, ok := targetPeers[peer.GetStoreId()]; !ok || other_peer.GetIsWitness() != peer.GetIsWitness() {
			return false
		}
	}
	return region.GetLeader().GetStoreId() == targetLeader
}

func (r *RegionScatterer) selectCandidates(region *core.RegionInfo, oldFit *placement.RegionFit, sourceStoreID uint64, sourceIsWitness bool, selectedStores map[uint64]struct{}, context engineContext) []uint64 {
	sourceStore := r.cluster.GetStore(sourceStoreID)
	if sourceStore == nil {
		log.Error("failed to get the store", zap.Uint64("store-id", sourceStoreID), errs.ZapError(errs.ErrGetSourceStore))
		return nil
	}
	filters := []filter.Filter{
		filter.NewExcludedFilter(r.name, nil, selectedStores),
	}
	scoreGuard := filter.NewPlacementSafeguard(r.name, r.cluster.GetOpts(), r.cluster.GetBasicCluster(), r.cluster.GetRuleManager(), region, sourceStore, oldFit)
	for _, filterFunc := range context.filterFuncs {
		filters = append(filters, filterFunc())
	}
	filters = append(filters, scoreGuard)
	stores := r.cluster.GetStores()
	candidates := make([]uint64, 0)
	maxStoreTotalCount := uint64(0)
	minStoreTotalCount := uint64(math.MaxUint64)
	if !sourceIsWitness {
		for _, store := range stores {
			count := context.selectedPeer.TotalCountByStore(store.GetID())
			if count > maxStoreTotalCount {
				maxStoreTotalCount = count
			}
			if count < minStoreTotalCount {
				minStoreTotalCount = count
			}
			var peer *metapb.Peer
			for _, p := range region.GetPeers() {
				if p.GetStoreId() == sourceStoreID {
					peer = p
					break
				}
			}
			log.Error("in selectCandidates", zap.Uint64("region", region.GetID()),
				zap.Uint64("peer", peer.GetId()),
				zap.Uint64("store_id", store.GetID()),
				zap.Bool("is_leader", region.GetLeader().GetId() == peer.GetId()),
				zap.Bool("is_witness", peer.GetIsWitness()),
				zap.Uint64("count", count),
				zap.Uint64("maxStoreTotalCount", maxStoreTotalCount),
				zap.Uint64("minStoreTotalCount", minStoreTotalCount))
		}
		for _, store := range stores {
			storeCount := context.selectedPeer.TotalCountByStore(store.GetID())
			// If storeCount is equal to the maxStoreTotalCount, we should skip this store as candidate.
			// If the storeCount are all the same for the whole cluster(maxStoreTotalCount == minStoreTotalCount), any store
			// could be selected as candidate.
			if storeCount < maxStoreTotalCount || maxStoreTotalCount == minStoreTotalCount {
				if filter.Target(r.cluster.GetOpts(), store, filters) {
					candidates = append(candidates, store.GetID())
				}
			}
		}
		for _, cand := range candidates {
			log.Error("in selectCandidates", zap.Uint64("region", region.GetID()),
				zap.Uint64("cand", cand))
		}
	} else {
		for _, store := range stores {
			count := context.selectedWitness.TotalCountByStore(store.GetID())
			if count > maxStoreTotalCount {
				maxStoreTotalCount = count
			}
			if count < minStoreTotalCount {
				minStoreTotalCount = count
			}
			var peer *metapb.Peer
			for _, p := range region.GetPeers() {
				if p.GetStoreId() == sourceStoreID {
					peer = p
					break
				}
			}
			log.Error("in selectCandidates witness", zap.Uint64("region", region.GetID()),
				zap.Uint64("peer", peer.GetId()),
				zap.Uint64("store_id", store.GetID()),
				zap.Bool("is_leader", region.GetLeader().GetId() == peer.GetId()),
				zap.Bool("is_witness", peer.GetIsWitness()),
				zap.Uint64("count", count),
				zap.Uint64("maxStoreTotalCount", maxStoreTotalCount),
				zap.Uint64("minStoreTotalCount", minStoreTotalCount))
		}
		for _, store := range stores {
			storeCount := context.selectedWitness.TotalCountByStore(store.GetID())
			// If storeCount is equal to the maxStoreTotalCount, we should skip this store as candidate.
			// If the storeCount are all the same for the whole cluster(maxStoreTotalCount == minStoreTotalCount), any store
			// could be selected as candidate.
			if storeCount < maxStoreTotalCount || maxStoreTotalCount == minStoreTotalCount {
				if filter.Target(r.cluster.GetOpts(), store, filters) {
					candidates = append(candidates, store.GetID())
				}
			}
		}
		for _, cand := range candidates {
			log.Error("in selectCandidates witness", zap.Uint64("region", region.GetID()),
				zap.Uint64("cand", cand))
		}
	}
	return candidates
}

func (r *RegionScatterer) selectStore(group string, region *core.RegionInfo, peer *metapb.Peer, sourceStoreID uint64, candidates []uint64, context engineContext) *metapb.Peer {
	if len(candidates) < 1 {
		return peer
	}
	var newPeer *metapb.Peer
	minCount := uint64(math.MaxUint64)
	if !peer.GetIsWitness() {
		for _, storeID := range candidates {
			count := context.selectedPeer.Get(storeID, group)
			if count < minCount {
				minCount = count
				newPeer = &metapb.Peer{
					StoreId: storeID,
					Role:    peer.GetRole(),
				}
				log.Error("in selectStore", zap.Uint64("region", region.GetID()), zap.Uint64("peer", peer.GetId()),
					zap.Bool("is_leader", region.GetLeader().GetId() == peer.GetId()),
					zap.Bool("is_witness", peer.GetIsWitness()),
					zap.Uint64("sourceStoreID", sourceStoreID), zap.Uint64("newStoreID", storeID))
			}
		}
	} else {
		for _, storeID := range candidates {
			count := context.selectedWitness.Get(storeID, group)
			if count < minCount {
				minCount = count
				newPeer = &metapb.Peer{
					StoreId:   storeID,
					Role:      peer.GetRole(),
					IsWitness: true,
				}
				log.Error("in selectStore witness", zap.Uint64("region", region.GetID()), zap.Uint64("peer", peer.GetId()),
					zap.Bool("is_leader", region.GetLeader().GetId() == peer.GetId()),
					zap.Bool("is_witness", peer.GetIsWitness()),
					zap.Uint64("sourceStoreID", sourceStoreID), zap.Uint64("newStoreID", storeID))
			}
		}
	}
	// if the source store have the least count, we don't need to scatter this peer
	for _, storeID := range candidates {
		if !peer.GetIsWitness() {
			if storeID == sourceStoreID && context.selectedPeer.Get(sourceStoreID, group) <= minCount {
				log.Error("in selectStore no need to scatter", zap.Uint64("region", region.GetID()), zap.Uint64("peer", peer.GetId()),
					zap.Bool("is_leader", region.GetLeader().GetId() == peer.GetId()),
					zap.Bool("is_witness", peer.GetIsWitness()),
					zap.Uint64("sourceStoreID", sourceStoreID))
				return peer
			}
		} else {
			if storeID == sourceStoreID && context.selectedWitness.Get(sourceStoreID, group) <= minCount {
				log.Error("in selectStore witness no need to scatter", zap.Uint64("region", region.GetID()), zap.Uint64("peer", peer.GetId()),
					zap.Bool("is_leader", region.GetLeader().GetId() == peer.GetId()),
					zap.Bool("is_witness", peer.GetIsWitness()),
					zap.Uint64("sourceStoreID", sourceStoreID))
				return peer
			}
		}
	}
	if newPeer == nil {
		return peer
	}
	return newPeer
}

// selectAvailableLeaderStore select the target leader store from the candidates. The candidates would be collected by
// the existed peers store depended on the leader counts in the group level. Please use this func before scatter spacial engines.
func (r *RegionScatterer) selectAvailableLeaderStore(group string, region *core.RegionInfo, leaderCandidateStores []uint64, context engineContext) uint64 {
	sourceStore := r.cluster.GetStore(region.GetLeader().GetStoreId())
	if sourceStore == nil {
		log.Error("failed to get the store", zap.Uint64("store-id", region.GetLeader().GetStoreId()), errs.ZapError(errs.ErrGetSourceStore))
		return 0
	}
	minStoreGroupLeader := uint64(math.MaxUint64)
	id := uint64(0)
	for _, storeID := range leaderCandidateStores {
		store := r.cluster.GetStore(storeID)
		if store == nil {
			continue
		}
		log.Error("In selectAvailableLeaderStore", zap.Uint64("region", region.GetID()), zap.Uint64("cand", storeID))
		storeGroupLeaderCount := context.selectedLeader.Get(storeID, group)
		if minStoreGroupLeader > storeGroupLeaderCount {
			minStoreGroupLeader = storeGroupLeaderCount
			id = storeID
		}
	}
	log.Error("In selectAvailableLeaderStore", zap.Uint64("region", region.GetID()), zap.Uint64("choose", id))
	return id
}

// Put put the final distribution in the context no matter the operator was created
func (r *RegionScatterer) Put(peers map[uint64]*metapb.Peer, leaderStoreID uint64, group string) {
	engineFilter := filter.NewEngineFilter(r.name, filter.NotSpecialEngines)
	// Group peers by the engine of their stores
	for _, peer := range peers {
		storeID := peer.GetStoreId()
		store := r.cluster.GetStore(storeID)
		if store == nil {
			continue
		}
		if engineFilter.Target(r.cluster.GetOpts(), store).IsOK() {
			r.ordinaryEngine.selectedPeer.Put(storeID, group)
			if peer.GetIsWitness() {
				r.ordinaryEngine.selectedWitness.Put(storeID, group)
			}
			scatterDistributionCounter.WithLabelValues(
				fmt.Sprintf("%v", storeID),
				fmt.Sprintf("%v", false),
				core.EngineTiKV).Inc()
		} else {
			engine := store.GetLabelValue(core.EngineKey)
			ctx, _ := r.specialEngines.Load(engine)
			ctx.(engineContext).selectedPeer.Put(storeID, group)
			scatterDistributionCounter.WithLabelValues(
				fmt.Sprintf("%v", storeID),
				fmt.Sprintf("%v", false),
				engine).Inc()
		}
	}
	r.ordinaryEngine.selectedLeader.Put(leaderStoreID, group)
	scatterDistributionCounter.WithLabelValues(
		fmt.Sprintf("%v", leaderStoreID),
		fmt.Sprintf("%v", true),
		core.EngineTiKV).Inc()
}
