package edgebound

import (
	"net"
	"time"

	"github.com/singchia/frontier/pkg/frontier/apis"
	"github.com/singchia/frontier/pkg/frontier/repo/model"
	"github.com/singchia/frontier/pkg/frontier/repo/query"
	"github.com/singchia/geminio"
	"github.com/singchia/geminio/delegate"
	"k8s.io/klog/v2"
)

func (em *edgeManager) online(end geminio.End) error {
	// TODO transaction
	// cache
	em.mtx.Lock()
	old, ok := em.edges[end.ClientID()]
	if ok {
		klog.Warningf("edge online, old end exists, edgeID: %d", end.ClientID())
		// 踢旧端同步化：同一把写锁内原子完成旧端本地逐出与新 end 上位，
		// 不再等待 offline 通知链完成。依据：死链旧 end 的 ConnOffline 可能
		// 永不到达（其包处理循环已死，Close 不会触发 delegate 回调），
		// 等待链在僵尸场景下永久失效，重连将持续撞上僵尸 end（churn 自持）。
		// 迟到的旧端 offline 因 addr 不匹配走无害分支，不会逐出新 end。
		delete(em.edges, end.ClientID())
	}
	em.edges[end.ClientID()] = end
	if em.informer != nil {
		em.informer.SetEdgeCount(len(em.edges))
	}
	em.mtx.Unlock()

	if ok {
		oldend := old.(geminio.End)
		if err := oldend.Close(); err != nil {
			klog.Warningf("edge online, kick off old end err: %s, edgeID: %d", err, end.ClientID())
		}
		// memdb：清理旧端残留的 RPC 注册与旧行，防止 stale 路由和 sqlite 后端下的主键冲突。
		// 清理失败仅记录不阻断——不应因清理失败拒绝新端上线。
		if err := em.repo.DeleteEdge(&query.EdgeDelete{
			EdgeID: end.ClientID(),
			Addr:   oldend.RemoteAddr().String(),
		}); err != nil {
			klog.Errorf("edge online, repo delete old edge err: %s, edgeID: %d", err, end.ClientID())
		}
		if err := em.repo.DeleteEdgeRPCs(end.ClientID()); err != nil {
			klog.Errorf("edge online, repo delete edge rpcs err: %s, edgeID: %d", err, end.ClientID())
		}
	}

	// memdb
	edge := &model.Edge{
		EdgeID:     end.ClientID(),
		Meta:       string(end.Meta()),
		Addr:       end.RemoteAddr().String(),
		CreateTime: time.Now().Unix(),
	}
	if err := em.repo.CreateEdge(edge); err != nil {
		klog.Errorf("edge online, repo create err: %s, edgeID: %d", err, end.ClientID())
		return err
	}

	// inform others
	if em.informer != nil {
		em.informer.EdgeOnline(end.ClientID(), end.Meta(), end.RemoteAddr())
	}

	return nil
}

func (em *edgeManager) offline(edgeID uint64, meta []byte, addr net.Addr) error {
	// TODO transaction
	// cache
	em.mtx.Lock()
	value, ok := em.edges[edgeID]
	if ok {
		end := value.(geminio.End)
		if end.RemoteAddr().String() == addr.String() {
			delete(em.edges, edgeID)
			klog.V(2).Infof("edge offline, edgeID: %d, remote addr: %s", edgeID, end.RemoteAddr().String())
		} else {
			// same edgeID but different connection addr
			klog.V(1).Infof("edge offline, edgeID: %d, remote addr: %s, offline addr: %s", edgeID, end.RemoteAddr(), addr.String())
			em.mtx.Unlock()
			return nil
		}
	} else {
		klog.Warningf("edge offline, edgeID: %d not found in cache", edgeID)
	}
	if em.informer != nil {
		// TODO merge events
		em.informer.SetEdgeCount(len(em.edges))
	}
	em.mtx.Unlock()

	// memdb
	if err := em.repo.DeleteEdge(&query.EdgeDelete{
		EdgeID: edgeID,
		Addr:   addr.String(),
	}); err != nil {
		klog.Errorf("edge offline, repo delete edge err: %s, edgeID: %d", err, edgeID)
		return err
	}
	if err := em.repo.DeleteEdgeRPCs(edgeID); err != nil {
		klog.Errorf("edge offline, repo delete edge rpcs err: %s, edgeID: %d", err, edgeID)
		return err
	}

	// inform others
	if em.informer != nil {
		em.informer.EdgeOffline(edgeID, meta, addr)
	}
	// exchange to service
	if em.exchange != nil {
		em.exchange.EdgeOffline(edgeID, meta, addr)
	}
	return nil
}

// delegations for all ends from edgebound, called by geminio
func (em *edgeManager) ConnOnline(d delegate.ConnDescriber) error {
	edgeID := d.ClientID()
	meta := d.Meta()
	addr := d.RemoteAddr()

	// exchange to service
	if em.exchange != nil {
		err := em.exchange.EdgeOnline(edgeID, meta, addr)
		if err != nil && err != apis.ErrServiceNotOnline {
			return err
		}
	}
	klog.V(2).Infof("edge online, edgeID: %d, meta: %s, addr: %s", edgeID, string(meta), addr)
	return nil
}

func (em *edgeManager) ConnOffline(d delegate.ConnDescriber) error {
	edgeID := d.ClientID()
	meta := d.Meta()
	addr := d.RemoteAddr()

	klog.V(2).Infof("edge offline, edgeID: %d, meta: %s, addr: %s", edgeID, string(meta), addr)
	// offline the cache
	err := em.offline(edgeID, meta, addr)
	if err != nil {
		klog.Errorf("edge offline, cache or db offline err: %s, edgeID: %d, meta: %s, addr: %s",
			err, edgeID, string(meta), addr)
		return err
	}
	return nil
}

func (em *edgeManager) Heartbeat(d delegate.ConnDescriber) error {
	edgeID := d.ClientID()
	meta := string(d.Meta())
	addr := d.RemoteAddr()
	klog.V(3).Infof("edge heartbeat, edgeID: %d, meta: %s, addr: %s", edgeID, string(meta), addr)
	if em.informer != nil {
		em.informer.EdgeHeartbeat(edgeID, d.Meta(), addr)
	}
	return nil
}

func (em *edgeManager) RemoteRegistration(rpc string, edgeID, streamID uint64) {
	klog.V(3).Infof("edge remote rpc registration, rpc: %s, edgeID: %d, streamID: %d", rpc, edgeID, streamID)

	// memdb
	er := &model.EdgeRPC{
		RPC:        rpc,
		EdgeID:     edgeID,
		CreateTime: time.Now().Unix(),
	}
	err := em.repo.CreateEdgeRPC(er)
	if err != nil {
		klog.Errorf("edge remote registration, create edge rpc err: %s, rpc: %s, edgeID: %d, streamID: %d", err, rpc, edgeID, streamID)
	}
}

func (em *edgeManager) GetClientID(_ uint64, meta []byte) (uint64, error) {
	var (
		edgeID uint64
		err    error
	)
	if em.exchange != nil {
		edgeID, err = em.exchange.GetEdgeID(meta)
		if err == nil {
			klog.V(2).Infof("edge get edgeID: %d from exchange, meta: %s", edgeID, string(meta))
			return edgeID, nil
		}
	}

	if (err == apis.ErrServiceNotOnline || err == apis.ErrRPCNotOnline) && em.conf.Edgebound.EdgeIDAllocWhenNoIDServiceOn {
		edgeID = em.idFactory.GetID()
		klog.V(2).Infof("edge get edgeID: %d, meta: %s, after no ID acquired from exchange", edgeID, string(meta))
		return em.idFactory.GetID(), nil
	}
	return 0, err
}
