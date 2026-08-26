package edgebound

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/singchia/frontier/pkg/frontier/config"
	"github.com/singchia/frontier/pkg/frontier/repo"
	"github.com/singchia/geminio"
)

// fakeAddr 实现 net.Addr，仅用于测试
type fakeAddr struct {
	network string
	addr    string
}

func (fa *fakeAddr) Network() string { return fa.network }
func (fa *fakeAddr) String() string  { return fa.addr }

// fakeEnd 仅实现 online/offline 路径用到的 End 方法，
// 其余接口嵌 nil 占位（测试内不会被调用）
type fakeEnd struct {
	geminio.Stream
	geminio.Multiplexer
	net.Listener

	id     uint64
	addr   net.Addr
	closed int32
}

func (fe *fakeEnd) ClientID() uint64    { return fe.id }
func (fe *fakeEnd) Meta() []byte        { return nil }
func (fe *fakeEnd) RemoteAddr() net.Addr { return fe.addr }
func (fe *fakeEnd) Close() error {
	atomic.StoreInt32(&fe.closed, 1)
	return nil
}

func newTestEdgeManager(t *testing.T) *edgeManager {
	t.Helper()
	conf := &config.Configuration{}
	rp, err := repo.NewRepo(conf)
	if err != nil {
		t.Fatalf("new repo err: %s", err)
	}
	em := &edgeManager{
		conf:  conf,
		edges: make(map[uint64]geminio.End),
		repo:  rp,
	}
	return em
}

// 僵尸场景回归（ongrid #128/#100）：旧 end 的 offline 通知永不到达
// （死链上 ConnOffline 不触发），新 end 必须即时上位而不是永久等待。
// 旧实现阻塞在 <-sync.C()，本测试以超时判失败。
func TestOnlineEvictsDeadOldEndImmediately(t *testing.T) {
	em := newTestEdgeManager(t)
	oldAddr := &fakeAddr{network: "tcp", addr: "192.168.0.240:49068"}
	newAddr := &fakeAddr{network: "tcp", addr: "192.168.0.240:42562"}
	oldEnd := &fakeEnd{id: 9, addr: oldAddr}
	newEnd := &fakeEnd{id: 9, addr: newAddr}
	em.edges[9] = oldEnd

	done := make(chan error, 1)
	go func() {
		done <- em.online(newEnd)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("online err: %s", err)
		}
		cur, ok := em.edges[9]
		if !ok || cur != newEnd {
			t.Fatalf("new end 未即时上位，map 现值: %v", cur)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("online 阻塞超时：旧 end offline 永不到达时新 end 未即时上位（僵尸回归）")
	}
}

// 正常踢端：新 end 上位后旧 end 必须被 Close
func TestOnlineClosesOldEnd(t *testing.T) {
	em := newTestEdgeManager(t)
	oldEnd := &fakeEnd{id: 9, addr: &fakeAddr{network: "tcp", addr: "1.1.1.1:1"}}
	newEnd := &fakeEnd{id: 9, addr: &fakeAddr{network: "tcp", addr: "2.2.2.2:2"}}
	em.edges[9] = oldEnd

	if err := em.online(newEnd); err != nil {
		t.Fatalf("online err: %s", err)
	}
	if atomic.LoadInt32(&oldEnd.closed) != 1 {
		t.Fatal("旧 end 未被 Close")
	}
	if cur := em.edges[9]; cur != newEnd {
		t.Fatal("map 未指向新 end")
	}
}

// 迟到的旧 end offline 必须无害：不逐出新 end（addr 不匹配分支）
func TestLateOldOfflineIsHarmless(t *testing.T) {
	em := newTestEdgeManager(t)
	oldAddr := &fakeAddr{network: "tcp", addr: "1.1.1.1:1"}
	newEnd := &fakeEnd{id: 9, addr: &fakeAddr{network: "tcp", addr: "2.2.2.2:2"}}
	em.edges[9] = newEnd

	// 踢端后旧 end 的 offline 迟到（带旧 addr）
	if err := em.offline(9, nil, oldAddr); err != nil {
		t.Fatalf("late offline err: %s", err)
	}
	if cur := em.edges[9]; cur != newEnd {
		t.Fatal("迟到的旧 offline 逐出了新 end（有害行为）")
	}
}

// 首次上线（无旧 end）：行为不变
func TestOnlineFirstConnect(t *testing.T) {
	em := newTestEdgeManager(t)
	end := &fakeEnd{id: 9, addr: &fakeAddr{network: "tcp", addr: "1.1.1.1:1"}}
	if err := em.online(end); err != nil {
		t.Fatalf("online err: %s", err)
	}
	if cur := em.edges[9]; cur != end {
		t.Fatal("map 未指向新 end")
	}
}
