// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"context"
	"os"
	"time"

	"go.uber.org/zap"
)

const (
	leadershipPollInterval = 5 * time.Second
	leadershipOpTimeout    = 3 * time.Second
	stewardLeaderKey       = "/steward/leader"
)

// watchLeadership periodically polls the etcd cluster status to detect leadership changes.
// When this member becomes the leader, it writes its pod name to the /steward/leader key
// so that the etcd-steward can discover the current leader.
func (a *Application) watchLeadership(ctx context.Context) {
	ticker := time.NewTicker(leadershipPollInterval)
	defer ticker.Stop()

	var lastLeader uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			statusCtx, cancel := context.WithTimeout(ctx, leadershipOpTimeout)
			resp, err := a.etcdClient.Status(statusCtx, a.etcdClient.Endpoints()[0])
			cancel()
			if err != nil {
				a.logger.Warn("failed to get etcd status for leadership watch", zap.Error(err))
				continue
			}

			if resp.Leader != lastLeader {
				lastLeader = resp.Leader
				podName := os.Getenv("POD_NAME")
				if resp.Leader == resp.Header.MemberId && podName != "" {
					putCtx, putCancel := context.WithTimeout(ctx, leadershipOpTimeout)
					_, err := a.etcdClient.Put(putCtx, stewardLeaderKey, podName)
					putCancel()
					if err != nil {
						a.logger.Error("failed to write leader key to etcd", zap.Error(err))
					} else {
						a.logger.Info("wrote leader key to etcd", zap.String("podName", podName))
					}
				}
			}
		}
	}
}
