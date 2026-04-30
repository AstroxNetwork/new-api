package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"

	"github.com/bytedance/gopkg/util/gopool"
)

const (
	topUpCountTaskRunHour = 1
)

var (
	topUpCountTaskOnce    sync.Once
	topUpCountTaskRunning atomic.Bool
)

type feishuTextMessage struct {
	MsgType string `json:"msg_type"`
	Content struct {
		Text string `json:"text"`
	} `json:"content"`
}

type feishuWebhookResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func StartTopUpCountTask(feishuWebhookURL string) {
	topUpCountTaskOnce.Do(func() {
		if !common.IsMasterNode {
			return
		}
		gopool.Go(func() {
			logger.LogInfo(context.Background(), fmt.Sprintf("topup daily count task started: run_at=%02d:00 local_time", topUpCountTaskRunHour))
			for {
				sleepDuration := durationUntilNextDailyRun(topUpCountTaskRunHour)
				time.Sleep(sleepDuration)
				runTopUpCountTaskOnce(feishuWebhookURL)
			}
		})
	})
}

func runTopUpCountTaskOnce(feishuWebhookURL string) {
	if !topUpCountTaskRunning.CompareAndSwap(false, true) {
		return
	}
	defer topUpCountTaskRunning.Store(false)

	ctx := context.Background()
	startTime, endTime := getYesterdayTimeRange()
	count, err := model.CountTopUpsByCreateTimeRange(startTime, endTime)
	if err != nil {
		logger.LogWarn(ctx, fmt.Sprintf("topup daily count task failed: start=%d end=%d err=%v", startTime, endTime, err))
		return
	}

	logger.LogInfo(ctx, fmt.Sprintf("topup daily count task result: start=%d end=%d count=%d", startTime, endTime, count))
	if err := sendTopUpCountToFeishu(feishuWebhookURL, startTime, endTime, count); err != nil {
		logger.LogWarn(ctx, fmt.Sprintf("topup daily count task send feishu failed: %v", err))
	}
}

func durationUntilNextDailyRun(runHour int) time.Duration {
	now := time.Now()
	nextRun := time.Date(now.Year(), now.Month(), now.Day(), runHour, 0, 0, 0, now.Location())
	if !nextRun.After(now) {
		nextRun = nextRun.Add(24 * time.Hour)
	}
	return nextRun.Sub(now)
}

func getYesterdayTimeRange() (int64, int64) {
	now := time.Now()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	yesterdayStart := todayStart.Add(-24 * time.Hour)
	yesterdayEnd := todayStart.Add(-time.Second)
	return yesterdayStart.Unix(), yesterdayEnd.Unix()
}

func sendTopUpCountToFeishu(webhookURL string, startTime int64, endTime int64, count int64) error {
	if webhookURL == "" {
		return nil
	}

	start := time.Unix(startTime, 0).Format("2006-01-02 15:04:05")
	end := time.Unix(endTime, 0).Format("2006-01-02 15:04:05")
	msg := feishuTextMessage{
		MsgType: "text",
	}
	msg.Content.Text = fmt.Sprintf("时间范围: %s ~ %s\n订单: %d", start, end, count)

	body, err := common.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal feishu payload failed: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, webhookURL, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("create feishu request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := GetHttpClient()
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send feishu request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read feishu response failed: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("feishu http status=%d body=%s", resp.StatusCode, string(respBody))
	}

	var feishuResp feishuWebhookResponse
	if err := common.Unmarshal(respBody, &feishuResp); err == nil {
		if feishuResp.Code != 0 {
			return fmt.Errorf("feishu response code=%d msg=%s", feishuResp.Code, feishuResp.Msg)
		}
	}
	return nil
}
