/*
 * TgMusicBot - Telegram Music Bot
 *  Copyright (c) 2025-2026 Team Arc
 *
 *  Licensed under GNU GPL v3
 *  See https://github.com/AshokShau/TgMusicBot
 */

package dl

// arcmusic.go — ArcMusic API downloader with DB cache.
//
// Mirrors _api.py exactly:
//   Cache.fetch_id  → fetch message_id from arcapi.medias using "videoID.mp3" / "videoID.mp4"
//   Cache.get_track → download from DL_LOGGER channel using the message_id
//   API.create_job  → POST /youtube/v2/download → job_id  (3 retries)
//   API.get_url     → GET  /youtube/jobStatus   → public_url  (10 polls × 3s)
//   API.save_file   → stream URL to downloads/<filename>
//   API.download    → in-memory cache → DB cache → Arc API (2 cycles)

import (
	"ashokshau/tgmusic/config"
	"ashokshau/tgmusic/src/core/db"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ── In-memory session caches (mirrors _api.py self.dl_cache / self.v_cache) ──

var (
	dlCacheMu sync.RWMutex
	dlCache   = map[string]string{} // videoID → local file path  (audio)
	vCacheMu  sync.RWMutex
	vCache    = map[string]string{} // videoID → local file path  (video)
)

func getCached(videoID string, isVideo bool) (string, bool) {
	if isVideo {
		vCacheMu.RLock()
		p, ok := vCache[videoID]
		vCacheMu.RUnlock()
		return p, ok
	}
	dlCacheMu.RLock()
	p, ok := dlCache[videoID]
	dlCacheMu.RUnlock()
	return p, ok
}

func setCached(videoID, path string, isVideo bool) {
	if isVideo {
		vCacheMu.Lock()
		vCache[videoID] = path
		vCacheMu.Unlock()
		return
	}
	dlCacheMu.Lock()
	dlCache[videoID] = path
	dlCacheMu.Unlock()
}

// ClearMemCache wipes the in-memory download caches (called by the 12-hour cleaner).
func ClearMemCache() {
	dlCacheMu.Lock()
	dlCache = map[string]string{}
	dlCacheMu.Unlock()

	vCacheMu.Lock()
	vCache = map[string]string{}
	vCacheMu.Unlock()
}

// ── API response shapes ───────────────────────────────────────────────────────

type arcJobResponse struct {
	Status string `json:"status"`
	JobID  string `json:"job_id"`
}

type arcStatusResponse struct {
	Status string `json:"status"`
	Job    struct {
		Status string `json:"status"`
		Result struct {
			PublicURL string `json:"public_url"`
		} `json:"result"`
	} `json:"job"`
}

// ── Step 1: create_job (mirrors API.create_job) ───────────────────────────────

func arcCreateJob(apiURL, apiKey, videoID string, isVideo bool) (string, error) {
	endpoint := strings.TrimRight(apiURL, "/") + "/youtube/v2/download"
	params := url.Values{
		"api_key": {apiKey},
		"query":   {videoID},
		"isVideo": {strings.ToLower(fmt.Sprintf("%v", isVideo))},
	}
	fullURL := endpoint + "?" + params.Encode()

	for attempt := 0; attempt < 3; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
		if err != nil {
			cancel()
			time.Sleep(time.Second)
			continue
		}

		resp, err := client.Do(req)
		cancel()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			time.Sleep(time.Second)
			continue
		}

		var data arcJobResponse
		if err = json.Unmarshal(body, &data); err != nil {
			time.Sleep(time.Second)
			continue
		}
		if data.Status != "queued" || data.JobID == "" {
			time.Sleep(time.Second)
			continue
		}

		return data.JobID, nil
	}
	return "", fmt.Errorf("[ArcMusic] create_job failed for %s", videoID)
}

// ── Step 2: get_url (mirrors API.get_url, 10 retries × 3s) ──────────────────

func arcGetURL(apiURL, jobID string) (string, error) {
	endpoint := strings.TrimRight(apiURL, "/") + "/youtube/jobStatus"
	fullURL := endpoint + "?" + url.Values{"job_id": {jobID}}.Encode()
	baseURL := strings.TrimRight(apiURL, "/")

	for attempt := 1; attempt <= 10; attempt++ {
		time.Sleep(3 * time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
		if err != nil {
			cancel()
			continue
		}

		resp, err := client.Do(req)
		cancel()
		if err != nil {
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			continue
		}

		var data arcStatusResponse
		if err = json.Unmarshal(body, &data); err != nil {
			continue
		}

		if data.Status != "success" || data.Job.Status != "done" {
			continue
		}

		publicURL := data.Job.Result.PublicURL
		if publicURL == "" {
			break
		}

		// Mirrors: return self.api_url + _url
		if strings.HasPrefix(publicURL, "/") {
			publicURL = baseURL + publicURL
		}

		slog.Info(fmt.Sprintf("[ArcMusic] Received #%d [%s]", attempt, publicURL))
		return publicURL, nil
	}

	return "", fmt.Errorf("[ArcMusic] get_url exhausted retries for job %s", jobID)
}

// ── Step 3: save_file (mirrors API.save_file) ─────────────────────────────────
// Saves to downloads/<filename_from_url> — exactly as in _api.py.

func arcSaveFile(dlURL string) (string, error) {
	// Extract filename from URL tail: url.split("/")[-1]
	rawName := dlURL
	if idx := strings.LastIndex(rawName, "?"); idx != -1 {
		rawName = rawName[:idx]
	}
	fname := filepath.Base(rawName)
	if fname == "" || fname == "." || fname == "/" {
		fname = "track_" + fmt.Sprintf("%d", time.Now().UnixMilli())
	}

	if err := os.MkdirAll(config.DownloadsDir, 0755); err != nil {
		return "", fmt.Errorf("mkdir downloads: %w", err)
	}
	fpath := filepath.Join(config.DownloadsDir, fname)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dlURL, nil)
	if err != nil {
		return "", fmt.Errorf("save_file request error: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("save_file download failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("save_file HTTP %d", resp.StatusCode)
	}

	f, err := os.Create(fpath)
	if err != nil {
		return "", fmt.Errorf("save_file create: %w", err)
	}

	const chunkLimit = 1024 * 1024
	buf := make([]byte, chunkLimit)
	if _, err = io.CopyBuffer(f, resp.Body, buf); err != nil {
		_ = f.Close()
		_ = os.Remove(fpath)
		return "", fmt.Errorf("save_file write: %w", err)
	}
	_ = f.Close()

	info, _ := os.Stat(fpath)
	if info == nil || info.Size() == 0 {
		_ = os.Remove(fpath)
		return "", fmt.Errorf("save_file: empty file at %s", fpath)
	}

	slog.Info("[ArcMusic] save_file: saved", "path", fpath)
	return fpath, nil
}

// ── DB cache (mirrors Cache.fetch_id + Cache.get_track) ──────────────────────
// track_id in DB is "videoID.mp3" or "videoID.mp4" — exactly as in _api.py.
// DB downloading requires DL_BOT_TOKEN — only DlBot has access to the private
// media channel. If DlBot is not configured, this step is skipped entirely.

func dbGetTrack(videoID string, isVideo bool) (string, error) {
	if DlBot == nil {
		return "", fmt.Errorf("[MediaDB] DL_BOT_TOKEN not configured")
	}
	if db.Instance == nil {
		return "", fmt.Errorf("[MediaDB] not initialised")
	}
	if config.MediaChannelId == 0 {
		return "", fmt.Errorf("[MediaDB] MEDIA_CHANNEL_ID not set")
	}
	if config.DbUri == "" {
		return "", fmt.Errorf("[MediaDB] DB_URI not set")
	}

	// Build track_id key exactly as _api.py: f"{video_id}.mp4" / f"{video_id}.mp3"
	ext := "mp3"
	if isVideo {
		ext = "mp4"
	}
	trackID := fmt.Sprintf("%s.%s", videoID, ext)

	msgID := db.Instance.GetMediaMsgID(trackID, isVideo)
	if msgID == 0 {
		return "", fmt.Errorf("[MediaDB] no entry for %s", trackID)
	}

	// Mirrors: app.get_messages(chat_id=config.DL_LOGGER, message_ids=int(m_id))
	// Uses DlBot (DL_BOT_TOKEN) — the bot that has access to the private media channel.
	msgInfo, err := DlBot.GetMessageLinkInfo(msgURL)
	if err != nil || msgInfo.Message == nil {
		return "", fmt.Errorf("[MediaDB] GetMessageLinkInfo failed: %w", err)
	}

	file, err := msgInfo.Message.Download(DlBot, 1, 0, 0, true)
	if err != nil || file == nil || file.Local == nil || !file.Local.IsDownloadingCompleted {
		return "", fmt.Errorf("[MediaDB] file download failed: %w", err)
	}

	slog.Info("[MediaDB] Retrieved from channel cache", "video_id", videoID, "msg_id", msgID)
	return file.Local.Path, nil
}

// ── Public entry point (mirrors API.download) ─────────────────────────────────
//
// Priority:
//   1. In-memory session cache  (self.dl_cache / self.v_cache)
//   2. Telegram DB channel cache (Cache.get_track via DB_URI + MEDIA_CHANNEL_ID)
//   3. ArcMusic API             (create_job → get_url → save_file, 2 cycles)

func ArcMusicDownload(videoID string, isVideo bool) (string, error) {
	// 1. In-memory cache
	if p, ok := getCached(videoID, isVideo); ok {
		slog.Info("[ArcMusic] In-memory cache hit", "video_id", videoID)
		return p, nil
	}

	// 2. DB channel cache — requires DL_BOT_TOKEN + MEDIA_CHANNEL_ID + DB_URI
	if DlBot != nil && config.MediaChannelId != 0 && config.DbUri != "" {
		if fpath, err := dbGetTrack(videoID, isVideo); err == nil {
			slog.Info("[ArcMusic] DB channel cache hit", "video_id", videoID)
			setCached(videoID, fpath, isVideo)
			return fpath, nil
		} else {
			slog.Debug("[ArcMusic] DB cache miss", "video_id", videoID, "reason", err)
		}
	}

	// 3. ArcMusic API — 2 cycles (mirrors for attempt in range(2))
	apiURL := config.ApiUrl
	apiKey := config.ApiKey
	if apiURL == "" || apiKey == "" {
		return "", fmt.Errorf("[ArcMusic] API_URL or API_KEY not configured")
	}

	for cycle := 0; cycle < 2; cycle++ {
		jobID, err := arcCreateJob(apiURL, apiKey, videoID, isVideo)
		if err != nil {
			if cycle == 0 {
				time.Sleep(2 * time.Second)
			}
			continue
		}

		dlURL, err := arcGetURL(apiURL, jobID)
		if err != nil {
			if cycle == 0 {
				time.Sleep(2 * time.Second)
			}
			continue
		}

		fpath, err := arcSaveFile(dlURL)
		if err != nil {
			slog.Error("[ArcMusic] save_file failed", "cycle", cycle+1, "error", err)
			if cycle == 0 {
				time.Sleep(2 * time.Second)
			}
			continue
		}

		setCached(videoID, fpath, isVideo)
		return fpath, nil
	}

	return "", fmt.Errorf("[ArcMusic] all download strategies failed for %s", videoID)
}
