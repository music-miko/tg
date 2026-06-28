/*
 * TgMusicBot - Telegram Music Bot
 *  Copyright (c) 2025-2026 Team Arc
 *
 *  Licensed under GNU GPL v3
 *  See https://github.com/AshokShau/TgMusicBot
 */

package dl

// cleanup.go — Periodic media file cleaner (runs every 12 hours).
//
// Cleans two directories:
//
//   config.DownloadsDir       — yt-dlp, Arc API, Spotify, HTTP direct downloads
//   <DlBotDbDir>/files/       — where TDLib saves files downloaded from the
//                               Telegram media channel (set by main.go at startup)
//
// Only files matching the media extension whitelist are removed.
// TDLib session files (td.bin, td.binlog, *.key, db.sqlite) are never touched.

import (
	"ashokshau/tgmusic/config"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const cleanupInterval = 12 * time.Hour

// mediaExtensions is the whitelist of extensions produced by all downloaders.
var mediaExtensions = map[string]bool{
	".mp3":       true,
	".mp4":       true,
	".m4a":       true,
	".ogg":       true,
	".webm":      true,
	".opus":      true,
	".flac":      true,
	".wav":       true,
	".aac":       true,
	".mkv":       true,
	".encrypted": true, // Spotify intermediate
	".part":      true, // incomplete downloads
	".tmp":       true, // temp files
}

// StartCleanupTask runs an immediate clean then repeats every 12 hours.
func StartCleanupTask() {
	cleanAll()

	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		cleanAll()
	}
}

func cleanAll() {
	total := 0

	// 1. Downloaded media files (yt-dlp, Arc API, Spotify, HTTP)
	total += cleanMediaFiles(config.DownloadsDir)

	// 2. TDLib channel-download files — only if it's a different directory
	if DlBotDbDir != "" {
		tdFilesDir := filepath.Join(DlBotDbDir, "files")
		if abs1, _ := filepath.Abs(config.DownloadsDir); abs1 != "" {
			if abs2, _ := filepath.Abs(tdFilesDir); abs2 != abs1 {
				total += cleanMediaFiles(tdFilesDir)
			}
		}
	}

	ClearMemCache()
	slog.Info("[Cleanup] Cycle complete", "files_removed", total)
}

// cleanMediaFiles removes media files (by extension whitelist) from dir recursively.
func cleanMediaFiles(dir string) int {
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return 0
	}

	count := 0
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(d.Name()))
		if !mediaExtensions[ext] {
			return nil
		}

		if removeErr := os.Remove(path); removeErr != nil {
			slog.Warn("[Cleanup] Failed to remove", "path", path, "error", removeErr)
		} else {
			count++
		}
		return nil
	})

	if count > 0 {
		slog.Info("[Cleanup] Cleaned", "dir", dir, "files_removed", count)
	}
	return count
}
