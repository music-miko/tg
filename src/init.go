/*
 * TgMusicBot - Telegram Music Bot
 *  Copyright (c) 2025-2026 Ashok Shau
 *
 *  Licensed under GNU GPL v3
 *  See https://github.com/AshokShau/TgMusicBot
 */

package src

import (
	"ashokshau/tgmusic/config"
	"ashokshau/tgmusic/src/core/db"
	"ashokshau/tgmusic/src/core/dl"
	"ashokshau/tgmusic/src/vc"

	"github.com/AshokShau/gotdbot"
)

func Init(client *gotdbot.Client) error {
	if err := db.InitDatabase(); err != nil {
		return err
	}

	// Start the 12-hour downloads folder cleaner.
	go dl.StartCleanupTask()

	for _, session := range config.SessionStrings {
		_, err := vc.Calls.StartClient(config.ApiId, config.ApiHash, session)
		if err != nil {
			return err
		}
	}

	vc.Calls.RegisterHandlers(client)
	return nil
}
