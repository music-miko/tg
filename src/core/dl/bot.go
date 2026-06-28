/*
 * TgMusicBot - Telegram Music Bot
 *  Copyright (c) 2025-2026 Ashok Shau
 *
 *  Licensed under GNU GPL v3
 *  See https://github.com/AshokShau/TgMusicBot
 */

package dl

import (
	td "github.com/AshokShau/gotdbot"
)

var DlBot *td.Client

// DlBotDbDir is the DatabaseDirectory of whichever bot client handles media-channel
// downloads (DlBot if configured, otherwise the main bot). Set by main.go at startup.
// The cleanup task uses it to locate <DlBotDbDir>/files/ where TDLib saves channel media.
var DlBotDbDir string
