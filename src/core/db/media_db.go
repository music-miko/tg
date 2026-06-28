/*
 * TgMusicBot - Telegram Music Bot
 *  Copyright (c) 2025-2026 Team Arc
 *
 *  Licensed under GNU GPL v3
 *  See https://github.com/AshokShau/TgMusicBot
 */

package db

// media_db.go — YouTube media file cache helpers.
//
// Mirrors tosu4/AnonXMusic/platforms/Youtube.py:
//   _get_media_collection() → uses config.DB_URI (separate from MONGO_URI)
//   _is_media(track_id, is_video)         → bool
//   _get_media_msg_id(track_id, is_video) → int64
//
// DB_URI connects to the arcapi database (db: "arcapi", collection: "medias").
// This is intentionally separate from the main bot MONGO_URI — exactly as in tosu4.
//
// Documents shape: { track_id: string, isVideo: bool, message_id: int64 }

import (
	"ashokshau/tgmusic/config"
	"context"
	"log/slog"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	mediaDBName         = "arcapi"
	mediaCollectionName = "medias"
)

var (
	mediaClient     *mongo.Client
	mediaClientOnce sync.Once
	mediaClientErr  error
)

type mediaDoc struct {
	TrackID   string `bson:"track_id"`
	IsVideo   bool   `bson:"isVideo"`
	MessageID int64  `bson:"message_id"`
}

// getMediaCollection returns the arcapi.medias collection connected via DB_URI.
// Returns nil if DB_URI is not configured or the connection fails.
func getMediaCollection() *mongo.Collection {
	if config.DbUri == "" {
		return nil
	}

	mediaClientOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		opts := options.Client().ApplyURI(config.DbUri).
			SetConnectTimeout(10 * time.Second).
			SetMinPoolSize(2)

		c, err := mongo.Connect(opts)
		if err != nil {
			slog.Error("[MediaDB] Failed to connect via DB_URI", "error", err)
			mediaClientErr = err
			return
		}
		if err = c.Ping(ctx, nil); err != nil {
			slog.Error("[MediaDB] Ping failed for DB_URI", "error", err)
			mediaClientErr = err
			return
		}
		slog.Info("[MediaDB] Connected to arcapi.medias via DB_URI")
		mediaClient = c
	})

	if mediaClient == nil {
		return nil
	}
	return mediaClient.Database(mediaDBName).Collection(mediaCollectionName)
}

func mediaCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// IsMedia returns true if a cached entry exists for trackID in arcapi.medias.
// Kept for completeness; prefer GetMediaMsgID which does both in one query.
func (db *Database) IsMedia(trackID string, isVideo bool) bool {
	return db.GetMediaMsgID(trackID, isVideo) > 0
}

// GetMediaMsgID returns the Telegram message_id for a cached track, or 0 if not found.
func (db *Database) GetMediaMsgID(trackID string, isVideo bool) int64 {
	col := getMediaCollection()
	if col == nil || trackID == "" {
		return 0
	}
	ctx, cancel := mediaCtx()
	defer cancel()

	var doc mediaDoc
	filter := bson.M{"track_id": trackID, "isVideo": isVideo}
	if err := col.FindOne(ctx, filter, nil).Decode(&doc); err != nil {
		slog.Debug("[MediaDB] GetMediaMsgID not found", "track_id", trackID, "error", err)
		return 0
	}
	return doc.MessageID
}
