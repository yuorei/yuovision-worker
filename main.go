package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type VideoProcessingMessage struct {
	VideoID   string `json:"video_id"`
	SourceURL string `json:"source_url"`
	UserID    string `json:"user_id"`
}

type VideoWorker struct {
	pubsubClient *pubsub.Client
	projectID    string
}

func NewVideoWorker(ctx context.Context, projectID, credentialsPath string) (*VideoWorker, error) {
	var client *pubsub.Client
	var err error

	if credentialsPath != "" {
		client, err = pubsub.NewClient(ctx, projectID, option.WithCredentialsFile(credentialsPath))
	} else {
		client, err = pubsub.NewClient(ctx, projectID)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create pub/sub client: %w", err)
	}

	return &VideoWorker{
		pubsubClient: client,
		projectID:    projectID,
	}, nil
}

func (w *VideoWorker) ProcessVideo(ctx context.Context, msg VideoProcessingMessage) error {
	log.Printf("Processing video: %s for user: %s", msg.VideoID, msg.UserID)

	// Create temporary directory for processing
	tempDir := fmt.Sprintf("/tmp/video_%s", msg.VideoID)
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Download source video
	inputPath := filepath.Join(tempDir, "input.mp4")
	if err := w.downloadVideo(msg.SourceURL, inputPath); err != nil {
		return fmt.Errorf("failed to download video: %w", err)
	}

	// Process video to HLS
	outputDir := filepath.Join(tempDir, "hls")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	if err := w.convertToHLS(inputPath, outputDir); err != nil {
		return fmt.Errorf("failed to convert to HLS: %w", err)
	}

	// Generate thumbnail
	thumbnailPath := filepath.Join(tempDir, "thumbnail.jpg")
	if err := w.generateThumbnail(inputPath, thumbnailPath); err != nil {
		return fmt.Errorf("failed to generate thumbnail: %w", err)
	}

	// Upload processed files to R2 (this would be implemented)
	log.Printf("Video processing completed for: %s", msg.VideoID)

	// Update Firestore with completed status (this would be implemented)

	return nil
}

func (w *VideoWorker) downloadVideo(sourceURL, outputPath string) error {
	// Implementation would download from R2 using signed URL
	log.Printf("Downloading video from: %s to: %s", sourceURL, outputPath)
	return nil
}

func (w *VideoWorker) convertToHLS(inputPath, outputDir string) error {
	// Use FFmpeg to convert video to HLS format
	playlistPath := filepath.Join(outputDir, "playlist.m3u8")

	cmd := exec.Command("ffmpeg",
		"-i", inputPath,
		"-codec:v", "libx264",
		"-codec:a", "aac",
		"-hls_time", "10",
		"-hls_playlist_type", "vod",
		"-hls_segment_filename", filepath.Join(outputDir, "segment%03d.ts"),
		playlistPath,
	)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ffmpeg conversion failed: %w", err)
	}

	log.Printf("HLS conversion completed: %s", playlistPath)
	return nil
}

func (w *VideoWorker) generateThumbnail(inputPath, outputPath string) error {
	// Generate thumbnail using FFmpeg
	cmd := exec.Command("ffmpeg",
		"-i", inputPath,
		"-ss", "00:00:01.000",
		"-vframes", "1",
		outputPath,
	)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("thumbnail generation failed: %w", err)
	}

	log.Printf("Thumbnail generated: %s", outputPath)
	return nil
}

func (w *VideoWorker) StartProcessing(ctx context.Context, subscriptionID string) error {
	sub := w.pubsubClient.Subscription(subscriptionID)

	return sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		var videoMsg VideoProcessingMessage
		if err := json.Unmarshal(msg.Data, &videoMsg); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			msg.Nack()
			return
		}

		if err := w.ProcessVideo(ctx, videoMsg); err != nil {
			log.Printf("Failed to process video %s: %v", videoMsg.VideoID, err)
			msg.Nack()
			return
		}

		msg.Ack()
		log.Printf("Successfully processed video: %s", videoMsg.VideoID)
	})
}

func main() {
	ctx := context.Background()

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT_ID")
	credentialsPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	subscriptionID := os.Getenv("PUBSUB_SUBSCRIPTION_ID")

	if projectID == "" {
		log.Fatal("GOOGLE_CLOUD_PROJECT_ID environment variable is required")
	}
	if subscriptionID == "" {
		log.Fatal("PUBSUB_SUBSCRIPTION_ID environment variable is required")
	}

	worker, err := NewVideoWorker(ctx, projectID, credentialsPath)
	if err != nil {
		log.Fatalf("Failed to create video worker: %v", err)
	}
	defer worker.pubsubClient.Close()

	log.Printf("Starting video worker with subscription: %s", subscriptionID)
	if err := worker.StartProcessing(ctx, subscriptionID); err != nil {
		log.Fatalf("Video worker error: %v", err)
	}
}
