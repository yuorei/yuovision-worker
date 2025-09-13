package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/option"
)

type VideoProcessingMessage struct {
	VideoID          string `json:"video_id"`
	VideoKey         string `json:"video_key"`
	ProcessingID     string `json:"processing_id"`
	UploaderID       string `json:"uploader_id"`
	Title            string `json:"title"`
	IsPrivate        bool   `json:"is_private"`
	IsAdult          bool   `json:"is_adult"`
	IsExternalCutout bool   `json:"is_external_cutout"`
}

type VideoProcessingStatus string

const (
	VideoProcessingStatusUploaded   VideoProcessingStatus = "UPLOADED"
	VideoProcessingStatusProcessing VideoProcessingStatus = "PROCESSING"
	VideoProcessingStatusCompleted  VideoProcessingStatus = "COMPLETED"
	VideoProcessingStatusFailed     VideoProcessingStatus = "FAILED"
)

type VideoProcessingInfo struct {
	ID        string
	VideoID   string
	Status    VideoProcessingStatus
	Progress  int
	Message   *string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type VideoWorker struct {
	pubsubClient    *pubsub.Client
	firestoreClient *firestore.Client
	r2Client        *s3.Client
	projectID       string
	r2BucketName    string
	r2AccountID     string
}

func NewVideoWorker(ctx context.Context, projectID, credentialsPath, r2AccountID, r2AccessKey, r2SecretKey, r2BucketName string) (*VideoWorker, error) {
	// Initialize Pub/Sub client
	var pubsubClient *pubsub.Client
	var err error

	if credentialsPath != "" {
		pubsubClient, err = pubsub.NewClient(ctx, projectID, option.WithCredentialsFile(credentialsPath))
	} else {
		pubsubClient, err = pubsub.NewClient(ctx, projectID)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create pub/sub client: %w", err)
	}

	// Initialize Firestore client
	var firestoreClient *firestore.Client
	if credentialsPath != "" {
		firestoreClient, err = firestore.NewClient(ctx, projectID, option.WithCredentialsFile(credentialsPath))
	} else {
		firestoreClient, err = firestore.NewClient(ctx, projectID)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create firestore client: %w", err)
	}

	// Initialize R2 client
	r2Endpoint := fmt.Sprintf("https://%s.r2.cloudflarestorage.com", r2AccountID)
	r2Resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{URL: r2Endpoint}, nil
	})

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithEndpointResolverWithOptions(r2Resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(r2AccessKey, r2SecretKey, "")),
		config.WithRegion("auto"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config for R2: %w", err)
	}

	r2Client := s3.NewFromConfig(awsCfg)

	return &VideoWorker{
		pubsubClient:    pubsubClient,
		firestoreClient: firestoreClient,
		r2Client:        r2Client,
		projectID:       projectID,
		r2BucketName:    r2BucketName,
		r2AccountID:     r2AccountID,
	}, nil
}

func (w *VideoWorker) updateProcessingStatus(ctx context.Context, processingID, videoID string, status VideoProcessingStatus, progress int, message *string) error {
	docRef := w.firestoreClient.Collection("video_processing").Doc(processingID)

	// Check if document exists first
	doc, err := docRef.Get(ctx)
	if err != nil && status == VideoProcessingStatusProcessing {
		// Document doesn't exist, create it with initial data
		initialData := map[string]any{
			"id":         processingID,
			"video_id":   videoID,
			"status":     string(status),
			"progress":   progress,
			"created_at": time.Now(),
			"updated_at": time.Now(),
		}
		if message != nil {
			initialData["message"] = *message
		}

		_, err = docRef.Set(ctx, initialData)
		if err != nil {
			return fmt.Errorf("failed to create processing status document: %w", err)
		}
		log.Printf("Created processing status document for %s: %s (%d%%)", processingID, status, progress)
		return nil
	}

	// Document exists or we're not creating it, update with merge
	data := map[string]any{
		"status":     string(status),
		"progress":   progress,
		"updated_at": time.Now(),
	}

	if message != nil {
		data["message"] = *message
	}

	// If document exists, include video_id and created_at from existing doc
	if err == nil && doc.Exists() {
		if existingVideoID, ok := doc.Data()["video_id"]; ok {
			data["video_id"] = existingVideoID
		} else {
			data["video_id"] = videoID
		}
		if createdAt, ok := doc.Data()["created_at"]; ok {
			data["created_at"] = createdAt
		}
		if docID, ok := doc.Data()["id"]; ok {
			data["id"] = docID
		}
	}

	_, err = docRef.Set(ctx, data, firestore.MergeAll)
	if err != nil {
		return fmt.Errorf("failed to update processing status: %w", err)
	}

	log.Printf("Updated processing status for %s: %s (%d%%)", processingID, status, progress)
	return nil
}

func (w *VideoWorker) downloadFromR2(ctx context.Context, key, outputPath string) error {
	log.Printf("Downloading from R2: %s to %s", key, outputPath)

	result, err := w.r2Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(w.r2BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object from R2: %w", err)
	}
	defer result.Body.Close()

	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, result.Body)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	log.Printf("Successfully downloaded from R2: %s", outputPath)
	return nil
}

func (w *VideoWorker) uploadToR2(ctx context.Context, filePath, key, contentType string) error {
	log.Printf("Uploading to R2: %s as %s", filePath, key)

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	_, err = w.r2Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(w.r2BucketName),
		Key:         aws.String(key),
		Body:        file,
		ContentType: aws.String(contentType),
	})

	if err != nil {
		return fmt.Errorf("failed to upload to R2: %w", err)
	}

	log.Printf("Successfully uploaded to R2: %s", key)
	return nil
}

func (w *VideoWorker) ProcessVideo(ctx context.Context, msg VideoProcessingMessage) error {
	log.Printf("Processing video: %s (ProcessingID: %s) for user: %s", msg.VideoID, msg.ProcessingID, msg.UploaderID)

	// Update status to PROCESSING
	if err := w.updateProcessingStatus(ctx, msg.ProcessingID, msg.VideoID, VideoProcessingStatusProcessing, 10, nil); err != nil {
		log.Printf("Failed to update status to PROCESSING: %v", err)
	}

	// Create temporary directory for processing
	tempDir := fmt.Sprintf("/tmp/video_%s", msg.VideoID)
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		errMsg := fmt.Sprintf("Failed to create temp directory: %v", err)
		w.updateProcessingStatus(ctx, msg.ProcessingID, msg.VideoID, VideoProcessingStatusFailed, 0, &errMsg)
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Download source video from R2
	inputPath := filepath.Join(tempDir, "input.mp4")
	if err := w.downloadFromR2(ctx, msg.VideoKey, inputPath); err != nil {
		errMsg := fmt.Sprintf("Failed to download video: %v", err)
		w.updateProcessingStatus(ctx, msg.ProcessingID, msg.VideoID, VideoProcessingStatusFailed, 10, &errMsg)
		return fmt.Errorf("failed to download video: %w", err)
	}

	// Update progress
	if err := w.updateProcessingStatus(ctx, msg.ProcessingID, msg.VideoID, VideoProcessingStatusProcessing, 25, nil); err != nil {
		log.Printf("Failed to update progress: %v", err)
	}

	// Process video to HLS
	outputDir := filepath.Join(tempDir, "hls")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		errMsg := fmt.Sprintf("Failed to create output directory: %v", err)
		w.updateProcessingStatus(ctx, msg.ProcessingID, msg.VideoID, VideoProcessingStatusFailed, 25, &errMsg)
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	if err := w.convertToHLS(inputPath, outputDir); err != nil {
		errMsg := fmt.Sprintf("Failed to convert to HLS: %v", err)
		w.updateProcessingStatus(ctx, msg.ProcessingID, msg.VideoID, VideoProcessingStatusFailed, 40, &errMsg)
		return fmt.Errorf("failed to convert to HLS: %w", err)
	}

	// Update progress
	if err := w.updateProcessingStatus(ctx, msg.ProcessingID, msg.VideoID, VideoProcessingStatusProcessing, 70, nil); err != nil {
		log.Printf("Failed to update progress: %v", err)
	}

	// Generate thumbnail
	thumbnailPath := filepath.Join(tempDir, "thumbnail.jpg")
	if err := w.generateThumbnail(inputPath, thumbnailPath); err != nil {
		log.Printf("Failed to generate thumbnail (non-fatal): %v", err)
		// Don't fail the entire process for thumbnail generation
	}

	// Update progress
	if err := w.updateProcessingStatus(ctx, msg.ProcessingID, msg.VideoID, VideoProcessingStatusProcessing, 80, nil); err != nil {
		log.Printf("Failed to update progress: %v", err)
	}

	// Upload HLS files to R2
	hlsPrefix := fmt.Sprintf("videos/%s/hls/", msg.VideoID)
	if err := w.uploadHLSFiles(ctx, outputDir, hlsPrefix); err != nil {
		errMsg := fmt.Sprintf("Failed to upload HLS files: %v", err)
		w.updateProcessingStatus(ctx, msg.ProcessingID, msg.VideoID, VideoProcessingStatusFailed, 80, &errMsg)
		return fmt.Errorf("failed to upload HLS files: %w", err)
	}

	// Upload thumbnail if generated successfully
	if _, err := os.Stat(thumbnailPath); err == nil {
		thumbnailKey := fmt.Sprintf("videos/%s/thumbnail.jpg", msg.VideoID)
		if err := w.uploadToR2(ctx, thumbnailPath, thumbnailKey, "image/jpeg"); err != nil {
			log.Printf("Failed to upload thumbnail (non-fatal): %v", err)
		}
	}

	// Update Firestore video record with processed URLs
	if err := w.updateVideoWithProcessedURLs(ctx, msg.VideoID, hlsPrefix); err != nil {
		log.Printf("Failed to update video URLs: %v", err)
		// Don't fail the processing, but log the error
	}

	// Update status to COMPLETED
	successMsg := "Video processing completed successfully"
	if err := w.updateProcessingStatus(ctx, msg.ProcessingID, msg.VideoID, VideoProcessingStatusCompleted, 100, &successMsg); err != nil {
		log.Printf("Failed to update status to COMPLETED: %v", err)
	}

	log.Printf("Video processing completed for: %s", msg.VideoID)
	return nil
}

func (w *VideoWorker) uploadHLSFiles(ctx context.Context, hlsDir, hlsPrefix string) error {
	return filepath.Walk(hlsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(hlsDir, path)
		if err != nil {
			return err
		}

		s3Key := hlsPrefix + relPath

		// Determine content type
		contentType := "application/octet-stream"
		if filepath.Ext(relPath) == ".m3u8" {
			contentType = "application/x-mpegURL"
		} else if filepath.Ext(relPath) == ".ts" {
			contentType = "video/MP2T"
		}

		if err := w.uploadToR2(ctx, path, s3Key, contentType); err != nil {
			return fmt.Errorf("failed to upload %s: %w", relPath, err)
		}

		return nil
	})
}

func (w *VideoWorker) updateVideoWithProcessedURLs(ctx context.Context, videoID, hlsPrefix string) error {
	videoDocRef := w.firestoreClient.Collection("videos").Doc(videoID)

	// HLS playlist URL with proper R2 format
	playlistURL := fmt.Sprintf("https://%s.%s.r2.cloudflarestorage.com/%splaylist.m3u8", w.r2BucketName, w.r2AccountID, hlsPrefix)

	_, err := videoDocRef.Update(ctx, []firestore.Update{
		{Path: "video_url", Value: playlistURL},
		{Path: "updated_at", Value: time.Now()},
	})

	if err != nil {
		return fmt.Errorf("failed to update video URLs in Firestore: %w", err)
	}

	log.Printf("Updated video %s with HLS URL: %s", videoID, playlistURL)
	return nil
}

func (w *VideoWorker) convertToHLS(inputPath, outputDir string) error {
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		return fmt.Errorf("input file does not exist: %s", inputPath)
	}

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
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		return fmt.Errorf("input file does not exist: %s", inputPath)
	}

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
	log.Printf("Starting processing with subscription: %s", subscriptionID)
	sub := w.pubsubClient.Subscription(subscriptionID)

	// Check subscription configuration
	config, err := sub.Config(ctx)
	if err != nil {
		log.Printf("Failed to get subscription config: %v", err)
		return fmt.Errorf("failed to get subscription config: %w", err)
	}

	log.Printf("Subscription config - Topic: %s, Push Config: %+v", config.Topic, config.PushConfig)

	if config.PushConfig.Endpoint != "" {
		log.Printf("WARNING: Subscription is configured for Push delivery (endpoint: %s), but worker expects Pull", config.PushConfig.Endpoint)
		return fmt.Errorf("subscription %s is configured for Push delivery, not Pull", subscriptionID)
	}

	log.Printf("Starting to receive messages from subscription: %s", subscriptionID)
	return sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		log.Printf("Received message - ID: %s, Data length: %d", msg.ID, len(msg.Data))

		var videoMsg VideoProcessingMessage
		if err := json.Unmarshal(msg.Data, &videoMsg); err != nil {
			log.Printf("Failed to unmarshal message %s: %v", msg.ID, err)
			log.Printf("Message data: %s", string(msg.Data))
			msg.Nack()
			return
		}

		log.Printf("Processing video message: %+v", videoMsg)
		if err := w.ProcessVideo(ctx, videoMsg); err != nil {
			log.Printf("Failed to process video %s (message %s): %v", videoMsg.VideoID, msg.ID, err)
			msg.Nack()
			return
		}

		msg.Ack()
		log.Printf("Successfully processed video: %s (message %s)", videoMsg.VideoID, msg.ID)
	})
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func main() {
	ctx := context.Background()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Start HTTP server for health checks
	go func() {
		http.HandleFunc("/health", healthCheckHandler)
		http.HandleFunc("/", healthCheckHandler)
		log.Printf("Starting HTTP server on port %s", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT_ID")
	credentialsPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	subscriptionID := os.Getenv("PUBSUB_SUBSCRIPTION_ID")
	r2AccountID := os.Getenv("R2_ACCOUNT_ID")
	r2AccessKey := os.Getenv("R2_ACCESS_KEY_ID")
	r2SecretKey := os.Getenv("R2_SECRET_ACCESS_KEY")
	r2BucketName := os.Getenv("R2_BUCKET_NAME")

	if projectID == "" {
		log.Fatal("GOOGLE_CLOUD_PROJECT_ID environment variable is required")
	}
	if subscriptionID == "" {
		log.Fatal("PUBSUB_SUBSCRIPTION_ID environment variable is required")
	}
	if r2AccountID == "" {
		log.Fatal("R2_ACCOUNT_ID environment variable is required")
	}
	if r2AccessKey == "" {
		log.Fatal("R2_ACCESS_KEY_ID environment variable is required")
	}
	if r2SecretKey == "" {
		log.Fatal("R2_SECRET_ACCESS_KEY environment variable is required")
	}
	if r2BucketName == "" {
		log.Fatal("R2_BUCKET_NAME environment variable is required")
	}

	worker, err := NewVideoWorker(ctx, projectID, credentialsPath, r2AccountID, r2AccessKey, r2SecretKey, r2BucketName)
	if err != nil {
		log.Fatalf("Failed to create video worker: %v", err)
	}
	defer worker.pubsubClient.Close()
	defer worker.firestoreClient.Close()

	log.Printf("Starting video worker with subscription: %s", subscriptionID)
	log.Printf("Environment variables - Project: %s, Subscription: %s, R2 Account: %s, R2 Bucket: %s",
		projectID, subscriptionID, r2AccountID, r2BucketName)

	if err := worker.StartProcessing(ctx, subscriptionID); err != nil {
		log.Printf("Video worker error: %v", err)
		log.Fatalf("Video worker error: %v", err)
	}
}
