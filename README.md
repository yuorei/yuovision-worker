# YuoVision Worker Service

動画処理専用のWorker Serviceです。Google Cloud Pub/Subからメッセージを受信し、非同期で動画変換とサムネイル生成を行います。

## 機能

- Google Cloud Pub/Subからの動画処理メッセージ受信
- FFmpegを使用したHLS形式への動画変換
- サムネイル画像生成
- Cloudflare R2への処理済みファイルアップロード
- Firestoreでの処理状況更新

## 環境変数

- `GOOGLE_CLOUD_PROJECT_ID`: Google CloudプロジェクトID
- `GOOGLE_APPLICATION_CREDENTIALS`: Firebase認証情報ファイルパス（オプション）
- `PUBSUB_SUBSCRIPTION_ID`: 動画処理用Pub/Subサブスクリプション名
- `R2_ACCESS_KEY_ID`: Cloudflare R2アクセスキー
- `R2_SECRET_ACCESS_KEY`: Cloudflare R2シークレットキー
- `R2_ACCOUNT_ID`: Cloudflare R2アカウントID
- `R2_BUCKET_NAME`: Cloudflare R2バケット名

## 開発・実行

### ローカル実行
```bash
go mod tidy
go run main.go
```

### Docker実行
```bash
docker build -t yuovision-worker .
docker run -e GOOGLE_CLOUD_PROJECT_ID=your-project \
           -e PUBSUB_SUBSCRIPTION_ID=video-processing \
           yuovision-worker
```

### Cloud Run デプロイ
```bash
gcloud run deploy yuovision-worker \
  --source . \
  --platform managed \
  --region asia-northeast1 \
  --allow-unauthenticated \
  --set-env-vars GOOGLE_CLOUD_PROJECT_ID=your-project,PUBSUB_SUBSCRIPTION_ID=video-processing
```

## メッセージ形式

```json
{
  "video_id": "video_uuid_12345",
  "source_url": "https://r2-bucket/uploads/video.mp4",
  "user_id": "user_uuid_67890"
}
```

## 処理フロー

1. Pub/Subメッセージ受信
2. 動画ファイルをR2からダウンロード
3. FFmpegでHLS形式に変換
4. サムネイル生成
5. 処理済みファイルをR2にアップロード
6. Firestoreで処理状況を更新