package backup

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3UploaderConfig controls uploads to S3-compatible object storage.
type S3UploaderConfig struct {
	Bucket    string
	Region    string
	Endpoint  string
	PathStyle bool
}

// S3Uploader uploads backup artifacts using the AWS SDK default credential chain.
type S3Uploader struct {
	bucket string
	client *s3.Client
}

// NewS3Uploader creates an S3 uploader using the AWS SDK v2 default credential chain.
func NewS3Uploader(ctx context.Context, cfg S3UploaderConfig) (*S3Uploader, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("backup s3 bucket is required")
	}
	loadOptions := []func(*config.LoadOptions) error{}
	if cfg.Region != "" {
		loadOptions = append(loadOptions, config.WithRegion(cfg.Region))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(options *s3.Options) {
		options.UsePathStyle = cfg.PathStyle
		if cfg.Endpoint != "" {
			options.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	return &S3Uploader{bucket: cfg.Bucket, client: client}, nil
}

// Upload sends a local backup artifact to S3.
func (u *S3Uploader) Upload(ctx context.Context, object UploadObject) error {
	file, err := os.Open(object.LocalPath)
	if err != nil {
		return fmt.Errorf("open backup object for s3 upload: %w", err)
	}
	defer file.Close()

	input := &s3.PutObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(object.Key),
		Body:   file,
	}
	if object.ContentType != "" {
		input.ContentType = aws.String(object.ContentType)
	}
	if _, err := u.client.PutObject(ctx, input); err != nil {
		return fmt.Errorf("put backup object to s3: %w", err)
	}
	return nil
}
