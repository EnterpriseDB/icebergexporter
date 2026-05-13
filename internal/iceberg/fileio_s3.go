// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package iceberg

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3FileIO implements FileIO using an S3-compatible backend. Writes use the
// multipart Uploader so large files stream in parallel chunks with per-part
// retry, rather than a single PutObject that fails the whole flush on any
// transient error.
type S3FileIO struct {
	client      *s3.Client
	uploader    *manager.Uploader
	bucket      string
	prefix      string
}

// S3UploadOptions tunes multipart upload behaviour. Zero values fall back to
// the manager.Uploader defaults.
type S3UploadOptions struct {
	// PartSize is the size of each multipart chunk. S3 enforces a 10,000-part
	// ceiling per object, so PartSize × 10,000 is the largest object that can
	// be uploaded. Default in NewS3FileIO is 16 MiB (~160 GiB max object).
	PartSize int64
	// Concurrency is the number of parts uploaded in parallel.
	Concurrency int
	// MaxAttempts is the number of attempts the SDK retryer makes per request
	// (each part is one request). Default is the SDK default (3).
	MaxAttempts int
}

// S3Config holds the configuration for creating an S3FileIO.
type S3Config struct {
	Endpoint  string
	Region    string
	Bucket    string
	Prefix    string
	AccessKey string
	SecretKey string
	PathStyle bool
	Upload    S3UploadOptions
}

// Defaults for multipart upload tuning.
const (
	defaultUploadPartSize    int64 = 16 * 1024 * 1024 // 16 MiB
	defaultUploadConcurrency       = 5
)

// NewS3FileIO creates a new S3FileIO from the given config.
func NewS3FileIO(ctx context.Context, cfg S3Config) (*S3FileIO, error) {
	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(cfg.Region))

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	if cfg.Upload.MaxAttempts > 0 {
		maxAttempts := cfg.Upload.MaxAttempts
		opts = append(opts, config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = maxAttempts
			})
		}))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}
	if cfg.PathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	partSize := cfg.Upload.PartSize
	if partSize <= 0 {
		partSize = defaultUploadPartSize
	}
	concurrency := cfg.Upload.Concurrency
	if concurrency <= 0 {
		concurrency = defaultUploadConcurrency
	}

	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = concurrency
	})

	return &S3FileIO{
		client:   client,
		uploader: uploader,
		bucket:   cfg.Bucket,
		prefix:   cfg.Prefix,
	}, nil
}

func (f *S3FileIO) fullKey(path string) string {
	if f.prefix == "" {
		return path
	}
	return f.prefix + "/" + path
}

func (f *S3FileIO) Write(ctx context.Context, path string, r io.Reader) (int64, error) {
	cr := &countingReader{r: r}
	_, err := f.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.fullKey(path)),
		Body:   cr,
	})
	if err != nil {
		return cr.n, fmt.Errorf("s3 put %s: %w", path, err)
	}
	return cr.n, nil
}

// countingReader wraps an io.Reader and tracks total bytes read. The uploader
// streams from r and so won't load the full body into memory; this lets the
// caller learn the final size after the upload completes.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (f *S3FileIO) Read(ctx context.Context, path string) ([]byte, error) {
	out, err := f.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.fullKey(path)),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get %s: %w", path, err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (f *S3FileIO) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := f.fullKey(prefix)
	var keys []string

	paginator := s3.NewListObjectsV2Paginator(f.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(f.bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 list %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	return keys, nil
}

func (f *S3FileIO) Delete(ctx context.Context, path string) error {
	_, err := f.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.fullKey(path)),
	})
	if err != nil {
		return fmt.Errorf("s3 delete %s: %w", path, err)
	}
	return nil
}

func (f *S3FileIO) URI(path string) string {
	return fmt.Sprintf("s3://%s/%s", f.bucket, f.fullKey(path))
}
