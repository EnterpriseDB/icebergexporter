// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3FileIO implements FileIO using an S3-compatible backend.
type S3FileIO struct {
	client *s3.Client
	bucket string
	prefix string
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
}

// NewS3FileIO creates a new S3FileIO from the given config.
func NewS3FileIO(ctx context.Context, cfg S3Config) (*S3FileIO, error) {
	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(cfg.Region))

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
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

	return &S3FileIO{
		client: client,
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
	}, nil
}

func (f *S3FileIO) fullKey(path string) string {
	if f.prefix == "" {
		return path
	}
	return f.prefix + "/" + path
}

func (f *S3FileIO) Write(ctx context.Context, path string, data []byte) error {
	_, err := f.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.fullKey(path)),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("s3 put %s: %w", path, err)
	}
	return nil
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
