package io.unitycatalog.server.service.credential.aws;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Tests for third-party S3 provider configuration support. This includes endpoint URL and
 * path-style access settings for S3-compatible providers like MinIO, Ceph, Wasabi, etc.
 */
public class ThirdPartyS3ConfigTest {

  @Test
  public void testS3StorageConfigWithEndpoint() {
    // Test that S3StorageConfig correctly stores endpoint and pathStyleAccess
    S3StorageConfig config =
        S3StorageConfig.builder()
            .bucketPath("s3a://my-bucket")
            .region("us-east-1")
            .endpoint("http://minio:9000")
            .pathStyleAccess(true)
            .accessKey("minioadmin")
            .secretKey("minioadmin")
            .build();

    assertThat(config.getBucketPath()).isEqualTo("s3a://my-bucket");
    assertThat(config.getRegion()).isEqualTo("us-east-1");
    assertThat(config.getEndpoint()).isEqualTo("http://minio:9000");
    assertThat(config.getPathStyleAccess()).isTrue();
    assertThat(config.getAccessKey()).isEqualTo("minioadmin");
    assertThat(config.getSecretKey()).isEqualTo("minioadmin");
    assertThat(config.getSessionToken()).isNull();
    assertThat(config.getAwsRoleArn()).isNull();
  }

  @Test
  public void testS3StorageConfigWithoutEndpoint() {
    // Test backward compatibility - AWS S3 config without endpoint
    S3StorageConfig config =
        S3StorageConfig.builder()
            .bucketPath("s3://aws-bucket")
            .region("us-west-2")
            .awsRoleArn("arn:aws:iam::123456789012:role/my-role")
            .build();

    assertThat(config.getBucketPath()).isEqualTo("s3://aws-bucket");
    assertThat(config.getRegion()).isEqualTo("us-west-2");
    assertThat(config.getAwsRoleArn()).isEqualTo("arn:aws:iam::123456789012:role/my-role");
    assertThat(config.getEndpoint()).isNull();
    assertThat(config.getPathStyleAccess()).isNull();
  }

  @Test
  public void testServerPropertiesLoadThirdPartyConfig() {
    // Test that ServerProperties correctly loads third-party S3 configuration
    Properties props = new Properties();
    props.setProperty("s3.bucketPath.0", "s3a://minio-bucket");
    props.setProperty("s3.region.0", "us-east-1");
    props.setProperty("s3.endpoint.0", "http://localhost:9000");
    props.setProperty("s3.pathStyleAccess.0", "true");
    props.setProperty("s3.accessKey.0", "minioadmin");
    props.setProperty("s3.secretKey.0", "minioadmin");

    ServerProperties serverProperties = new ServerProperties(props);
    Map<String, S3StorageConfig> configs = serverProperties.getS3Configurations();

    assertThat(configs).hasSize(1);
    assertThat(configs).containsKey("s3a://minio-bucket");

    S3StorageConfig config = configs.get("s3a://minio-bucket");
    assertThat(config.getBucketPath()).isEqualTo("s3a://minio-bucket");
    assertThat(config.getRegion()).isEqualTo("us-east-1");
    assertThat(config.getEndpoint()).isEqualTo("http://localhost:9000");
    assertThat(config.getPathStyleAccess()).isTrue();
    assertThat(config.getAccessKey()).isEqualTo("minioadmin");
    assertThat(config.getSecretKey()).isEqualTo("minioadmin");
  }

  @Test
  public void testServerPropertiesLoadMixedConfigs() {
    // Test loading both AWS S3 and third-party S3 configurations
    Properties props = new Properties();

    // Third-party S3 (MinIO)
    props.setProperty("s3.bucketPath.0", "s3a://minio-bucket");
    props.setProperty("s3.region.0", "us-east-1");
    props.setProperty("s3.endpoint.0", "http://minio:9000");
    props.setProperty("s3.pathStyleAccess.0", "true");
    props.setProperty("s3.accessKey.0", "minioadmin");
    props.setProperty("s3.secretKey.0", "minioadmin");

    // AWS S3 with STS
    props.setProperty("s3.bucketPath.1", "s3://aws-bucket");
    props.setProperty("s3.region.1", "us-west-2");
    props.setProperty("s3.awsRoleArn.1", "arn:aws:iam::123456789012:role/my-role");

    ServerProperties serverProperties = new ServerProperties(props);
    Map<String, S3StorageConfig> configs = serverProperties.getS3Configurations();

    assertThat(configs).hasSize(2);

    // Verify MinIO config
    S3StorageConfig minioConfig = configs.get("s3a://minio-bucket");
    assertThat(minioConfig).isNotNull();
    assertThat(minioConfig.getEndpoint()).isEqualTo("http://minio:9000");
    assertThat(minioConfig.getPathStyleAccess()).isTrue();

    // Verify AWS config
    S3StorageConfig awsConfig = configs.get("s3://aws-bucket");
    assertThat(awsConfig).isNotNull();
    assertThat(awsConfig.getAwsRoleArn()).isEqualTo("arn:aws:iam::123456789012:role/my-role");
    assertThat(awsConfig.getEndpoint()).isNull();
  }

  @Test
  public void testServerPropertiesPathStyleAccessFalse() {
    // Test pathStyleAccess=false for providers that use virtual-hosted style
    Properties props = new Properties();
    props.setProperty("s3.bucketPath.0", "s3://wasabi-bucket");
    props.setProperty("s3.region.0", "us-east-1");
    props.setProperty("s3.endpoint.0", "https://s3.wasabisys.com");
    props.setProperty("s3.pathStyleAccess.0", "false");
    props.setProperty("s3.accessKey.0", "wasabi-access-key");
    props.setProperty("s3.secretKey.0", "wasabi-secret-key");

    ServerProperties serverProperties = new ServerProperties(props);
    Map<String, S3StorageConfig> configs = serverProperties.getS3Configurations();

    assertThat(configs).hasSize(1);
    S3StorageConfig config = configs.get("s3://wasabi-bucket");
    assertThat(config.getPathStyleAccess()).isFalse();
  }
}
