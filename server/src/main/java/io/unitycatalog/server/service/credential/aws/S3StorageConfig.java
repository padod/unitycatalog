package io.unitycatalog.server.service.credential.aws;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class S3StorageConfig {
  private final String bucketPath;
  private final String region;
  private final String awsRoleArn;
  private final String accessKey;
  private final String secretKey;
  private final String sessionToken;
  private final String credentialsGenerator;
  private final String endpoint; // Custom S3 endpoint URL for S3-compatible providers
  private final Boolean pathStyleAccess; // Use path-style access (required for some providers)
}
