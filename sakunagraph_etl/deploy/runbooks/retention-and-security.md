# Retention and supply-chain operations

- Published and input run artifacts retain 365 days by default; quarantine
  retains 90 days; GraphDB backups retain seven years and transition to Glacier
  Instant Retrieval after 30 days. Change Terraform variables only with data
  owner approval.
- On-premise storage administrators must apply equivalent age/version policies
  to the artifact and backup shares, preserve manifests with their referenced
  files, and test restoration before expiry. Never prune a file independently
  from its run namespace; retain drill evidence in a separate failure domain.
- S3 versioning, encryption, public-access blocks, least-privilege task roles,
  private Fargate networking, immutable ECR tags, and scan-on-push are required.
- Dependabot checks Python, Docker/Compose, Terraform, and GitHub Actions weekly.
- The security workflow generates a CycloneDX dependency SBOM using pinned
  `pip-audit`, validates Terraform, and rebuilds the core image.
- Review dependency and base-image updates individually. Rebuild both images,
  run golden/SHACL tests, compare canonical RDF, and promote by digest rather
  than mutable tag.
- Treat critical ECR or dependency findings as release blockers. If no fixed
  version exists, document exposure, compensating controls, owner, and expiry.
