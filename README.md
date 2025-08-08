# K8s Scanner

A modern CLI tool built with Typer that connects to Kubernetes clusters to extract resources into organized files or generate comprehensive cluster reports. This tool helps you backup configurations, understand cluster state, and plan upgrades from Kubernetes 1.25 to 1.34.

## Features

- 🚀 **Modern CLI** - Built with Typer for excellent user experience
- 📦 **Resource Extraction** - Extract all Kubernetes resources from a cluster
- 🗂️ **Smart Organization** - Organize by service (labels), namespace, or type
- 📄 **Multiple Formats** - Export as YAML or JSON
- 🎯 **Precise Filtering** - Include/exclude specific resource types
- 🧹 **Clean Output** - Removes runtime-specific fields
- 📊 **Comprehensive Reports** - Generate detailed cluster analysis:
  - Cluster version and node information
  - Helm releases and repositories
  - Resource statistics and summaries
  - Helm-managed vs manually-managed resources
- ⬆️ **Upgrade Advisor** - Detailed upgrade paths from K8s 1.25 to 1.34:
  - Version-specific deprecations and changes
  - Required migration actions
  - Best practices for each version
- 📈 **Historical Tracking** - Store and analyze cluster changes over time:
  - Automatic change detection between scans
  - Resource history and timeline tracking
  - Drift analysis and stability scoring
  - Historical summaries and statistics
- 🔌 **Works Everywhere** - Any cluster accessible via kubectl

## Prerequisites

- Python 3.8+
- kubectl configured with access to your target cluster
- uv (for Python package management)
- helm (optional, for Helm repository detection)

## Installation

1. Clone this repository
2. Install using make:

```bash
make install
```

For development:
```bash
make dev
```

## Usage

The tool provides several main commands:

### 1. Scan - Extract Resources
```bash
# Basic scan with default settings
make scan

# Or use the CLI directly
.venv/bin/python -m src.main scan
```

### 2. Report - Generate Cluster Analysis
```bash
# Generate a text report
make report

# Generate JSON report
make report ARGS="--format json"
```

### 3. Upgrade Path - Show upgrade recommendations
```bash
# Show upgrade path from current version
make upgrade-path ARGS="v1.25.0"

# Show path to specific version
make upgrade-path ARGS="v1.25.0 v1.30.0"
```

### 4. IaC Drift Analysis - Compare IaC with running cluster
```bash
# Compare IaC with cluster state
make iac-drift ARGS="/path/to/iac ./k8s-resources"

# Hide EKS system resources for cleaner output
make iac-drift ARGS="/path/to/iac ./k8s-resources --hide-system"
```

### 5. Historical Commands - Track Changes Over Time
```bash
# Show recent scan history
make history

# Show recent changes
make changes

# Compare two specific scans
make compare ARGS="1 2"

# Show history of a specific resource
make resource-history ARGS="Deployment my-app --namespace production"

# Show historical summary and drift analysis
make summary

# Clean up old scan data
make cleanup ARGS="--keep-days 60"

# Show database information
make db-info
```

### Command Options

#### Scan Command
- `--namespace, -n`: Scan specific namespace (default: all namespaces)
- `--output, -o`: Output directory for files (default: ./k8s-resources)
- `--context, -c`: Kubernetes context to use
- `--organize-by`: How to organize files - 'service', 'namespace', 'type', or 'annotation' (default: service)
- `--organize-annotation-key`: Specific annotation key to use when organizing by annotation
- `--exclude-type, -e`: Resource types to exclude (can be used multiple times)
- `--include-type, -i`: Only include these resource types (can be used multiple times)
- `--annotation-filter, -a`: Filter by annotations (format: key:operator:value)
- `--annotation-config`: Path to company-specific annotation configuration file
- `--format, -f`: Output format - 'yaml' or 'json' (default: yaml)
- `--store-history/--no-store-history`: Store scan results in database (default: enabled)
- `--detect-changes/--no-detect-changes`: Detect and report changes since last scan (default: enabled)

#### Report Command
- `--namespace, -n`: Scan specific namespace (default: all namespaces)
- `--output, -o`: Output directory for report (default: ./k8s-resources)
- `--context, -c`: Kubernetes context to use
- `--format, -f`: Report format - 'text', 'json', or 'yaml' (default: text)
- `--exclude-type, -e`: Resource types to exclude from statistics
- `--include-type, -i`: Only include these resource types in statistics

#### Drift Command
- `iac_path`: Path to Infrastructure as Code directory
- `cluster_path`: Path to cluster export directory (default: ./k8s-resources)
- `--hide-system`: Hide EKS/K8s system resources from analysis

#### Historical Commands
- `history --limit N`: Show N recent scans (default: 10)
- `changes --days N --limit N`: Show changes in last N days, limit results
- `compare scan1_id scan2_id`: Compare two specific scans
- `resource-history kind name --namespace ns`: Show history of specific resource
- `summary --days N`: Show historical summary for last N days (default: 30)
- `cleanup --keep-days N --dry-run`: Clean up old scan data
- `db-info`: Show database information and statistics

### Examples

#### Resource Extraction
```bash
# Scan all resources in default namespace
make scan ARGS="--namespace default"

# Scan production cluster and organize by namespace
make scan ARGS="--context prod-cluster --organize-by namespace"

# Exclude Events and Pods
make scan ARGS="--exclude-type Event --exclude-type Pod"

# Only scan Deployments and Services
make scan ARGS="--include-type Deployment --include-type Service"

# Extract resources in JSON format
make scan ARGS="--format json"

# Organize by annotation (e.g., by team)
make scan ARGS="--organize-by annotation --organize-annotation-key team"

# Filter by annotation
make scan ARGS="--annotation-filter team:equals:platform"

# Multiple annotation filters
make scan ARGS="-a team:equals:platform -a environment:in:prod,staging"

# Use company-specific annotation config
make scan ARGS="--annotation-config ./company-annotations.yaml"
```

#### Annotation Filtering

The tool supports powerful annotation-based filtering and organization:

**Filter Operators:**
- `equals`: Exact match (e.g., `team:equals:platform`)
- `not_equals`: Not equal (e.g., `env:not_equals:test`)
- `contains`: Substring match (e.g., `description:contains:api`)
- `starts_with`: Prefix match (e.g., `version:starts_with:v2`)
- `ends_with`: Suffix match (e.g., `domain:ends_with:.com`)
- `regex`: Regular expression (e.g., `cost-center:regex:^CC-[0-9]+$`)
- `exists`: Key exists (e.g., `owner:exists`)
- `not_exists`: Key doesn't exist (e.g., `deprecated:not_exists`)
- `in`: Value in list (e.g., `env:in:prod,staging,qa`)
- `not_in`: Value not in list (e.g., `tier:not_in:test,dev`)

**Examples:**
```bash
# Find resources owned by platform team
make scan ARGS="-a team:equals:platform"

# Find production resources with cost center
make scan ARGS="-a environment:equals:production -a cost-center:exists"

# Exclude test resources
make scan ARGS="-a lifecycle:not_equals:test -a temporary:not_exists"

# Complex filtering with regex
make scan ARGS="-a owner:regex:.*@company.com -a cost-center:regex:^CC-[0-9]{4}$"
```

#### Report Generation
```bash
# Generate a cluster report
make report

# Generate JSON format report
make report ARGS="--format json"

# Report for specific namespace
make report ARGS="--namespace production"
```

#### Upgrade Planning
```bash
# Check upgrade from 1.25
make upgrade-path ARGS="v1.25.0"

# Plan multi-version upgrade
make upgrade-path ARGS="v1.25.0 v1.34.0"
```

#### IaC Drift Analysis
```bash
# Compare IaC with cluster reality
make iac-drift ARGS="~/workspace/syntin/infra/k8s ./k8s-resources"

# Focus on app drift, hide system resources
make iac-drift ARGS="~/workspace/syntin/infra/k8s ./k8s-resources --hide-system"
```

#### Historical Tracking
```bash
# Scan and store in database (default behavior)
make scan

# Scan without storing history
make scan ARGS="--no-store-history"

# View recent scan history
make history

# Show changes in last 7 days
make changes

# Show detailed summary for last 30 days
make summary

# Track a specific deployment over time
make resource-history ARGS="Deployment my-app --namespace production"

# Compare two specific scans
make compare ARGS="5 8"

# Clean up old data (keep last 60 days)
make cleanup ARGS="--keep-days 60"
```

## Company-Specific Annotation Configuration

For enterprise use, you can create a configuration file to enforce annotation standards:

```yaml
# company-annotations.yaml
required_annotations:
  - cost-center
  - team
  - environment

annotation_patterns:
  cost-center: "^CC-[0-9]{4}$"
  team: "^(platform|backend|frontend|data)$"
  environment: "^(production|staging|development)$"

rules:
  - name: "production-requirements"
    description: "Production resources must have owner"
    filters:
      - key: "environment"
        operator: "equals"
        value: "production"
    action: "require"
    metadata:
      required_annotations:
        - owner
        - on-call-team
```

Use with: `make scan ARGS="--annotation-config ./company-annotations.yaml"`

See `examples/annotation-config.yaml` for a complete example.

## Output Structure

### Resource Extraction Mode

When organizing by service (default), the output structure looks like:
```
k8s-resources/
├── my-app/
│   ├── deployment.yaml
│   ├── service.yaml
│   └── configmap.yaml
├── another-app/
│   ├── deployment.yaml
│   └── service.yaml
├── uncategorized/
│   └── namespace.yaml
└── scan-summary.txt
```

When organizing by annotation (e.g., `--organize-by annotation --organize-annotation-key team`):
```
k8s-resources/
├── team=platform/
│   ├── deployment.yaml
│   └── service.yaml
├── team=backend/
│   ├── deployment.yaml
│   └── configmap.yaml
├── team=frontend/
│   └── deployment.yaml
├── no-annotation/
│   └── namespace.yaml
└── scan-summary.txt
```

### Report Mode

When generating a report (`--report`), the output includes:
```
k8s-resources/
└── cluster-report.txt  # or .json/.yaml based on --report-format
```

The report contains:
- Cluster version and platform information
- Node details (version, OS, container runtime)
- Helm releases and repositories
- Resource statistics (total count, by type, by namespace)
- Identification of Helm-managed vs non-Helm resources
- Upgrade suggestions with:
  - Recommended next version
  - API deprecations
  - Required migration actions
  - General upgrade best practices

### Historical Data

When historical tracking is enabled (default), scan data is stored in a SQLite database at `~/.k8s-scanner/history.db`. This enables:
- Change detection between scans
- Resource timeline tracking
- Drift analysis and stability scoring
- Historical summaries and statistics

## Testing

Run tests:
```bash
make test
```

Run linting:
```bash
make lint
```

## Project Structure

The project follows a modular architecture:

```
src/
├── cli/            # CLI interface using Typer
│   └── main.py     # Command definitions
├── core/           # Core business logic
│   ├── organizer.py    # Resource organization
│   ├── reporter.py     # Report generation
│   └── helm.py         # Helm integration
├── k8s/            # Kubernetes interaction
│   ├── client.py       # kubectl wrapper
│   └── scanner.py      # Resource scanning
├── models/         # Pydantic data models
│   ├── cluster.py      # Cluster info models
│   ├── export.py       # Export options
│   ├── kubernetes.py   # K8s resource models
│   └── report.py       # Report models
├── upgrade/        # Upgrade advisor
│   ├── advisor.py      # Upgrade logic
│   └── versions.py     # Version database (1.25-1.34)
├── exporters/      # Export implementations
│   ├── yaml_exporter.py
│   └── json_exporter.py
└── utils/          # Utilities
    └── logger.py       # Logging config
```

## Development

Built with modern Python tools:
- **CLI Framework**: Typer with Rich for beautiful terminal output
- **Data Validation**: Pydantic for robust data models
- **Package Management**: uv for fast dependency resolution
- **Testing**: pytest with comprehensive test coverage

## License

MIT