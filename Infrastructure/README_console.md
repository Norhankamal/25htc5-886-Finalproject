# Infrastructure — AWS Console Provisioning Guide
**CISC 886 Cloud Computing | Group 31 | Queen's University**  
**Project:** 25htc5 Beauty LLM Assistant  
**Region:** `us-east-1`

> All infrastructure was provisioned manually via the **AWS Management Console** rather than Terraform. For a single-environment, single-lifecycle academic project with no planned replication or multi-region deployment, Terraform's state management and module overhead would add complexity without meaningful benefit. All configuration decisions are documented here in lieu of `.tf` files.

---

## Table of Contents

- [VPC](#1-vpc)
- [Subnet](#2-subnet)
- [Internet Gateway](#3-internet-gateway)
- [Route Table](#4-route-table)
- [Security Group](#5-security-group)
- [S3 Bucket](#6-s3-bucket)
- [EMR Cluster](#7-emr-cluster)
- [EC2 Instance](#8-ec2-instance)
- [Design Decisions](#9-design-decisions)
- [Resource Summary](#10-resource-summary)

---

## 1. VPC

| Field | Value |
|---|---|
| **Name** | `25htc5-vpc-Group31` |
| **VPC ID** | `vpc-031c5fb9bfc6d64ac` |
| **CIDR Block** | `10.0.0.0/16` |
| **Region** | `us-east-1` |
| **DNS Resolution** | Enabled |
| **DNS Hostnames** | Disabled |
| **Default VPC** | No |

**Console path:** VPC → Your VPCs → Create VPC

### Steps

1. Open the VPC console in `us-east-1`.
2. Click **Create VPC** → select **VPC only**.
3. Set Name tag: `25htc5-vpc-Group31`.
4. Set IPv4 CIDR: `10.0.0.0/16`.
5. Leave IPv6 and tenancy as defaults.
6. Click **Create VPC**.

### Rationale

The `/16` block provides up to 65,536 addresses, giving ample room for future subnet expansion without requiring a VPC replacement.

---

## 2. Subnet

| Field | Value |
|---|---|
| **Name** | `25htc5-subnet-public` |
| **Subnet ID** | `subnet-00c8cad395160fd89` |
| **VPC** | `25htc5-vpc-Group31` |
| **CIDR Block** | `10.0.1.0/24` |
| **Availability Zone** | `us-east-1a` |
| **Auto-assign Public IPv4** | **Enabled** |
| **Available Addresses** | 251 usable |

**Console path:** VPC → Subnets → Create subnet

### Steps

1. Open VPC → **Subnets** → **Create subnet**.
2. Select VPC: `25htc5-vpc-Group31`.
3. Set Subnet name: `25htc5-subnet-public`.
4. Set Availability Zone: `us-east-1a`.
5. Set IPv4 CIDR: `10.0.1.0/24`.
6. Click **Create subnet**.
7. Select the subnet → **Actions** → **Edit subnet settings**.
8. Enable **Auto-assign public IPv4 address** → **Save**.

### Rationale

A single public subnet was sufficient — all compute components (EC2, EMR) require outbound internet access to reach S3, Hugging Face, and package repositories. A private subnet was not provisioned because no component holds data sensitive enough to warrant NAT Gateway cost for isolation. NAT Gateway would add approximately **$32/month** in baseline charges — a cost not justified given the two-week project lifecycle.

---

## 3. Internet Gateway

| Field | Value |
|---|---|
| **Name** | `25htc5-igw` |
| **State** | Attached |
| **Attached VPC** | `25htc5-vpc-Group31` |

**Console path:** VPC → Internet Gateways → Create internet gateway

### Steps

1. Open VPC → **Internet Gateways** → **Create internet gateway**.
2. Set Name tag: `25htc5-igw`.
3. Click **Create internet gateway**.
4. Select `25htc5-igw` → **Actions** → **Attach to VPC**.
5. Select `25htc5-vpc-Group31` → **Attach internet gateway**.

---

## 4. Route Table

| Field | Value |
|---|---|
| **Route Table ID** | `rtb-0844169c17cd79800` |
| **Name** | *(main route table for `25htc5-vpc-Group31`)* |
| **VPC** | `25htc5-vpc-Group31` |

**Console path:** VPC → Route Tables

### Routes

| Destination | Target | Status |
|---|---|---|
| `10.0.0.0/16` | `local` | Active |
| `0.0.0.0/0` | `25htc5-igw` | Active |

### Steps

1. Open VPC → **Route Tables**.
2. Select the route table associated with `25htc5-vpc-Group31`.
3. Click **Routes** tab → **Edit routes**.
4. Click **Add route**:
   - Destination: `0.0.0.0/0`
   - Target: **Internet Gateway** → `25htc5-igw`
5. Click **Save changes**.
6. Click **Subnet associations** tab → **Edit subnet associations**.
7. Select `25htc5-subnet-public` → **Save associations**.

---

## 5. Security Group

| Field | Value |
|---|---|
| **Name** | `25htc5-sg-Group31` |
| **Security Group ID** | `sg-04774f8e5fb96e4f1` |
| **VPC** | `25htc5-vpc-Group31` |
| **Description** | Security group for ChatAssistant project |
| **Outbound** | All traffic `0.0.0.0/0` (permit all) |

**Console path:** VPC → Security Groups → Create security group

### Inbound Rules

| Rule | Port | Protocol | Source | Purpose |
|---|---|---|---|---|
| 1 | `3000` | TCP | `0.0.0.0/0` | Open WebUI — publicly accessible chat interface |
| 2 | `11434` | TCP | `41.218.155.22/32` | Ollama REST API — team machines only |
| 3 | `22` | TCP | `41.218.155.22/32` | SSH access — team machines only |

### Steps

1. Open VPC → **Security Groups** → **Create security group**.
2. Set Name: `25htc5-sg-Group31`, Description: `Security group for ChatAssistant project`.
3. Select VPC: `25htc5-vpc-Group31`.
4. Under **Inbound rules**, add:
   - Type: `Custom TCP` | Port: `3000` | Source: `0.0.0.0/0`
   - Type: `Custom TCP` | Port: `11434` | Source: `41.218.155.22/32`
   - Type: `SSH` | Port: `22` | Source: `41.218.155.22/32`
5. Leave outbound as default (all traffic).
6. Click **Create security group**.

### Rationale

This follows the principle of least privilege: only the chat interface (port 3000) is publicly exposed. The Ollama API and SSH are restricted to the team's static IP to prevent unauthorized model access or remote login.

---

## 6. S3 Bucket

| Field | Value |
|---|---|
| **Bucket Name** | `25htc5-project-data` |
| **Region** | `us-east-1` |
| **Versioning** | Disabled |
| **Public Access** | Blocked |

**Console path:** S3 → Create bucket

### Folder Structure

```
25htc5-project-data/
├── raw/                        # 240 JSONL files (beauty_part_0000.jsonl – beauty_part_0239.jsonl)
│                               # Total: 23,911,390 records
├── processed/
│   ├── sample_preview.jsonl    # 500-record preview (199.1 KB)
│   ├── train/                  # 10,077 training pairs (Parquet)
│   ├── val/                    # 1,089 validation pairs (Parquet)
│   └── test/                   # 1,220 test pairs (Parquet)
├── models/                     # Exported GGUF model file
└── scripts/                    # spark_pipeline.py and supporting scripts
```

### Steps

1. Open S3 → **Create bucket**.
2. Set Bucket name: `25htc5-project-data`, Region: `us-east-1`.
3. Leave all other settings as default (block all public access enabled).
4. Click **Create bucket**.
5. Upload raw JSONL files to the `raw/` prefix via AWS CloudShell:

```bash
# Upload all 240 JSONL files
for i in $(seq -w 0 239); do
  aws s3 cp beauty_part_${i}.jsonl s3://25htc5-project-data/raw/
done
# Confirmed: "Done! 23,911,390 records in 240 files"
```

---

## 7. EMR Cluster

| Field | Value |
|---|---|
| **Cluster Name** | `25htc5-emr-beauty-Group31` |
| **Cluster ID** | `j-2Y1J2STVELEJX` (active run) / `j-3BZF3O10NK1EL` (final terminated) |
| **EMR Version** | `emr-7.13.0` |
| **Spark Version** | `3.5.6` |
| **Hadoop Version** | `3.4.2` |
| **Primary Node** | `1 × m5.xlarge` |
| **Core Nodes** | `1 × m5.xlarge` |
| **Log URI** | `s3://25htc5-project-data/logs/` |
| **Auto-terminate** | Enabled (idle: 1 hour) |
| **End Time** | April 30, 2026, 19:25 UTC+02:00 |

**Console path:** EMR → Clusters → Create cluster

### Steps

1. Open EMR → **Create cluster**.
2. Set Name: `25htc5-emr-beauty-Group31`.
3. Select EMR release: `emr-7.13.0`.
4. Select applications: **Spark** (includes Hadoop).
5. Under **Cluster configuration**:
   - Primary: `1 × m5.xlarge`
   - Core: `1 × m5.xlarge`
   - Task: `0`
6. Under **Cluster logs**, enable log archiving → set S3 path: `s3://25htc5-project-data/logs/`.
7. Under **Cluster termination**, enable **Auto-terminate cluster after idle time**: `1 hour`.
8. Select or create an EC2 key pair for SSH access.
9. Click **Create cluster**.

### Submitting the PySpark Step

1. Select the running cluster → **Steps** tab → **Add step**.
2. Set Step type: **Spark application**.
3. Set Name: `BeautyPipeline`.
4. Set Script location: `s3://25htc5-project-data/scripts/spark_pipeline.py`.
5. Set Arguments (if needed): S3 input/output paths.
6. Click **Add**.

### Authoritative Steps

| Step Name | Status | Notes |
|---|---|---|
| `Final_BeautyPreprocessing` | ✅ Completed | Authoritative — produced `processed/` output |
| `BeautyPipeline` | ✅ Completed | Authoritative — produced `processed/` output |
| Earlier `BeautyPreprocessing` runs (×6) | ❌ Failed | Iterative debugging — configuration/path errors; no output used downstream |

> All `Failed` steps in the console reflect iterative development runs during pipeline debugging. Each failure was caused by a configuration or path error corrected in the subsequent attempt. Only the final two completed steps produced the output used downstream.

### Cluster Configuration

A two-node configuration was sufficient: although the raw dataset contains 23.9 million records, Spark partitioned the 240 input files across both nodes, fully utilizing distributed execution without requiring additional core capacity.

---

## 8. EC2 Instance

| Field | Value |
|---|---|
| **Name** | `25htc5-Ec2` |
| **Instance ID** | `i-00d9f2bd3ba7ad9be` |
| **Instance Type** | `t3.large` |
| **AMI** | Ubuntu 22.04 LTS |
| **Public IP** | `32.192.232.248` |
| **Private IP** | `10.0.1.87` |
| **VPC** | `25htc5-vpc-Group31` |
| **Subnet** | `25htc5-subnet-public` |
| **Security Group** | `25htc5-sg-Group31` |
| **Key Pair** | `25htc5-key.pem` |
| **State** | Running |

**Console path:** EC2 → Instances → Launch instances

### Steps

1. Open EC2 → **Launch instances**.
2. Set Name: `25htc5-Ec2`.
3. Select AMI: **Ubuntu Server 22.04 LTS (HVM), SSD Volume Type**.
4. Select Instance type: `t3.large`.
5. Select or create Key pair: `25htc5-key` (`.pem` format).
6. Under **Network settings**:
   - VPC: `25htc5-vpc-Group31`
   - Subnet: `25htc5-subnet-public`
   - Auto-assign public IP: **Enable**
   - Security group: **Select existing** → `25htc5-sg-Group31`
7. Configure storage (default 8 GB gp3, increase to 30+ GB recommended for the GGUF model).
8. Click **Launch instance**.

### Post-Launch Setup

```bash
# 1. Transfer the GGUF model file from local machine
scp -i 25htc5-key.pem 25htc5-model.Q4_K_M.gguf ubuntu@32.192.232.248:~/

# 2. SSH into the instance
ssh -i 25htc5-key.pem ubuntu@32.192.232.248

# 3. Install Ollama
curl -fsSL https://ollama.com/install.sh | sh

# 4. Create a Modelfile
cat <<EOF > Modelfile
FROM ./25htc5-model.Q4_K_M.gguf
SYSTEM "You are a beauty and personal care assistant. You provide product recommendations and skincare advice grounded in real customer reviews."
EOF

# 5. Register the model with Ollama
ollama create 25htc5-beauty-assistant -f Modelfile

# 6. Verify the model responds correctly
ollama run 25htc5-beauty-assistant:latest \
  "What moisturizer do you recommend for oily skin?"

# 7. Verify via REST API
curl -s http://localhost:11434/api/generate \
  -H "Content-Type: application/json" \
  -d '{"model": "beauty_assistant", "prompt": "What moisturizer is best for dry skin?", "stream": false}' \
  | python3 -c "import sys, json; print(json.load(sys.stdin)['response'])"

# 8. Deploy Open WebUI via Docker
docker run -d \
  --add-host=host.docker.internal:host-gateway \
  -p 3000:8080 --restart always \
  -v open-webui:/app/backend/data \
  --name open-webui \
  ghcr.io/open-webui/open-webui:main
```

**Web UI** is accessible at: `http://32.192.232.248:3000` (no external signup required)

### Instance Type Rationale

`t3.large` (2 vCPU, 8 GB RAM) is sufficient for serving a Q4_K_M quantized 1B-parameter GGUF model via Ollama without a dedicated GPU. A GPU instance would be unnecessary and costly for inference at this scale.

---

## 9. Design Decisions

### Why Console Instead of Terraform?

For a single-environment, single-lifecycle academic project with no planned replication or multi-region deployment, Terraform's state management and module overhead would add complexity without meaningful benefit. The Console provides sufficient visibility and auditability for this scope.

### Why No Private Subnet?

All compute components (EC2, EMR) require outbound internet access to reach S3, Hugging Face, and package repositories. No component holds data sensitive enough to warrant NAT Gateway cost for isolation. A NAT Gateway would add approximately **$32/month** in baseline charges — unjustified for a two-week lifecycle with no sensitive data at rest.

### Why `/16` VPC and `/24` Subnet?

The `/16` block provides up to 65,536 addresses for future subnet expansion. The `/24` subnet provides 251 usable addresses — sufficient for the project's single-instance workload while keeping the allocation proportionate to actual use.

### Why `t3.large` for Serving?

The Q4_K_M quantized GGUF model for Llama 3.2-1B fits within 8 GB RAM and runs efficiently on CPU via Ollama, eliminating the need for a costly GPU instance for inference at this scale.

### Why `m5.xlarge` for EMR?

A two-node `m5.xlarge` configuration (4 vCPU, 16 GB RAM each) was sufficient: Spark partitioned the 240 input JSONL files across both nodes, fully utilizing distributed execution without requiring additional core capacity for the 23.9M-record dataset.

---

## 10. Resource Summary

| Resource | Name | ID |
|---|---|---|
| VPC | `25htc5-vpc-Group31` | `vpc-031c5fb9bfc6d64ac` |
| Subnet | `25htc5-subnet-public` | `subnet-00c8cad395160fd89` |
| Internet Gateway | `25htc5-igw` | — |
| Route Table | — | `rtb-0844169c17cd79800` |
| Security Group | `25htc5-sg-Group31` | `sg-04774f8e5fb96e4f1` |
| S3 Bucket | `25htc5-project-data` | — |
| EMR Cluster | `25htc5-emr-beauty-Group31` | `j-3BZF3O10NK1EL` |
| EC2 Instance | `25htc5-Ec2` | `i-00d9f2bd3ba7ad9be` |

All resources provisioned in **`us-east-1`** under the `25htc5-vpc-Group31` VPC.
