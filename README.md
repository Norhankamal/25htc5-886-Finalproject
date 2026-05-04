# 25htc5 Beauty Assistant — CISC 886 Cloud Computing Project
**Group 31 | Queen's University**

A domain-specific beauty and personal care chatbot built by fine-tuning **Llama 3.2-1B** on 23.9 million Amazon product reviews, deployed on AWS with a full end-to-end cloud pipeline.

---

## Table of Contents

- [System Overview](#system-overview)
- [Architecture](#architecture)
- [VPC & Networking](#vpc--networking)
- [Dataset](#dataset)
- [Data Preprocessing (EMR + PySpark)](#data-preprocessing-emr--pyspark)
- [Model Fine-Tuning](#model-fine-tuning)
- [Model Deployment](#model-deployment)
- [Web Interface](#web-interface)
- [Results](#results)

---

## System Overview

| Component | Details |
|---|---|
| **Base Model** | Llama 3.2-1B (`unsloth/Llama-3.2-1B-Instruct`) |
| **Fine-tuning Method** | QLoRA (4-bit) with Unsloth + SFTTrainer |
| **Training Data** | 10,000 samples from Amazon Beauty & Personal Care reviews |
| **Serving** | Ollama on EC2 `t3.large` (Ubuntu 22.04) |
| **UI** | Open WebUI via Docker on port 3000 |
| **Region** | AWS `us-east-1` |

---

## Architecture

Data flows through three phases:

```
[User Browser]
      │  HTTP :3000
      ▼
[Internet Gateway — 25htc5-igw]
      │
      ▼
[VPC: 25htc5-vpc-Group31  10.0.0.0/16]
  [Public Subnet: 10.0.1.0/24]
  [Security Group: 25htc5-sg-Group31]
      │
      ├── [EMR Cluster]  ──────────── [S3: 25htc5-project-data]
      │    PySpark pipeline                raw/ → processed/
      │
      ├── [Google Colab T4 GPU] ───── [S3: models/]
      │    QLoRA fine-tuning               GGUF export
      │
      └── [EC2: 25htc5-Ec2  t3.large]
           ├── Ollama :11434  (GGUF model)
           └── Open WebUI :3000  (Docker)
```

**Preprocessing Phase:** Raw JSONL data → S3 → EMR/PySpark → cleaned Parquet splits  
**Fine-tuning Phase:** Google Colab T4 → QLoRA on 10K samples → GGUF export → SCP to EC2  
**Serving Phase:** Ollama loads GGUF → Open WebUI → browser

---

## VPC & Networking

Provisioned via AWS Management Console (no Terraform — single-lifecycle academic project).

| Resource | ID / Value |
|---|---|
| VPC | `25htc5-vpc-Group31` (`vpc-031c5fb9bfc6d64ac`) — CIDR `10.0.0.0/16` |
| Public Subnet | `25htc5-subnet-public` — `10.0.1.0/24`, `us-east-1a`, auto-assign IPv4 |
| Internet Gateway | `25htc5-igw` — route `0.0.0.0/0 → 25htc5-igw` |
| Route Table | `rtb-0844169c17cd79800` |
| Security Group | `25htc5-sg-Group31` (`sg-04774f8e5fb96e4f1`) |

**Security Group Inbound Rules:**

| Port | Protocol | Source | Purpose |
|---|---|---|---|
| 3000 | TCP | `0.0.0.0/0` | Open WebUI (public) |
| 11434 | TCP | `41.218.155.22/32` | Ollama API (team only) |
| 22 | TCP | `41.218.155.22/32` | SSH (team only) |

> No private subnet or NAT Gateway was provisioned — all components require outbound internet access and no sensitive data is stored at rest. NAT Gateway would add ~$32/month with no security benefit for this scope.

---

## Dataset

**Source:** `McAuley-Lab/Amazon-Reviews-2023` — Beauty and Personal Care subset  
**Size:** ~23.9 million records (~1.1B tokens) across 240 JSONL files  
**S3 Bucket:** `25htc5-project-data`

### Processed Split

| Split | Records | S3 Prefix |
|---|---|---|
| Train (80%) | 10,077 | `processed/train/` |
| Validation (10%) | 1,089 | `processed/val/` |
| Test (10%) | 1,220 | `processed/test/` |

**Key stats:** Average rating 4.27/5 · Median output length 89 words · Max sequence length 512 tokens

### Q&A Types (balanced, ~4,129 each)

- **User-Aware Recommendation** — personalized by skin type, budget, problem
- **Reasoned Summary** — aggregated sentiment across reviews
- **Specific Q&A** — targeted product questions

### Sample Record

```json
{
  "instruction": "What do customers with all skin say about Conair Ceramic 1 1/2-inch Hot Rollers for general skincare?",
  "input": "",
  "output": "Conair Ceramic Hot Rollers may not be the best fit for general skincare (rated 2.6/5 across 14 reviews). Customers report: 'Plugged these in and within 30 seconds started to smell something burning and then saw smoke...' Consider looking for alternatives.",
  "type": "user_aware_recommendation",
  "skin_type": "all",
  "problem": "general skincare",
  "budget": "high",
  "product_id": "B00005A441"
}
```

---

## Data Preprocessing (EMR + PySpark)

**Cluster:** `25htc5-emr-beauty-Group31` (`j-2Y1J2STVELEJX`)  
**Config:** EMR 7.13.0 · Spark 3.5.6 · Hadoop 3.4.2 · 1 Primary + 1 Core `m5.xlarge`

### Pipeline Stages (`spark_pipeline.py`)

1. **Data Ingestion** — reads all 240 JSONL files from `s3://25htc5-project-data/raw/`
2. **Schema Validation & Filtering** — drops null/empty fields, removes outputs < 30 chars or non-English
3. **Feature Engineering** — computes word count, token length; assigns `skin_type`, `budget`, `problem` tags
4. **Q&A Pair Construction** — formats records into instruction–input–output triples using the Llama 3.2 chat template
5. **Shuffling & Splitting** — 80/10/10 split with fixed random seed; written to separate S3 prefixes to prevent leakage
6. **Preview Export** — 500-record sample written to `processed/sample_preview.jsonl`

> **Note on failed steps:** Earlier EMR steps in the console show `Failed` status — these are iterative debugging runs. The two authoritative steps (`BeautyPipeline` and `Final_BeautyPreprocessing`) both completed successfully and produced all downstream output.

Cluster terminated after job completion: **April 30, 2026, 19:25 UTC+02:00**

---

## Model Fine-Tuning

Implemented in `Model_Fine_Tuning.ipynb` on Google Colab (T4 GPU).  
GitHub: [https://github.com/Norhankamal/25htc5-886-Finalproject](https://github.com/Norhankamal/25htc5-886-Finalproject)

### Hyperparameters

| Parameter | Value |
|---|---|
| Base model | `unsloth/Llama-3.2-1B-Instruct` |
| Quantization | 4-bit (QLoRA) |
| LoRA rank (r) | 8 |
| LoRA alpha | 16 |
| LoRA dropout | 0.1 |
| Learning rate | 1e-4 |
| Batch size (per device) | 4 |
| Gradient accumulation steps | 4 |
| Effective batch size | 16 |
| Epochs | 1 |
| Warmup ratio | 0.1 |
| Optimizer | AdamW (8-bit) |
| LR scheduler | Cosine |
| Weight decay | 0.05 |
| Training samples | 10,000 |
| Max sequence length | 512 |

**LoRA target modules:** `q_proj`, `k_proj`, `v_proj`, `o_proj`, `gate_proj`, `up_proj`, `down_proj`

After training, the adapter was merged and exported to **GGUF (Q4_K_M)** via `save_pretrained_gguf`, uploaded to `s3://25htc5-project-data/models/`, then transferred to EC2 via SCP.

---

## Model Deployment

**EC2 Instance:** `25htc5-Ec2` (`i-00d9f2bd3ba7ad9be`)  
**Spec:** `t3.large` · Ubuntu 22.04 LTS · Public IP `32.192.232.248` · Private IP `10.0.1.87`

```bash
# Transfer model
scp -i 25htc5-key.pem 25htc5-model.Q4_K_M.gguf ubuntu@32.192.232.248:~/

# Install Ollama
curl -fsSL https://ollama.com/install.sh | sh

# Modelfile
FROM ./25htc5-model.Q4_K_M.gguf
SYSTEM "You are a beauty and personal care assistant..."

# Register and verify
ollama create 25htc5-beauty-assistant -f Modelfile
ollama run 25htc5-beauty-assistant:latest "What moisturizer do you recommend for oily skin?"
```

Model registered as `25htc5-beauty-assistant:latest` on Ollama port `11434`.

---

## Web Interface

Open WebUI deployed via Docker, accessible at `http://32.192.232.248:3000` — no external signup required.

```bash
docker run -d \
  --add-host=host.docker.internal:host-gateway \
  -p 3000:8080 --restart always \
  -v open-webui:/app/backend/data \
  --name open-webui \
  ghcr.io/open-webui/open-webui:main
```

Connects to Ollama via `host.docker.internal:11434`. The `--restart always` flag satisfies the auto-start requirement.

---

## Results

Fine-tuning produced clear domain adaptation across all three test prompts:

| Prompt | Base Model | Fine-Tuned Model |
|---|---|---|
| CeraVe / dry skin / hydration | Fabricated review platforms, invented ratings | Real rating (2.4/5), verbatim customer quote, skin-type verdict |
| Neutrogena Hydro Boost / oily skin | Hallucinated reviewers, contradictory ratings | Focused negative verdict with specific complaint |
| L'Oréal Revitalift / sensitive skin | Generic summary, no grounding | Rating, price, customer quote, individual-variation caveat |

The fine-tuned model consistently follows the output structure: **rating → customer quote → verdict → caveat** — producing responses grounded in real review data rather than hallucinated content.
