# pg-batch-bench

## Usage

### 1. Setup VM

1. Copy `.env.example` to `.env` and fill in the values.

2. Run:

```bash
cd vm

export $(grep -v '^#' .env | xargs)

terraform init

terraform apply
```

### 2. Check if metrics are available

1. Get IP of VM:

```bash
terraform output -raw public_ip
```

2. Access metrics in browser:

Postgres Exporter: http://droplet_ip:9187/metrics

Node Exporter: http://droplet_ip:9100/metrics

### 3. Modify prometheus config

1. Replace `<DROPLET_IP>` in `prometheus.yml` with the IP of the VM.

### 4. Run benchmark

1. Build go binary

2. Scp binary and `init.sql` to VM

3. Run benchmark
