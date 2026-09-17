# Server deploy (EC2 / odcollection.etc-research.com)

These files let you rebuild the server after a new instance or `git clone`.

## What’s in this folder

| File | Purpose |
|---|---|
| `streamlit.service` | systemd unit for the Streamlit app (`:8501`) |
| `morning-od-sync.service` | One-shot OD sync for all projects |
| `morning-od-sync.timer` | Daily **05:00 America/Chicago** |
| `nginx-odcollection.conf` | Reverse proxy domain → Streamlit |
| `env.example` | Template for `.env` (no secrets) |
| `install_on_server.sh` | Copies units + restarts services |

Also in the repo (outside this folder):

- `scripts/morning_od_sync.py` — headless sync script
- `.streamlit/config.toml.example` — copy to `.streamlit/config.toml`
- `path/to/README.md` — where to put `key.p8`

**Never commit:** `.env`, `path/to/key.p8`, real AWS/Snowflake secrets.

---

## Fresh server checklist

### 1. Machine prep
- Ubuntu (Focal+), open SG ports: **22**, **80**, **443**, **8501**
- Route 53 A record `odcollection.etc-research.com` → this instance’s public/Elastic IP
- Prefer an **Elastic IP** so DNS doesn’t break on reboot/replace

### 2. Clone + Python env
```bash
cd ~
git clone <YOUR_REPO_URL> CR_Dashboard
cd CR_Dashboard

# Miniconda + env (once)
# ... install miniconda if needed ...
conda create -y -n crdash python=3.10
conda activate crdash
pip install --upgrade pip
pip install -r requirements.txt
# if missing: pip install plotly python-docx
```

### 3. Secrets (manual each time)
```bash
cp deploy/env.example .env
# edit .env with real values
vim .env
chmod 600 .env

mkdir -p path/to
# copy Snowflake JWT key into path/to/key.p8
chmod 600 path/to/key.p8

cp .streamlit/config.toml.example .streamlit/config.toml
mkdir -p logs
```

### 4. Install services from the repo
```bash
conda activate crdash
bash deploy/install_on_server.sh
```

### 5. HTTPS
```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d odcollection.etc-research.com --agree-tos -m YOUR_EMAIL@etcinstitute.com
# choose redirect (option 2)
```

### 6. Verify
```bash
sudo systemctl status streamlit nginx
curl -I http://127.0.0.1:8501
curl -I https://odcollection.etc-research.com
systemctl list-timers | grep morning-od
```

---

## Day-2 commands

```bash
cd ~/CR_Dashboard
git pull
conda activate crdash
pip install -r requirements.txt   # if deps changed
bash deploy/install_on_server.sh  # refresh units + restart

sudo systemctl restart streamlit
sudo journalctl -u streamlit -f
```

Morning sync manual test:
```bash
conda activate crdash
python scripts/morning_od_sync.py --dry-run
python scripts/morning_od_sync.py --project Oahu_Honolulu
sudo systemctl start morning-od-sync.service
tail -f logs/morning_od_sync.log
```

Log rotation (14 days) — already on server if you installed earlier; from repo:
```bash
sudo cp deploy/logrotate-morning-od-sync.conf /etc/logrotate.d/morning-od-sync
```

**Sync History** (super admins): dashboard → Management → Sync History (`/?page=sync_history`).
Stores runs in Snowflake `APP_CONFIG.OD_SYNC_*`, morning On/Off toggle, and email alerts for failures / configurable record-drop threshold.

---

## After `git pull` only (existing server)

If `.env`, `key.p8`, conda env, and systemd are already in place:

```bash
cd ~/CR_Dashboard
git pull
conda activate crdash
pip install -r requirements.txt
sudo systemctl restart streamlit
# if deploy/*.service changed:
bash deploy/install_on_server.sh
```
