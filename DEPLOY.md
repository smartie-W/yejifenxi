# Deploy Guide

## 1. Push to GitHub

```bash
cd /Users/wang/Documents/codex/listed-supply-chain-mvp
git init
git add .
git commit -m "feat: production deploy setup"
git branch -M main
git remote add origin git@github.com:<your-account>/<your-repo>.git
git push -u origin main
```

## 2. Render (recommended for always-on)

- Create a Render Web Service from this GitHub repo.
- Render detects `render.yaml` and Dockerfile automatically.
- Health check: `/api/health`
- Keep one paid instance (`starter`) to avoid sleeping.

Recommended instance sizing for 10 concurrent query users:
- 1 vCPU / 2 GB RAM minimum
- If traffic increases, scale up vertically first.

## 3. Railway (alternative)

- New Project -> Deploy from GitHub repo.
- Keep service always-on (paid plan).
- Health check already set in `railway.json`.

## 4. Production checklist

- Ensure `HOST=0.0.0.0`
- Keep `NODE_ENV=production`
- Turn on auto deploy from GitHub
- Verify:

```bash
curl https://<your-domain>/api/health
```

