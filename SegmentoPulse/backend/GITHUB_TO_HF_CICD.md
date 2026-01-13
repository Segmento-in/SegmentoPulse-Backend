# GitHub ↔ Hugging Face Spaces CI/CD Setup Guide

Complete guide to set up automatic deployment from GitHub to Hugging Face Spaces.

## Overview

This setup allows you to:
1. ✅ Push code to GitHub (your organization repo)
2. ✅ GitHub Actions automatically syncs to HF Spaces
3. ✅ HF Spaces rebuilds and deploys automatically

## Prerequisites

- ✅ GitHub repository: `https://github.com/Segmento-in/SegmentoPulse-Backend`
- ✅ HF Space: `https://huggingface.co/spaces/WORKWITHSHAFISK/segmentopulse-backend`
- ✅ Hugging Face account

## Step 1: Get Your Hugging Face Token

1. **Go to Hugging Face Settings**:
   - Navigate to: https://huggingface.co/settings/tokens

2. **Create a new token**:
   - Click "New token"
   - **Name**: `github-actions-segmentopulse`
   - **Type**: Select "Write" access
   - Click "Generate a token"

3. **Copy the token** (you'll need it in Step 3)
   - Format: `hf_xxxxxxxxxxxxxxxxxxxxx`
   - ⚠️ Save it somewhere safe - you can't see it again!

## Step 2: Push Backend to GitHub

```powershell
# Navigate to your backend directory
cd C:\Users\Dell\Desktop\Segmento-app-website-dev\SegmentoPulse\backend

# Initialize git (if not already done)
git init

# Add GitHub remote
git remote add origin https://github.com/Segmento-in/SegmentoPulse-Backend.git

# Add all files
git add .

# Commit
git commit -m "Initial commit: SegmentoPulse Backend"

# Push to GitHub
git push -u origin main
```

**If branch is 'master' instead of 'main'**:
```powershell
git branch -M main
git push -u origin main
```

## Step 3: Add HF Token to GitHub Secrets

1. **Go to your GitHub repository**:
   - https://github.com/Segmento-in/SegmentoPulse-Backend

2. **Navigate to Settings**:
   - Click "Settings" tab → "Secrets and variables" → "Actions"

3. **Add Repository Secret**:
   - Click "New repository secret"
   - **Name**: `HF_TOKEN`
   - **Value**: Paste your Hugging Face token from Step 1
   - Click "Add secret"

## Step 4: GitHub Actions Workflow (Already Created!)

The workflow file is already created at:
```
.github/workflows/deploy-to-hf.yml
```

This workflow:
- ✅ Triggers on every push to `main` branch
- ✅ Checks out your code
- ✅ Pushes to Hugging Face Spaces
- ✅ Can be manually triggered from GitHub Actions tab

## Step 5: Test the CI/CD Pipeline

### Initial Test

```powershell
# Make a small change
cd C:\Users\Dell\Desktop\Segmento-app-website-dev\SegmentoPulse\backend

# Edit a file (e.g., README.md)
Add-Content -Path README.md -Value "`n## Updated via GitHub"

# Commit and push
git add .
git commit -m "Test CI/CD pipeline"
git push
```

### Monitor Deployment

1. **Watch GitHub Actions**:
   - Go to: https://github.com/Segmento-in/SegmentoPulse-Backend/actions
   - You'll see "Sync to Hugging Face Spaces" workflow running

2. **Check HF Spaces**:
   - Go to: https://huggingface.co/spaces/WORKWITHSHAFISK/segmentopulse-backend
   - Click "Logs" tab to watch the rebuild

## How It Works

```mermaid
graph LR
    A[Developer] -->|git push| B[GitHub Repo]
    B -->|GitHub Actions| C[CI/CD Workflow]
    C -->|Sync Code| D[HF Spaces]
    D -->|Auto Rebuild| E[Live Backend API]
```

### Workflow Steps

1. **You push to GitHub**:
   ```bash
   git push origin main
   ```

2. **GitHub Actions triggers**:
   - Checks out your code
   - Configures git with HF token
   - Force pushes to HF Spaces

3. **HF Spaces detects changes**:
   - Automatically triggers rebuild
   - Runs Dockerfile
   - Deploys updated backend

4. **Your API is live**:
   - Updated code is running at: `https://workwithshafisk-segmentopulse-backend.hf.space`

## Workflow File Explanation

```yaml
name: Sync to Hugging Face Spaces

on:
  push:
    branches:
      - main          # Triggers on push to main
      - master        # Also triggers on master (if used)
  workflow_dispatch:   # Allows manual trigger

jobs:
  sync-to-huggingface:
    runs-on: ubuntu-latest
    
    steps:
      # Checkout your code
      - uses: actions/checkout@v3
        with:
          fetch-depth: 0  # Full history for proper sync
      
      # Sync to HF Spaces
      - env:
          HF_TOKEN: ${{ secrets.HF_TOKEN }}  # Uses secret
        run: |
          # Configure git
          git config --global user.email "github-actions[bot]@users.noreply.github.com"
          git config --global user.name "github-actions[bot]"
          
          # Add HF remote with token
          git remote add hf https://WORKWITHSHAFISK:$HF_TOKEN@huggingface.co/spaces/WORKWITHSHAFISK/segmentopulse-backend
          
          # Push to HF
          git push --force hf HEAD:main
```

## Development Workflow

### Daily Development Cycle

```powershell
# 1. Make changes
code app/main.py

# 2. Test locally
uvicorn app.main:app --reload

# 3. Commit changes
git add .
git commit -m "Add new feature"

# 4. Push to GitHub
git push

# 5. ✨ Automatic deployment!
# - GitHub Actions syncs to HF
# - HF Spaces rebuilds
# - New version goes live
```

### Manual Trigger

If you need to trigger deployment without pushing:

1. Go to: https://github.com/Segmento-in/SegmentoPulse-Backend/actions
2. Select "Sync to Hugging Face Spaces"
3. Click "Run workflow" → "Run workflow"

## Benefits

✅ **Single Source of Truth**: GitHub is your primary repository  
✅ **Automatic Deployment**: Push once, deploys everywhere  
✅ **Version Control**: Full Git history on GitHub  
✅ **Team Collaboration**: Multiple developers can contribute  
✅ **CI/CD Best Practices**: Automated testing and deployment  
✅ **Rollback Capability**: Easy to revert to previous versions  

## Troubleshooting

### Workflow Fails: "remote: Permission denied"

**Solution**: Check HF_TOKEN secret
- Ensure token has "Write" access
- Verify token hasn't expired
- Re-create token if needed

### Workflow Succeeds But HF Doesn't Update

**Solution**: Check HF Space logs
- Space may have build errors
- Check Dockerfile syntax
- Verify all files are committed

### Push Rejected: "Updates were rejected"

**Solution**: Force push from workflow
- Already configured in workflow with `--force`
- Ensures HF Spaces always matches GitHub

## Security Best Practices

✅ **Never commit tokens**: Use GitHub Secrets only  
✅ **Use write token only for CI/CD**: Don't use admin tokens  
✅ **Rotate tokens periodically**: Change tokens every 90 days  
✅ **Don't commit .env files**: Already in .gitignore  

## Monitoring

### GitHub Actions Dashboard
- **URL**: https://github.com/Segmento-in/SegmentoPulse-Backend/actions
- **Shows**: All workflow runs, status, logs

### HF Spaces Logs
- **URL**: https://huggingface.co/spaces/WORKWITHSHAFISK/segmentopulse-backend
- **Shows**: Build logs, runtime logs, errors

## Complete Setup Checklist

- [ ] Created HF token with write access
- [ ] Added HF_TOKEN to GitHub secrets
- [ ] Created .github/workflows/deploy-to-hf.yml
- [ ] Pushed code to GitHub
- [ ] Verified GitHub Actions workflow runs
- [ ] Confirmed HF Spaces rebuilds
- [ ] Tested backend API endpoint
- [ ] Updated frontend with backend URL

## Alternative: Hugging Face Can Also Sync FROM GitHub

Instead of GitHub → HF sync, you can also set up HF → Pull from GitHub:

1. Go to your HF Space settings
2. Under "Repository" → Click "Link to a GitHub repository"
3. Authorize and select your GitHub repo

**This creates bidirectional sync!**

## Next Steps

After setup:

1. ✅ **Test the workflow**: Make a small change and push
2. ✅ **Add branch protection**: Protect main branch on GitHub
3. ✅ **Add status badge**: Show build status in README
4. ✅ **Set up PR checks**: Run tests before merging

---

**You're all set!** Every push to GitHub will now automatically deploy to Hugging Face Spaces! 🚀
