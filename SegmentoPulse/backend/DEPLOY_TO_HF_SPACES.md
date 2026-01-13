# SegmentoPulse Backend - Hugging Face Spaces Deployment Guide

Complete guide to deploying the SegmentoPulse news aggregation backend to Hugging Face Spaces.

## Prerequisites

✅ Hugging Face account (free)  
✅ News API keys (GNews, NewsAPI, NewsData.io)  
✅ Git installed on your machine

## Step 1: Create a New Space on Hugging Face

1. **Go to**: https://huggingface.co/new-space

2. **Configure your Space**:
   - **Space name**: `segmentopulse-backend` (or your choice)
   - **License**: MIT
   - **SDK**: Select **Docker** 🐳
   - **Visibility**: Public or Private (your choice)

3. **Click "Create Space"**

## Step 2: Clone the Space Repository

```bash
# Clone your new Space
git clone https://huggingface.co/spaces/YOUR_USERNAME/segmentopulse-backend
cd segmentopulse-backend
```

## Step 3: Copy Backend Files

Copy the entire SegmentoPulse backend to your Space directory:

**On Windows (PowerShell)**:
```powershell
# Navigate to your Space directory
cd path\to\segmentopulse-backend

# Copy all backend files
Copy-Item -Path "C:\Users\Dell\Desktop\Segmento-app-website-dev\SegmentoPulse\backend\*" -Destination "." -Recurse -Force

# Remove .env file (don't commit secrets!)
Remove-Item -Path ".env" -ErrorAction SilentlyContinue

# Remove __pycache__ directories
Get-ChildItem -Path "." -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
```

**Your Space structure should look like**:
```
segmentopulse-backend/
├── .git/
├── .gitignore
├── Dockerfile              ✅ HF Spaces compatible
├── README.md               ✅ Space description
├── requirements.txt
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── config.py
│   ├── models.py
│   ├── routes/
│   └── services/
└── ...
```

## Step 4: Configure Environment Secrets

1. **Go to your Space settings**: https://huggingface.co/spaces/YOUR_USERNAME/segmentopulse-backend/settings

2. **Add Repository Secrets** (under "Variables and secrets" tab):

```bash
# News API Keys (REQUIRED)
GNEWS_API_KEY=4a3a51ecd8bed1d10dc2201c86207729
NEWSAPI_API_KEY=fb8702e8ff9f45b4ab8f2508890d505f
NEWSDATA_API_KEY=pub_9a26c29ebbdf46a1abde2fbfe05e4196

# Provider Priority (OPTIONAL - uses default if not set)
NEWS_PROVIDER_PRIORITY=gnews,newsapi,newsdata,google_rss

# Firebase (OPTIONAL)
FIREBASE_DATABASE_URL=https://dbsegpulse-fbc35-default-rtdb.asia-southeast1.firebasedatabase.app
FIREBASE_PROJECT_ID=dbsegpulse-fbc35

# Server Config (OPTIONAL - uses defaults)
ENVIRONMENT=production
HOST=0.0.0.0
PORT=7860
CORS_ORIGINS=http://localhost:3000,https://segmento.in
CACHE_TTL=120
```

**Important Notes**:
- ⚠️ Never commit API keys to Git - always use Spaces Secrets!
- ✅ The app will work with just the 3 news API keys
- ✅ Firebase and Redis are optional

## Step 5: Push to Hugging Face

```bash
# Add all files
git add .

# Commit
git commit -m "Initial SegmentoPulse backend deployment"

# Push to Hugging Face
git push
```

## Step 6: Monitor Deployment

1. **Go to your Space**: https://huggingface.co/spaces/YOUR_USERNAME/segmentopulse-backend

2. **Watch the build logs** in the "Logs" tab

3. **Build time**: ~2-5 minutes (first time)

4. **Look for**: 
```
Application startup complete.
Uvicorn running on http://0.0.0.0:7860
```

## Step 7: Test Your Deployment

Once deployed, your backend will be live at:
```
https://YOUR_USERNAME-segmentopulse-backend.hf.space
```

**Test endpoints**:

```bash
# Health check
curl https://YOUR_USERNAME-segmentopulse-backend.hf.space/health

# Provider statistics
curl https://YOUR_USERNAME-segmentopulse-backend.hf.space/api/news/system/stats

# Fetch AI news
curl https://YOUR_USERNAME-segmentopulse-backend.hf.space/api/news/ai
```

## Step 8: Update Frontend Environment Variable

Once your backend is deployed and working:

### For Vercel/Netlify Production:

Add environment variable:
```bash
NEXT_PUBLIC_PULSE_API_URL=https://YOUR_USERNAME-segmentopulse-backend.hf.space
```

### Example URLs:
```bash
# If your username is "workwithshafisk" and space is "segmentopulse-backend"
NEXT_PUBLIC_PULSE_API_URL=https://workwithshafisk-segmentopulse-backend.hf.space
```

## Troubleshooting

### Build Fails

**Check**:
1. Dockerfile syntax
2. All files copied correctly
3. requirements.txt is valid

**Solution**: Check build logs in HF Spaces for specific errors

### App Starts But No News

**Check**:
1. API keys are set in Spaces Secrets
2. API keys are valid (not expired)
3. Check provider stats endpoint: `/api/news/system/stats`

**Solution**: Verify secrets are set correctly in Space settingsimportância

### CORS Errors

**Check**: `CORS_ORIGINS` in Spaces Secrets includes your frontend domain

**Solution**: Add your production domain:
```bash
CORS_ORIGINS=https://segmento.in,http://localhost:3000
```

### 429 Rate Limit Errors

**This is NORMAL!** The hybrid system will:
1. Detect rate limit (HTTP 429)
2. Automatically switch to next provider
3. Continue serving news seamlessly

**Check**: `/api/news/system/stats` to see which providers are active

## Maintenance

### Update Deployment

```bash
# Make changes locally
# Commit and push
git add .
git commit -m "Update backend"
git push
```

HF Spaces will **automatically rebuild** and deploy!

### Monitor Usage

- Check `/api/news/system/stats` for provider health
- Monitor HF Spaces logs for errors
- Track rate limit usage

### Update API Keys

If a provider hits limits:
1. Get new API key
2. Update in Spaces Secrets
3. Restart Space (automatic)

## Performance Tips

✅ **Enable caching**: Redis optional but recommended  
✅ **Use provider priority**: Put fastest providers first  
✅ **Monitor stats**: Check which providers are used most  
✅ **Scale up**: Upgrade HF Spaces tier if needed

## Cost

🎉 **FREE TIER**:
- Hugging Face Spaces: FREE for Docker Spaces
- News APIs: All have free tiers (400 total requests/day)
- Total cost: **$0/month** ✨

## Next Steps

After deployment:

1. ✅ Test all API endpoints
2. ✅ Update frontend with new backend URL
3. ✅ Push frontend to production
4. ✅ Monitor provider statistics
5. ✅ Enjoy your live news aggregation platform! 🚀

## Support

- HF Spaces Docs: https://huggingface.co/docs/hub/spaces
- SegmentoPulse Backend GitHub: [Your repo]
- Issues: Create issue in your repository

---

**Deployment checklist**:
- [ ] Create HF Space (Docker SDK)
- [ ] Copy backend files
- [ ] Add API keys to Secrets
- [ ] Push to HF
- [ ] Test endpoints
- [ ] Update frontend env var
- [ ] Deploy frontend
- [ ] 🎉 Go live!
