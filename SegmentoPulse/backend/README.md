---
title: SegmentoPulse Backend
emoji: 📰
colorFrom: blue
colorTo: purple
sdk: docker
pinned: false
license: mit
---

# SegmentoPulse Backend API

Real-time technology intelligence platform powered by hybrid multi-provider news aggregation.

## Features

- 🔄 **Hybrid News API System**: Automatic failover between GNews, NewsAPI, NewsData.io, and Google News RSS
- 🚀 **Ultra-Low Latency**: Multi-provider approach ensures fast response times
- 📊 **Smart Caching**: Redis-backed caching for optimal performance
- 🔥 **Firebase Integration**: Real-time database support for view counting
- 📡 **Multiple Categories**: AI, Data Security, Cloud Computing, and more

## API Endpoints

- `GET /api/news/{category}` - Fetch news by category
- `GET /api/news/system/stats` - Monitor provider health and statistics
- `GET /api/search?q={query}` - Search news articles
- `GET /health` - Health check endpoint

## Configuration

This Space requires environment secrets for news API providers:

1. **GNEWS_API_KEY** - Get from https://gnews.io
2. **NEWSAPI_API_KEY** - Get from https://newsapi.org
3. **NEWSDATA_API_KEY** - Get from https://newsdata.io

Firebase credentials (optional):
- **FIREBASE_DATABASE_URL**
- **FIREBASE_PROJECT_ID**

## Usage

```bash
# Fetch AI news
curl https://your-space-name.hf.space/api/news/ai

# Check provider stats
curl https://your-space-name.hf.space/api/news/system/stats
```

## Local Development

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

---

Built with FastAPI | Deployed on Hugging Face Spaces
