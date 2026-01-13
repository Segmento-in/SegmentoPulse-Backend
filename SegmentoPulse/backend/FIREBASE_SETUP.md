# Firebase Admin SDK Credentials for SegmentoPulse Backend

## Overview
The SegmentoPulse backend uses Firebase Realtime Database from the `dbsegpulse-fbc35` project (same as seg-pulse).

## Configuration Status
✅ **Database URL**: `https://dbsegpulse-fbc35-default-rtdb.asia-southeast1.firebasedatabase.app`  
✅ **Project ID**: `dbsegpulse-fbc35`  

## Service Account Credentials (Optional)

For the backend to work with Firebase Admin SDK, you need a service account credentials JSON file.

### How to Get Your Service Account Key:

1. **Go to Firebase Console**: https://console.firebase.google.com/
2. **Select your project**: `dbsegpulse-fbc35`
3. **Navigate to**: Project Settings (⚙️ gear icon) → Service Accounts
4. **Click**: "Generate new private key" button
5. **Download** the JSON file
6. **Rename** it to `firebase-credentials.json`
7. **Place** it in the backend directory: `c:\Users\Dell\Desktop\Segmento-app-website-dev\SegmentoPulse\backend\firebase-credentials.json`

### Important Security Notes:

> ⚠️ **DO NOT commit** `firebase-credentials.json` to Git  
> ⚠️ This file contains sensitive credentials  
> ⚠️ It should already be in `.gitignore`

## Current Status

The backend is configured to use Firebase but will show a warning if the credentials file is missing:
```
Firebase initialization error: [Errno 2] No such file or directory: './firebase-credentials.json'
```

This is **optional** - the backend will continue to work without Firebase. Firebase is only needed if you're using:
- User authentication tracking
- View count persistence
- Analytics data storage

## Alternative: Use Frontend Firebase Config Only

The seg-pulse and frontend already have Firebase initialized with client-side SDK credentials. If you don't need server-side Firebase features, the backend can work without the credentials file.
