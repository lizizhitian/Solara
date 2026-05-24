const express = require('express');
const Database = require('better-sqlite3');
const path = require('path');
const cors = require('cors');
const cookieParser = require('cookie-parser');
//const fetch = require('node-fetch');
const fs = require('fs');
const { pipeline } = require('stream');
const { promisify } = require('util');
const pipelineAsync = promisify(pipeline);
const axios = require('axios');
const NodeID3 = require('node-id3');
const MetaFlac = require('metaflac-js');
//const mm = require('music-metadata');
// const ffmpeg = require('fluent-ffmpeg'); // 移除 ffmpeg 依赖
const { buildPalette } = require('./lib/palette');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;
const dbPath = process.env.DB_PATH || path.join(__dirname, 'solara.db');//修正了数据库路径，确保在当前目录下创建 solara.db 文件
const NAS_DOWNLOAD_DIR = process.env.NAS_DOWNLOAD_DIR || path.join(__dirname, 'downloads');

// ======================【新增防风控核心依赖 开始】======================
const http = require('http');
const https = require('https');
const {LRUCache} = require('lru-cache');
const crypto = require('crypto');
const Semaphore = require('semaphore');
// ======================【新增防风控核心依赖 结束】======================

// ======================【新增连接池、缓存、限流、并发控制 开始】======================
// 全局TCP连接池，保障连接复用
const defaultAgentOptions = {
    keepAlive: true,
    maxSockets: 20,
    keepAliveMsecs: 60000
};

const defaultAgent = new https.Agent(defaultAgentOptions);
const agentCache = new Map();
function getAgentForUrl(urlString) {
    try {
        const urlObj = new URL(urlString);
        const key = `${urlObj.protocol}//${urlObj.hostname}`;
        if (agentCache.has(key)) return agentCache.get(key);

        const agentOptions = {
            keepAlive: true,
            maxSockets: urlObj.hostname.includes('gdstudio.xyz') ? 40 : 20,
            keepAliveMsecs: 60000,
            maxFreeSockets: 10,
        };

        const agent = urlObj.protocol === 'https:' ? new https.Agent(agentOptions) : new http.Agent(agentOptions);
        agentCache.set(key, agent);
        return agent;
    } catch (err) {
        return defaultAgent;
    }
}

// 复用的 axios 实例，统一配置 agent 与超时
const axiosInstance = axios.create({
    httpAgent: defaultAgent,
    httpsAgent: defaultAgent,
    timeout: 20000,
    headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; Solara/1.0)'
    },
    responseType: 'json',
});

const metrics = {
    apiRequests: 0,
    upstreamRequests: 0,
    cacheHits: 0,
    cacheMisses: 0,
    pendingHits: 0,
    retryCount: 0,
    successfulRequests: 0,
    failedRequests: 0,
    activeRequests: 0,
    totalResponseTimeMs: 0,
};

// LRU请求缓存，减少重复请求
const cache = new LRUCache({
    max: 500,
    ttl: 5 * 60 * 1000
});

// 并行请求去重容器
const pending = new Map();

// 最大并发请求数限制
const semaphore = Semaphore(5);

// 5分钟滑动窗口限流：最多50次请求
const rateLimit = {
    window: 300 * 1000,
    max: 50,
    list: []
};

// 限流等待函数
async function throttle() {
    const now = Date.now();
    rateLimit.list = rateLimit.list.filter(t => now - t < rateLimit.window);
    if (rateLimit.list.length >= rateLimit.max) {
        await new Promise(r => setTimeout(r, 500));
        return throttle();
    }
    rateLimit.list.push(now);
}

// 带重试、连接复用的请求函数
async function fetchWithRetry(url, opts, retries = 3) {
    await throttle();
    const startTime = Date.now();
    metrics.upstreamRequests += 1;
    metrics.activeRequests += 1;
    const agent = getAgentForUrl(url);

    try {
        const response = await axiosInstance.request({ url, httpAgent: agent, httpsAgent: agent, ...opts });
        const duration = Date.now() - startTime;
        metrics.successfulRequests += 1;
        metrics.totalResponseTimeMs += duration;
        return response;
    } catch (err) {
        if (retries > 0 && err.response && err.response.status >= 500) {
            metrics.retryCount += 1;
            const delay = Math.min(1000 * (2 ** (3 - retries)) + Math.random() * 500, 30000);
            await new Promise(r => setTimeout(r, delay));
            return fetchWithRetry(url, opts, retries - 1);
        }
        metrics.failedRequests += 1;
        throw err;
    } finally {
        metrics.activeRequests -= 1;
    }
}
// ======================【新增连接池、缓存、限流、并发控制 结束】======================

// 确保下载目录存在
if (!fs.existsSync(NAS_DOWNLOAD_DIR)) {
  fs.mkdirSync(NAS_DOWNLOAD_DIR, { recursive: true });
}

// 初始化数据库
const db = new Database(dbPath);
db.exec(`
  CREATE TABLE IF NOT EXISTS playback_store (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS favorites_store (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  );
`);

app.use(cors());
app.use(express.json());
app.use(cookieParser());

// 静态文件服务
app.use(express.static(path.join(__dirname)));

// 存储接口 (SQLite 替代原 Cloudflare D1)
app.get('/api/storage', (req, res) => {
  const { status, keys: keysParam } = req.query;

  if (status) {
    return res.json({ remoteAvailable: true });
  }

  const keys = (keysParam || '').split(',').map(k => k.trim()).filter(Boolean);
  const data = {};

  const favoriteKeys = new Set(["favoriteSongs", "currentFavoriteIndex", "favoritePlayMode", "favoritePlaybackTime"]);
  
  try {
    if (keys.length > 0) {
      keys.forEach(key => {
        const table = favoriteKeys.has(key) ? 'favorites_store' : 'playback_store';
        const row = db.prepare(`SELECT value FROM ${table} WHERE key = ?`).get(key);
        data[key] = row ? row.value : null;
      });
    }
    res.json({ remoteAvailable: true, data });
  } catch (error) {
    console.error('Storage get error:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/api/storage', (req, res) => {
  const { data } = req.body;
  if (!data) return res.status(400).json({ error: 'Missing data' });

  const favoriteKeys = new Set(["favoriteSongs", "currentFavoriteIndex", "favoritePlayMode", "favoritePlaybackTime"]);

  try {
    const upsertPlayback = db.prepare(`INSERT OR REPLACE INTO playback_store (key, value) VALUES (?, ?)`);
    const upsertFavorites = db.prepare(`INSERT OR REPLACE INTO favorites_store (key, value) VALUES (?, ?)`);

    const transaction = db.transaction((items) => {
      for (const [key, value] of Object.entries(items)) {
        if (favoriteKeys.has(key)) {
          upsertFavorites.run(key, typeof value === 'string' ? value : JSON.stringify(value));
        } else {
          upsertPlayback.run(key, typeof value === 'string' ? value : JSON.stringify(value));
        }
      }
    });

    transaction(data);
    res.json({ success: true });
  } catch (error) {
    console.error('Storage post error:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.delete('/api/storage', (req, res) => {
  const { keys: queryKeys } = req.query;
  const { keys: bodyKeys } = req.body;
  
  let keys = [];
  if (queryKeys) {
    keys = queryKeys.split(',').map(k => k.trim()).filter(Boolean);
  } else if (Array.isArray(bodyKeys)) {
    keys = bodyKeys;
  }

  if (keys.length === 0) return res.json({ success: true });

  const favoriteKeys = new Set(["favoriteSongs", "currentFavoriteIndex", "favoritePlayMode", "favoritePlaybackTime"]);

  try {
    const deletePlayback = db.prepare(`DELETE FROM playback_store WHERE key = ?`);
    const deleteFavorites = db.prepare(`DELETE FROM favorites_store WHERE key = ?`);

    const transaction = db.transaction((targetKeys) => {
      for (const key of targetKeys) {
        if (favoriteKeys.has(key)) {
          deleteFavorites.run(key);
        } else {
          deletePlayback.run(key);
        }
      }
    });

    transaction(keys);
    res.json({ success: true });
  } catch (error) {
    console.error('Storage delete error:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Proxy 接口
const API_BASE_URL = "https://music-api.gdstudio.xyz/api.php";

async function embedMetadata(filePath, song, providedPicUrl) {
    if (!song) return;
    const fileExt = path.extname(filePath).toLowerCase();
    const tempCoverPath = filePath + '.cover.jpg';
    
    try {
        // 1. 获取封面图片
        let imageBuffer = null;
        
        // // 优先使用前端传来的经过代理处理的 URL，如果不可用则回退到拼接 URL
        // const signature = Math.random().toString(36).substring(2, 15);
        // const fallbackPicUrl = `${API_BASE_URL}?types=pic&id=${song.pic_id}&source=${song.source || "netease"}&size=300&s=${signature}`;
        // let picUrl = providedPicUrl || (song.pic_id ? fallbackPicUrl : null);
        let picUrl = providedPicUrl || null;

        // 修复：如果 picUrl 是相对路径（如 /proxy...），转换为绝对路径
        if (picUrl && picUrl.startsWith('/')) {
            picUrl = `http://localhost:${port}${picUrl}`;
        }

        if (picUrl) {
            try {
                // 如果是本机的代理 URL，尝试直接抓取原始 URL 或修正 URL
                if (picUrl.includes('localhost') && picUrl.includes('target=')) {
                    const urlObj = new URL(picUrl);
                    const target = urlObj.searchParams.get('target');
                    if (target) {
                        picUrl = target;
                    }
                }

                // 根据用户提供的抓包信息，完美模拟浏览器 Headers
                const imgRes = await axios.get(picUrl, { 
                    responseType: 'arraybuffer', 
                    timeout: 20000, // 增加到 20s，应对 3s 甚至更慢的加载
                    headers: { 
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                        'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
                        'Referer': picUrl.includes('kuwo.cn') ? 'https://www.kuwo.cn/' : (picUrl.includes('netease') ? 'https://music.163.com/' : 'https://www.google.com/'),
                        'Cache-Control': 'no-cache'
                    }
                });
                
                const contentType = imgRes.headers['content-type'] || '';
                let buffer = Buffer.from(imgRes.data);
                
                // 如果返回的是 JSON (某些源的 proxy 接口返回的是包含真实 URL 的 JSON)
                if (contentType.includes('application/json') || (buffer.length < 1000 && buffer.toString().trim().startsWith('{'))) {
                    try {
                        const json = JSON.parse(buffer.toString());
                        if (json.url) {
                            const realImgRes = await axios.get(json.url, {
                                responseType: 'arraybuffer',
                                timeout: 15000,
                                headers: {
                                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                                    'Referer': json.url.includes('kuwo.cn') ? 'https://www.kuwo.cn/' : 'https://music.163.com/'
                                }
                            });
                            buffer = Buffer.from(realImgRes.data);
                        }
                    } catch (e) {
                        console.warn('Failed to parse JSON cover response or fetch real image:', e.message);
                    }
                }

                    if (buffer.length > 1000) {
                        imageBuffer = buffer;
                        await fs.promises.writeFile(tempCoverPath, imageBuffer);
                    } else {
                    console.warn(`Invalid cover image data: ${buffer.length} bytes.`);
                }
            } catch (err) {
                console.warn(`Failed to fetch cover image from ${picUrl}:`, err.message);
            }
        }

        // 2. 获取歌词
        let lyric = '';
        // if (song.lyric_id || song.id) {
        //     const signature = Math.random().toString(36).substring(2, 15);
        //     const lrcUrl = `${API_BASE_URL}?types=lyric&id=${song.lyric_id || song.id}&source=${song.source || "netease"}&s=${signature}`;
        //     try {
        //         const lrcRes = await axios.get(lrcUrl, { timeout: 10000 });
        //         if (lrcRes.data && lrcRes.data.lyric) {
        //             lyric = lrcRes.data.lyric;
        //         }
        //     } catch (err) {
        //         console.warn('Failed to fetch lyrics:', err.message);
        //     }
        // }

        const artistStr = Array.isArray(song.artist) ? song.artist.join(', ') : (song.artist || '');

        // 3. 根据格式嵌入元数据
        if (fileExt === '.mp3') {
            const tags = {
                title: song.name,
                artist: artistStr,
                album: song.album || '',
                unsynchronisedLyrics: {
                    language: 'eng',
                    text: lyric
                }
            };
            if (imageBuffer) {
                tags.image = {
                    mime: "image/jpeg",
                    type: { id: 3, name: 'front cover' },
                    description: 'front cover',
                    imageBuffer: imageBuffer
                };
            }
            // 尝试使用 ID3v2.3 增加兼容性并写入
            const options = {
                include: ['TALB', 'TIT2', 'TPE1', 'USLT', 'APIC'],
                noAutoTag: false
            };
            try {
                const success = NodeID3.write(tags, filePath, options);
                if (!success) console.warn('NodeID3.write returned false or failed to write tags');
            } catch (id3Err) {
                console.error('ID3 write failed:', id3Err && id3Err.message ? id3Err.message : id3Err);
            }
        } else if (fileExt === '.flac') {
            try {
                const flac = new MetaFlac(filePath);
                // 强制清除旧标签以防冲突
                flac.removeAllTags();
                
                flac.setTag(`TITLE=${song.name}`);
                flac.setTag(`ARTIST=${artistStr}`);
                flac.setTag(`ALBUM=${song.album || ''}`);
                if (lyric) {
                    // FLAC 标准歌词标签
                    flac.setTag(`LYRICS=${lyric}`);
                    flac.setTag(`DESCRIPTION=${lyric}`); // 增加兼容性
                }
                if (imageBuffer) {
                    flac.importPictureFromBuffer(imageBuffer);
                }
                flac.save();
            } catch (flacError) {
                console.error('FLAC Metadata embedding failed:', flacError.message);
            }
        }
    } catch (error) {
        console.error('Metadata embedding failed:', error);
    } finally {
        // 清理临时封面文件
        if (fs.existsSync(tempCoverPath)) {
            try { fs.unlinkSync(tempCoverPath); } catch(e) {}
        }
    }
}

// app.get('/proxy', async (req, res) => {
//   const targetUrl = req.query.target;

//   // 处理音频代理 (酷我)
//   if (targetUrl && targetUrl.includes('kuwo.cn')) {
//     try {
//       const response = await fetch(targetUrl, {
//         headers: {
//           'User-Agent': req.headers['user-agent'] || 'Mozilla/5.0',
//           'Referer': 'https://www.kuwo.cn/'
//         }
//       });
//       res.set('Access-Control-Allow-Origin', '*');
//       res.set('Content-Type', response.headers.get('content-type'));
//       return response.body.pipe(res);
//     } catch (error) {
//       return res.status(500).send('Proxy error');
//     }
//   }

//   // 处理 API 代理
//   const url = new URL(API_BASE_URL);
//   Object.keys(req.query).forEach(key => {
//     if (key !== 'target') {
//       url.searchParams.set(key, req.query[key]);
//     }
//   });

//   try {
//     const response = await fetch(url.toString(), {
//       headers: {
//         'User-Agent': req.headers['user-agent'] || 'Mozilla/5.0',
//         'Referer': 'https://music.gdstudio.xyz/'
//       }
//     });
//     const data = await response.text();
//     res.set('Access-Control-Allow-Origin', '*');
//     res.set('Content-Type', 'application/json');
//     res.send(data);
//   } catch (error) {
//     res.status(500).send('API Proxy error');
//   }
// });

// ==================== 【优化修改】Proxy 接口（仅替换这里，其余完全不变） ====================
app.get('/proxy', async (req, res) => {
    const targetUrl = req.query.target;

    // 处理音频代理 (酷我)
    if (targetUrl && targetUrl.includes('kuwo.cn')) {
        try {
            const response = await fetch(targetUrl, {
                headers: {
                    'User-Agent': req.headers['user-agent'] || 'Mozilla/5.0',
                    'Referer': 'https://www.kuwo.cn/'
                }
            });
            res.set('Access-Control-Allow-Origin', '*');
            res.set('Content-Type', response.headers.get('content-type'));
            await pipelineAsync(response.body, res);
            return;
        } catch (error) {
            console.error('API Proxy error (kuwo):', error && error.message ? error.message : error);
            res.status(500).json({ error: 'API Proxy error', message: error && error.message ? error.message : String(error) });
        }
    }

    // 处理 API 代理
    try {
        const urlObj = new URL(API_BASE_URL);
        Object.keys(req.query).forEach(key => {
            if (key !== 'target' && key !== 's') urlObj.searchParams.set(key, req.query[key]);
        });

        const cacheKey = crypto.createHash('md5').update(urlObj.toString()).digest('hex');
        const type = urlObj.searchParams.get('types');
        metrics.apiRequests += 1;

        // 缓存
        const cached = cache.get(cacheKey);
        if (cached) {
            metrics.cacheHits += 1;
            res.set('Access-Control-Allow-Origin', '*');
            return res.send(cached);
        }
        metrics.cacheMisses += 1;

        // 去重
        if (pending.has(cacheKey)) {
            metrics.pendingHits += 1;
            const result = await pending.get(cacheKey);
            res.set('Access-Control-Allow-Origin', '*');
            return res.send(result);
        }

        // 并发 + 重试 + 连接复用
        const promise = new Promise((resolve, reject) => {
            semaphore.take(async () => {
                try {
                    const resp = await fetchWithRetry(urlObj.toString(), {
                        timeout: 15000,
                        headers: {
                            'User-Agent': req.headers['user-agent'] || 'Mozilla/5.0',
                            'Accept': 'application/json'
                        }
                    });
                    resolve(resp);
                } catch (e) {
                    reject(e);
                } finally {
                    semaphore.leave();
                }
            });
        });

        pending.set(cacheKey, promise);
        promise.finally(() => pending.delete(cacheKey));
        const response = await promise;
        const text = typeof response.data === 'string' ? response.data : JSON.stringify(response.data);

        // 不缓存播放地址
        const isValid = response.status === 200 && text.trim() !== '[]' && !text.includes('"error"');
        const canCache = type !== 'url' && isValid;
        if (canCache) cache.set(cacheKey, text);

        res.set('Access-Control-Allow-Origin', '*');
        res.set('Content-Type', 'application/json');
        res.send(text);
        //pending.delete(cacheKey);

    } catch (error) {
        res.status(500).send('API Proxy error');
    }
});
// ==================== 【优化修改】结束 ====================

// 调色板接口 (Palette)
app.get('/api/metrics', (req, res) => {
    const averageResponseTimeMs = metrics.successfulRequests > 0
        ? Math.round(metrics.totalResponseTimeMs / metrics.successfulRequests)
        : 0;

    res.json({
        apiRequests: metrics.apiRequests,
        upstreamRequests: metrics.upstreamRequests,
        cacheHits: metrics.cacheHits,
        cacheMisses: metrics.cacheMisses,
        pendingHits: metrics.pendingHits,
        retryCount: metrics.retryCount,
        successfulRequests: metrics.successfulRequests,
        failedRequests: metrics.failedRequests,
        activeRequests: metrics.activeRequests,
        cacheSize: cache.size,
        pendingCount: pending.size,
        averageResponseTimeMs,
        agentPools: Array.from(agentCache.keys()),
    });
});

app.get('/palette', async (req, res) => {
    const imageUrl = req.query.image || req.query.url;
    if (!imageUrl) return res.status(400).send('Missing image URL');
    
    try {
        const response = await fetch(imageUrl);
        if (!response.ok) throw new Error(`Fetch failed: ${response.status}`);
        
        const contentType = response.headers.get('content-type') || '';
        if (!contentType.startsWith('image/')) {
            return res.status(415).json({ error: 'Unsupported content type' });
        }

        const buffer = await response.arrayBuffer();
        const palette = await buildPalette(Buffer.from(buffer), contentType);
        palette.source = imageUrl;

        res.set('Cache-Control', 'public, max-age=3600');
        res.json(palette);
    } catch (error) {
        console.error('Palette generation failed:', error);
        res.status(500).json({ error: 'Failed to analyze image' });
    }
});

// NAS 下载接口
app.post('/api/nas-download', async (req, res) => {
    let { url, filename, song, picUrl } = req.body;
    if (!url || !filename) return res.status(400).json({ error: 'Missing url or filename' });

    // 清洗文件名，防止非法字符（尤其是 /）导致路径解析错误
    const safeFilename = filename.replace(/[\/\\?%*:|"<>]/g, '-');

    try {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`Fetch failed: ${response.status}`);

        const filePath = path.join(NAS_DOWNLOAD_DIR, safeFilename);
        const fileStream = fs.createWriteStream(filePath);
        try {
            await pipelineAsync(response.body, fileStream);
            // 嵌入元数据，增加 picUrl 参数
            await embedMetadata(filePath, song, picUrl);
            res.json({ success: true, path: filePath });
        } catch (err) {
            console.error('NAS file stream or metadata error:', err && err.message ? err.message : err);
            // 清理可能存在的残留文件
            try { if (fs.existsSync(filePath)) fs.unlinkSync(filePath); } catch(e){}
            res.status(500).json({ error: 'Failed to save file to NAS' });
        }
    } catch (error) {
        console.error('NAS download failed:', error);
        res.status(500).json({ error: 'Failed to download file to NAS' });
    }
});

// 浏览器下载接口：嵌入元数据后返回文件流
app.post('/api/download', async (req, res) => {
    const { url, filename, song, picUrl } = req.body;
    if (!url || !filename) {
        return res.status(400).json({ error: 'Missing url or filename' });
    }

    // 清洗文件名，防止非法字符（尤其是 /）导致路径解析错误
    const safeFilename = filename.replace(/[\/\\?%*:|"<>]/g, '-');

    const tempDir = path.join(__dirname, 'temp_downloads');
    if (!fs.existsSync(tempDir)) {
        fs.mkdirSync(tempDir, { recursive: true });
    }

    const tempFilePath = path.join(tempDir, `${Date.now()}_${safeFilename}`);
    
    try {
        // 1. 下载原始音频文件
        const response = await axios({
            url: url,
            method: 'GET',
            responseType: 'stream',
            timeout: 30000,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': url.includes('kuwo.cn') ? 'https://www.kuwo.cn/' : 'https://music.163.com/'
            }
        });

        const writer = fs.createWriteStream(tempFilePath);
        try {
            await pipelineAsync(response.data, writer);
        } catch (streamErr) {
            console.error('Error writing temp file stream:', streamErr && streamErr.message ? streamErr.message : streamErr);
            if (fs.existsSync(tempFilePath)) {
                try { fs.unlinkSync(tempFilePath); } catch(e){}
            }
            return res.status(500).json({ error: 'Failed to download file' });
        }

        // 2. 嵌入元数据
        await embedMetadata(tempFilePath, song, picUrl);

        // 3. 发送文件给浏览器
        res.download(tempFilePath, safeFilename, (err) => {
            if (err) {
                console.error('Error sending file:', err);
            }
            // 4. 清理临时文件
            if (fs.existsSync(tempFilePath)) {
                try { fs.unlinkSync(tempFilePath); } catch(e) {}
            }
        });
    } catch (error) {
        console.error('Browser download failed:', error);
        if (fs.existsSync(tempFilePath)) {
            try { fs.unlinkSync(tempFilePath); } catch(e) {}
        }
        res.status(500).json({ error: 'Failed to process download' });
    }
});

app.listen(port, () => {
  console.log(`Solara server running at http://localhost:${port}`);
  console.log(`Using SQLite database at: ${path.resolve(dbPath)}`);
});
