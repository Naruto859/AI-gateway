# claude-branch_22 — audit, bug fixes and latency work by Claude

**Branch:** `claude-branch_22` (branched from `branch_21` @ `e593d99`)
**Author:** Claude (Anthropic), working under the repo owner's direction
**Date:** 2026-08-27 → 2026-08-28

Every change here was developed and tested in an isolated sandbox
(`/root/sandbox-gw21`, port 9787) against a copy of the production database. The
live deployment (docker `ciel-gateway-v7`, port 8787) was never restarted or
rebuilt while this work was done.

## What this branch contains, in short

- **Routing Order panel** — the exact upstream try-order is now visible and
  reorderable, with per-endpoint success/latency stats. Previously only the
  "primary" star was visible, so the fallback order was guesswork.
- **~35 bug fixes** across the dashboard, the forwarder, the hedger and the DB layer.
  The notable ones: 9 HTML-injection sites, a modal that permanently locked page
  scroll, silent data loss when saving an endpoint whose API key contained a quote,
  a proxy list that claimed 15k rows but only ever loaded 200, and hedging that
  credited/blamed the wrong proxy on every raced request.
- **Latency work** — most latency was being burned in failed retries, not in
  generation. Measured, then reduced by per-endpoint proxy chains, a dedicated-proxy
  cooldown, and honest defaults. See "Latency" and "Round 3" below.
- **Latency *measurement*** was itself wrong (counted attempts as requests, and
  ignored the time spent by failed requests entirely). Fixed in Round 3.

Nothing in this branch changes the live deployment. Merging is the owner's call.

---

## Sandbox used for development

Sandbox: `/root/sandbox-gw21/`
Source: `branch_21` @ `e593d99` (cloned, original untouched)
Runs on: `127.0.0.1:9787`
Also hosted for review at `https://ceil.indevs.in` (staging; separate from live).

## Rules followed during this work
- Live Docker `ciel-gateway-v7` (port 8787): **read only**. No restart, no rebuild.
- `/root/AI-gateway-v7`: **read only**. All edits live in `sandbox-gw21/repo`.
- Nothing deleted.

## Change applied to LIVE (approved by user, DB-only, no container restart)
`api.justwoker.icu` priority 11 → 6, so it is tried before `gorouter.app`
(gorouter is low on credit). Done via the admin API; container stayed `Up`.
Backup of the previous order: `analysis/endpoint_priority_backup_20260827_114708.txt`

---

## Feature added: Routing Order panel (the transparency request)

Before: only the primary star was visible, so the fallback order was invisible.

New `GET /admin/routing` returns the exact try-order plus per-endpoint outcomes.
New `POST /admin/endpoint/reorder` persists an explicit order.
`db.set_primary_endpoint()` now moves the endpoint to `priority 0` instead of only
setting a display flag — previously the star and the real routing order could disagree.

Panel (Endpoints tab) shows per endpoint: position number (enabled only), PRIMARY
tag, proxy chain (`Scrape.do → Custom 1 → Free pool`), a **NO FALLBACK** warning,
served/failed counts, success %, median latency, last-used time. Reorder by drag
(SortableJS) or ▲▼ buttons; changes are a draft until *Save order*, and a
background refresh cannot wipe an unsaved draft.

Verified: `forwarder._targets()` was called directly and its order matched the
panel exactly, before and after a reorder.

---

## Bugs fixed

### Critical
1. Modal backdrop click permanently locked page scroll and leaked a 2s poll
   forever (a generic `.modal` `onclick` clobbered the Hot Pool close handler).
   All dismissal now routes through `closeModal()` with per-modal cleanup.
2. **9 HTML-injection sites** — proxy url/status/ptype, log note/endpoint/detail,
   keyword value/replacement, api-key name/label, endpoint-modal
   name/url/api_key/model_override/scrape_do_token/custom proxies, csel option
   labels, hot-pool urls + log lines. A single `"` in any of these broke out of the
   attribute. Added one `esc()` helper and applied it everywhere.
3. API-key copy button read the key from an HTML attribute, so a key containing a
   quote silently copied a truncated, unusable key. Now reads from the array.
4. Endpoint-settings modal truncated any field containing `">` and then **saved the
   truncated value back to the DB** — silent data loss.

### Major
5. `connect_timeout` was bound to two inputs on two cards; the wrong one won.
   The Proxy Engine card now mirrors the value read-only.
6. `proxy_scanner_enabled` was missing from `DEFAULT_SETTINGS`, so the toggle
   painted ON for a setting that did not exist. Added, plus 6 duplicate literal
   keys in `DEFAULT_SETTINGS` removed (later value was silently winning).
7. Custom-proxy priority used positional `custom_N` ids, so deleting a row
   silently reordered or dropped the saved priority. Now keyed by stable uid and
   remapped to `custom_N` only at save time.
8. SortableJS was re-created on every `renderPriority()` without destroying the old
   instance — one drag fired `onEnd` N times. Now destroyed first.
9. `render()` re-initialised `.csel` widgets while a dropdown was open, stranding
   the popup in `<body>` as an unclickable floating menu. Now closed first.
10. 5 toggles + 5 save buttons had no error handling: a failed save cleared the
    dirty flag and mutated local state, so the UI showed a value the DB never had.
    All now confirm-then-paint via `bindToggle()` / `saveSettings()`.
11. The 12s auto-refresh swapped `ST.endpoints` under an open modal, so the modal
    saved against a stale object. Refresh now pauses while any modal is open.
12. Proxy list said "15,689 total" but only ever loaded 200 rows; search reported
    "0 match" for proxies that existed. Added `GET /admin/proxies` with SQL-side
    search + paging (verified: 1,846 socks5 matches across 15,738 rows).
13. Endpoint API key was rendered `type="text"` in the modal. Now `password` with
    a reveal toggle.
14. The endpoint 🧪 Test button was missing from the row template (its handler
    existed but could never run). Restored, and Test/Chat now use the endpoint's
    own `model_override` instead of the global `model_note`.

### Minor
15. `#rotInfo`, `#hpStatus` were always empty; `#hstatus` was hard-coded "Online"
    even when the connection was down. All three now reflect real state.
16. `maximum-scale=1.0` disabled pinch-zoom on mobile — removed.
17. Bulk-paste textarea collapsed to ~186px (`.wide` only matched `input`).
18. `min`/`max` on 3 numeric inputs contradicted the shipped defaults, so valid
    saved values rendered as browser-invalid.
19. Proxy-add box: input now flexes, delete button is a fixed 30px square.
20. csel popup was clipped off-screen at 360px — now clamped to the viewport.
21. No Escape-key handler for modals — added.
22. "Max 3 custom proxies" toast used a nonexistent style key (`err` → `bad`).

## Tests
- `bash /tmp/verify_fixes.sh` — 14/14 API assertions pass
- `node /tmp/ro_logic_test.js` — 14/14 routing-order draft-logic assertions
- `node /tmp/xss_render_test.js` — 11/11 escaping assertions
- `_strip_thinking` — 8/8 shape assertions
- `_skip_backoff` — 19/19 classification assertions, plus an old-vs-new diff over
  all 41 distinct failure reasons found in the live logs
- `node --check` on the extracted dashboard JS, `ast.parse` on all Python
- End-to-end through the sandbox gateway: non-stream 200 in 4.4s, and a streaming
  request carrying a `thinking` block returned a complete, spec-valid SSE
- Hostile values were inserted into the sandbox DB, rendering verified, then removed

---

## Backend fixes (forwarder.py)

23. **`_strip_thinking` traded one 400 for another.** A turn that was only thinking
    blocks was left as `[{"type":"text","text":""}]`, and agentrouter rejects that
    empty text. Now the turn is dropped and same-role neighbours are merged.
    Verified live: old shape → 500, new shape → 200 in 3.1s.
24. **`_consume_assemble` called `mark_bad`/`mark_good` unguarded** while every
    other call site checks `type(p["id"]) == int`. With a dedicated proxy the id is
    a string (`"scrape"`, `"custom_0"`), so the health update silently no-opped on
    `WHERE id=?`. All five calls now guard on `pooled`.
25. **Retry backoff was decided by substring-sniffing** `"Error" in detail`. Replaced
    with an explicit `_skip_backoff()` classifier. Measured over the 328 retry events
    in the live log window, 13 reasons changed decision — all of them proxy-local
    (`waf`, `truncated`, `incomplete`, `conn`, `empty-stream`, `400/502 proxy switch`)
    where the next attempt uses a different exit IP and waiting bought nothing.
    Saving at this box's settings (cap 0.5s) is only ~6s; at the shipped defaults
    (initial 1.0s, cap 8.0s) the same events cost 556s → 490s.

    Correction to an earlier claim of mine: the old condition did NOT skip backoff
    "almost always" — it correctly waited on bare `500`/`502`/`503`/`524`, because
    those strings contain no "Error". The real defect was the opposite: it waited on
    proxy-local failures where a fresh IP was already being used.

---

## Hosted staging deployment (आपके domain पर)

**URL: https://ceil.indevs.in** — dashboard password is set in the sandbox DB
(not recorded here). API base: `https://ceil.indevs.in/v1/messages`; the gateway key
is the one already in the copied DB.

- systemd unit `gateway-staging` (port 9787), auto-restart, boot पर चालू
- nginx site `gateway-staging`, Let's Encrypt cert (2026-11-25 तक), http→https redirect
- SSE के लिए `proxy_buffering off`, 3600s read timeout
- hedger port 9091 (live container अपने netns में 9090 पर है — टकराव नहीं)
- live 8787 / `ciel.sryze.cc` को छुआ नहीं गया; उसका nginx site और docker अछूते

---

## Latency: क्या बदला (नाप कर)

| | live (24h, tuning से पहले) | staging (tuned) |
|---|---|---|
| average | 61s | **3.9s** |
| पहली attempt में सफल | 69% (131/191) | **78% (7/9)** |
| average attempts | 1.7 | **1.22** |
| 41KB body, streaming | — | first content 6.1s |

तीन बदलावों से आया:

1. **Endpoint order data से तय किया.** justwoker 79% serve करता है (127/160),
   agentrouter 11% (34/303). पहले agentrouter पहला था। अब justwoker पहला।
2. **Per-endpoint proxy chain.** scrape.do → agentrouter पर 130 में 1 सफल (0.8%)
   और हर नाकामी ~59s की; justwoker पर 45 में 24 (53%). इसलिए agentrouter से
   scrape.do हटाया, justwoker पर रखा।
3. **Dedicated-proxy cooldown** (नया). फ़ोन बंद हो तो हर request पहली attempt उसे
   खोजने में गँवाती थी — sandbox logs में लगातार 10/10. अब connect-level नाकामी
   60s याद रहती है, फिर अपने आप वापस। सिर्फ़ connect नाकामी गिनती है; upstream
   4xx/5xx या WAF अच्छे proxy को बाहर नहीं करता।

## Tuned defaults (DEFAULT_SETTINGS में, dashboard से बदले जा सकते हैं)

| setting | पहले | अब | क्यों |
|---|---|---|---|
| `connect_timeout` | 60 | 8 | ज़िंदा proxy 1-3s में जुड़ता है; 60s सिर्फ़ मरे हुए पर खर्च |
| `max_retries` | 10 | 6 | 6 attempts = 471s औसत; उससे आगे पीटने का फ़ायदा नहीं |
| `hedging_concurrency` | 3 | 5 | अब एक attempt सिर्फ़ winner + असली नाकाम खपाता है |
| `failover_5xx_threshold` | 3 | 2 | 2 संकेत काफ़ी; एक पूरा धीमा attempt बचता है |
| `retry_max_delay` | 8.0 | 4.0 | proxy-local नाकामी पर तो रुकना ही नहीं होता |
| `keepalive_idle` / `intvl` | 30 / 5 | 15 / 3 | tunnel idle-drop = 48 RemoteProtocolError/दिन |
| `dedicated_cooldown` | — | 60 | नया |

`get_setting`/`get_all_settings` अब DEFAULT_SETTINGS पर fallback करते हैं — पहले हर
call site का अपना literal fallback था जो चुपचाप इनसे अलग हो जाता था।

---

## Backend fixes (दूसरा दौर)

26. **Hedging ग़लत proxy को दोषी ठहराता था.** hedger जानता था कौन जीता, पर बताता
    नहीं था; forwarder हमेशा `candidates[0]` को credit/blame करता था। 10 proxies
    race करने पर एक proxy बाक़ी नौ का बोझ उठाता था, और pool की health धीरे-धीरे
    कल्पना बन जाती थी। अब `x-hedge-id` side-channel से असली winner मिलता है।
    Race हारने वाले अछूते रहते हैं — धीमा होना खराबी नहीं है।
27. **`race_proxies` उन्हीं को गिनता था जो winner से पहले जवाब दे चुके थे.**
    `as_completed` जल्दी रुक जाता है, तो अच्छे proxy के साथ race करने वाला मरा हुआ
    proxy हमेशा साफ़ record रखता था। अब पहले से पूरे हो चुके tasks भी harvest होते हैं
    (और दूसरा खुला tunnel बंद होता है, socket leak नहीं)।
28. **एक attempt पूरे `hedging_concurrency` proxies खपा देता था.** `attempted_pids`
    में सारे raced candidates जुड़ते थे, इसलिए max_retries=10 पर असल में 3 attempt
    मिलते थे (नापा गया)। अब सिर्फ़ winner + असली नाकाम गिने जाते हैं → पूरे 10.
    `_ensure_progress()` से loop कभी अटकता नहीं।
29. **openai relay path में `detail` semicolon के पीछे छिपा था** —
    `if type(p["id"]) == int: mark_bad(...); detail = ...` — तो dedicated proxy पर
    `detail` पिछली attempt का बचा रहता था (या पहली बार खाली)। अब बाहर निकाला।
30. **`_skip_backoff` openai path में लगा ही नहीं था** — वहाँ हर retry रुकता था।
31. **Unroutable proxies pool में घुस जाते थे.** live pool में `0.0.0.0:80`
    status "ok" पर बैठा था और असली requests पर race होकर हर बार एक attempt जलाता था।
    अब add/bulk-paste पर loopback/private/0.0.0.0/port-0 रुकते हैं
    (`skipped_unroutable` गिनती लौटती है), और sandbox pool से 2 हटाए।
32. **hedger port hardcoded 9090 था** — एक host पर दो instance नहीं चल सकते थे।
    अब `HEDGER_PORT` env से (staging 9091 पर)।

## Dashboard
33. Routing panel अब dedicated proxy का cooldown दिखाता है
    (`Custom 1 (cooling 58s)`), वरना मरा फ़ोन UI में अदृश्य था।

## Tests (सब हरे)
- `/tmp/verify_fixes.sh` — 31/31 (paging, routing, tuned defaults, public HTTPS, auth, live-अछूता)
- `/tmp/hedge_attr_test.py` — 12/12 असली hedger के साथ fake proxies race कराकर
- `/tmp/attempt_budget_test.py` — 7/7
- `/tmp/cooldown_test.py` — 12/12
- `/tmp/xss_render_test.js` — 11/11, `/tmp/ro_logic_test.js` — 14/14
- forwarder logic — 4/4 (`_strip_thinking`, `_skip_backoff`)
- end-to-end public URL से: non-stream 200, streaming (thinking block के साथ) पूरा SSE,
  41KB body → first content 6.1s

---

## अभी बाक़ी (आपके फ़ैसले के लिए)

**A. फ़ोन proxy बंद है** (`ECONNREFUSED`). चालू करें तो cooldown अपने आप हट जाएगा और
सबसे तेज़ रास्ता वापस मिलेगा — 24h में उसकी सारी 131 तेज़ requests इसी से गई थीं।

**B. scrape.do का quota**: 1000 में 454 बचे। agentrouter पर उसे हटाने से बर्बादी रुकी।

**C. gorouter** sandbox में disabled — credit बचाने के लिए। Live में चालू है।

**D. Live पर यही tuning लगाना है?** उसके लिए container restart चाहिए, जिसकी
permission नहीं है। DB-only हिस्सा (endpoint order, proxy chain, settings) बिना
restart लग सकता है — कहें तो लगाऊँ।

**E. Security, live repo:** GitHub PAT plaintext में `/root/AI-gateway-v7/.git/config`
के remote URL में पड़ा है।

---

## पुराने findings (पहला दौर)

### Findings NOT yet fixed (need decisions)

**A. agentrouter.org has a secret-scanner.** Isolated by 25+ controlled probes:
`ghp_*`, `sk-*`, `user:pass@ip:port`, PEM blocks and binary junk each return an
instant `400 content-blocked` (~1.4s), while 240KB bodies, 20 tool schemas,
thinking, cache_control and safety text all return 200. Not a WAF, not our bug —
their content filter. Suggested: add `content-blocked` to that endpoint's
`endpoint_failover_keywords` so the gateway switches target instead of returning 400.

**B. agentrouter has `proxy_fallback = 0` and a single dead proxy** (the phone at
`100.97.11.41:8080`, `ECONNREFUSED` — the Every Proxy app is off). So the primary
dies on attempt 1 with no backup. Fix is either turning the phone proxy back on,
or setting fallback = 1, or adding the scrape.do token to that endpoint too.

**C. Latency source, measured from logs.** Median success by proxy:
scrape.do 56s (n=22), mobile 150s (n=10), free pool 221s (n=8). 1-attempt requests
average 58s; 6-attempt requests 580s. So most latency is burned in failed retries,
not model generation.

**D. Hedging still blames the wrong proxy.** `_consume_assemble` and the streaming
path both attribute the result to `candidates[0]` even when `hedging_concurrency`
raced 10 proxies through the local hedger, so a fast proxy can be marked bad for a
slow one's failure. Fixing this needs the hedger to report which URL actually won —
a change to `hedger.py`'s protocol, so left alone pending your go-ahead.

**E. `attempted_pids` is updated with every hedged candidate before any of them is
tried,** so one attempt burns up to 10 pool proxies. With `max_retries=10` and
`hedging_concurrency=10` a target gets only 4-5 real attempts. Worth discussing what
you want the attempt budget to mean.

**F. Security, live repo:** the GitHub PAT is stored in plaintext in
`/root/AI-gateway-v7/.git/config` as part of the remote URL.

---

## Round 3 — latency measurement was lying (2026-08-28)

The operator said the dashboard's latency looked wrong. It was. Measured against
the raw logs:

| | dashboard said | reality |
|---|---|---|
| total requests | 266 | **52** (5.1x inflated) |
| success rate | 12% | **63%** |
| avg latency | 146s | **171s** |

Two separate defects, both in `db.stats()`:

1. **It aggregated log ROWS, not requests.** Every retry writes its own row, so a
   request that retried 7 times counted as 7 "requests". Success rate collapsed
   because the 6 failed attempts of a successful request were counted as 6 failures.
2. **Failed requests contributed no latency at all.** Only 2xx rows carry `ms`, so a
   request that burned 10 minutes across three endpoints and then failed added
   nothing to the average. This is exactly the "beech ka timing" the operator noticed
   missing — the worst waits were invisible.

Fix: `logs` gained `req_id` (same value for every attempt of one request) and `final`
(1 on the row that decided the outcome). `stats()` now reads only `final=1` rows, so
one row = one client request, and latency spans successes AND failures.

Grouping is now recorded, not inferred. The obvious alternative — "a new request
starts when `attempts` resets to 1" — breaks under concurrency, because two
overlapping requests interleave their rows and cannot be separated afterwards.

Also fixed while here:
- `gen_openai()` never logged its terminal failure, so a fully-failed OpenAI-mode
  request left no row at all. Now logged.
- The 400 (content-filter) and 503 (no proxies) exits called `db.add_log` directly,
  bypassing the `final` flag. Marked.
- `.gitignore` had `data/` (trailing slash), which does not match the sandbox's
  `data` SYMLINK — the DB pointer showed up as untracked and a careless `git add -A`
  would have committed it. Now `data`.
- The Scrape.do input's placeholder was the first 12 characters of the operator's
  real token. Replaced with a generic hint.

`endpoint_stats()` deliberately stays attempt-level: "how does this endpoint behave
when the router tries it" is an attempt-level question. Only whole-request latency
moved to `stats()`.

## Dashboard
- Headline tile is now **median** latency (avg is skewed by 600s outliers), with
  avg / p95 / worst / first-try-% in its tooltip and a `p95 · % 1st try` subline.
