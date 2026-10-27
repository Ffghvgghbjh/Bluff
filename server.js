// BLUFF / ბლეFი — online server. The server owns the rules; browsers only render.
// All game text is sent as language-neutral data (keys + player indexes); the client translates it.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;
const SL = process.env.FAST ? 0.005 : 1; // FAST=1 shortens pauses (tests only)

const server = http.createServer((q, r) => {
  if (q.url === '/healthz') return r.end('ok');
  r.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
  r.end(fs.readFileSync(path.join(__dirname, 'public', 'index.html')));
});
const wss = new WebSocketServer({ server, maxPayload: 4096 });
const rooms = new Map();

const esc = s => String(s).replace(/[&<>"']/g, c => '&#' + c.charCodeAt(0) + ';');
const shuf = a => { for (let i = a.length - 1; i > 0; i--) { const j = Math.random() * (i + 1) | 0; [a[i], a[j]] = [a[j], a[i]]; } return a; };
const sleep = ms => new Promise(r => setTimeout(r, ms * SL));
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const randAns = (o, c) => shuf(o.map((x, i) => x.off ? -1 : i).filter(i => i >= 0)).slice(0, c);
const ROLES = ['D', 'A', 'C', 'M', 'G'];

// ---------- rooms ----------
function lob(rm) {
  rm.nb = Math.max(0, Math.min(rm.nb, 6 - rm.pl.length));
  const players = rm.pl.map(x => ({ n: x.nick, h: x.host, on: !!x.ws }));
  rm.pl.forEach((c, i) => send(c.ws, { t: 'lobby', code: rm.code, players, nb: rm.nb, host: c.host, me: i }));
}

function join(ws, m) {
  const code = String(m.room || '').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 8);
  const tok = String(m.tok || '').slice(0, 64);
  if (!code || !tok) return;
  let rm = rooms.get(code);
  if (!rm) {
    if (!m.create) return send(ws, { t: 'err', k: 'nf' });
    rm = { code, pl: [], g: null, nb: 1 };
    rooms.set(code, rm);
  }
  let c = rm.pl.find(x => x.tok === tok);
  if (c && c.left) return send(ws, { t: 'err', k: 'started' });
  if (!c) {
    if (rm.g) return send(ws, { t: 'err', k: 'started' });
    if (rm.pl.length >= 6) return send(ws, { t: 'err', k: 'full' });
    c = { tok, host: !rm.pl.length };
    rm.pl.push(c);
  }
  c.nick = esc(String(m.nick || '').trim().slice(0, 12) || '•');
  if (c.ws && c.ws !== ws) c.ws.close();
  c.ws = ws; ws.rm = rm; rm.last = Date.now();
  if (rm.g) { rm.g.push(1); rm.g.resend(tok); } else lob(rm);
}

function leave(ws, rm, c) {
  ws.rm = null; c.ws = null;
  if (!rm.g) {
    rm.pl = rm.pl.filter(x => x !== c);
    if (!rm.pl.length) return rooms.delete(rm.code);
    if (c.host) rm.pl[0].host = true;
    lob(rm);
  } else { c.left = true; rm.g.leave(c.tok); }
}

wss.on('connection', ws => {
  ws.alive = true;
  ws.on('pong', () => ws.alive = true);
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m !== 'object') return;
    if (m.t === 'join') return join(ws, m);
    const rm = ws.rm; if (!rm) return;
    rm.last = Date.now();
    const c = rm.pl.find(x => x.ws === ws);
    if (!c) return;
    if (m.t === 'leave') return leave(ws, rm, c);
    if (m.t === 'bots' && c.host && !rm.g) { rm.nb = Math.max(0, Math.min(6 - rm.pl.length, +m.n | 0)); return lob(rm); }
    if (m.t === 'start' && c.host && !rm.g) {
      const tot = rm.pl.length + rm.nb;
      if (tot < 2 || tot > 6) return send(ws, { t: 'err2', k: 'count' });
      rm.g = runGame(rm, rm.nb);
    }
    if (m.t === 'again' && c.host && rm.g && rm.g.isOver()) { rm.pl = rm.pl.filter(x => !x.left); rm.g = null; lob(rm); }
    if (m.t === 'emote' && rm.g) rm.g.emote(c.tok, m.e | 0);
    if (m.t === 'ans' && rm.g) rm.g.answer(c.tok, m);
  });
  ws.on('close', () => {
    const rm = ws.rm; if (!rm) return;
    const c = rm.pl.find(x => x.ws === ws);
    if (c) { c.ws = null; if (!rm.g) lob(rm); else rm.g.push(); }
  });
});
setInterval(() => {
  wss.clients.forEach(w => { if (!w.alive) return w.terminate(); w.alive = false; w.ping(); });
  for (const [k, rm] of rooms) if (Date.now() - (rm.last || 0) > 6 * 3600e3 && !rm.pl.some(x => x.ws)) rooms.delete(k);
}, 25000);

// ---------- game ----------
function runGame(rm, nb) {
  const g = { pend: {}, qid: 0 };
  let evt = null, evn = 0, turn = 0, log = [], over = null, waiting = -1, bt;
  const deck = shuf(ROLES.flatMap(r => [r, r, r]));
  const P = shuf([...rm.pl.map(c => ({ n: c.nick, h: 1, tok: c.tok })), ...Array.from({ length: nb }, (_, i) => ({ b: i }))]);
  P.forEach(p => { p.coins = 2; p.cards = [{ r: deck.pop() }, { r: deck.pop() }]; });
  const ix = p => P.indexOf(p);
  const ev = o => { evt = { id: ++evn, ...o }; g.push(1); };
  const live = p => p.cards.filter(c => !c.d);
  const others = p => { const r = []; for (let i = 1; i < P.length; i++) { const q = P[(P.indexOf(p) + i) % P.length]; if (live(q).length) r.push(q); } return r; };
  const conn = tok => (rm.pl.find(x => x.tok === tok) || {}).ws;

  const view = i => ({
    t: 'st', me: i, tn: turn, host: !!(rm.pl.find(x => x.tok === P[i].tok) || {}).host, w: waiting, o: over === null ? -1 : over, l: log.slice(0, 10), ev: evt,
    a: (Object.values(g.pend).find(e => e.p === P[i]) || { m: { id: 0 } }).m.id,
    p: P.map((p, j) => ({
      n: p.b == null ? p.n : null, b: p.b == null ? null : p.b, c: p.coins, on: p.tok ? (!p.gone && !!conn(p.tok)) : true,
      k: p.cards.map(c => c.d || j === i ? [c.r, c.d ? 1 : 0] : 0)
    }))
  });
  g.push = now => {
    clearTimeout(bt);
    bt = setTimeout(() => P.forEach((p, i) => p.tok && send(conn(p.tok), view(i))), now ? 0 : 80);
  };
  g.isOver = () => over !== null;
  const say = e => { log.unshift(e); g.push(); };

  function ask(p, q, opts, count = 1, tl = 0, def = null) {
    return new Promise(res => {
      const id = ++g.qid; waiting = ix(p);
      const m = { t: 'ask', id, q, o: opts.map(o => o.l), off: opts.map(o => o.off ? 1 : 0), c: count };
      const e = { p, m, opts, count, tl, at: Date.now() };
      e.res = a => { clearTimeout(e.timer); delete g.pend[id]; waiting = -1; res(a.map(i => opts[i].v)); g.push(); };
      e.timer = tl ? setTimeout(() => e.res(def || randAns(opts, count)), tl * 1000)
        : setTimeout(() => { p.h = 0; say({ k: 'afk', n: ix(p) }); e.res(randAns(opts, count)); }, 60000);
      g.pend[id] = e; sendAsk(e); g.push();
    });
  }
  const sendAsk = e => { const left = e.tl ? Math.max(1, Math.ceil(e.tl - (Date.now() - e.at) / 1000)) : 0; send(conn(e.p.tok), { ...e.m, tl: left }); };
  g.resend = tok => Object.values(g.pend).forEach(e => e.p.tok === tok && sendAsk(e));
  g.answer = (tok, m) => {
    const e = g.pend[m.id]; if (!e || e.p.tok !== tok || !Array.isArray(m.a)) return;
    const a = [...new Set(m.a)];
    if (a.length !== e.count || !a.every(i => e.opts[i] && !e.opts[i].off)) return;
    e.res(a);
  };
  g.leave = tok => {
    const p = P.find(x => x.tok === tok); if (!p) return;
    p.h = 0; p.gone = 1;
    Object.values(g.pend).filter(e => e.p === p).forEach(e => e.res(randAns(e.opts, e.count)));
    g.push();
  };
  g.emote = (tok, e) => {
    const i = P.findIndex(x => x.tok === tok), p = P[i];
    if (!p || e < 0 || e > 3 || Date.now() - (p.emo || 0) < 1200) return;
    p.emo = Date.now();
    P.forEach(x => x.tok && send(conn(x.tok), { t: 'emo', p: i, e }));
  };

  async function lose(p, why) {
    const l = live(p); if (!l.length) return;
    let c;
    if (p.h && l.length > 1) c = (await ask(p, { k: 'q_lose', w: why }, l.map(x => ({ l: { k: 'role', r: x.r }, v: x })), 1, 30))[0];
    else c = l[Math.random() * l.length | 0];
    c.d = 1; say({ k: 'lose', n: ix(p), r: c.r });
    if (!live(p).length) say({ k: 'out', n: ix(p) });
  }
  const swap = c => { deck.push(c.r); shuf(deck); c.r = deck.pop(); };

  async function challengeRound(actor, role, a, t) {
    for (const q of others(actor)) {
      if (!live(q).length) continue;
      let ch;
      if (q.h) ch = (await ask(q, { k: 'q_call', n: ix(actor), r: role, a, t: t ? ix(t) : -1 }, [{ l: { k: 'c_yes' }, v: 1 }, { l: { k: 'c_no' }, v: 0 }], 1, 15, [1]))[0];
      else { await sleep(500); ch = Math.random() < .12; }
      if (ch) {
        say({ k: 'call', n: ix(q) });
        const c = live(actor).find(x => x.r === role);
        if (c) { say({ k: 'shown', n: ix(actor), r: role }); ev({ k: 'reveal', ok: 1, r: role, n: ix(actor) }); await sleep(2600); swap(c); await lose(q, 'badcall'); return true; }
        say({ k: 'bluffed', n: ix(actor) }); ev({ k: 'reveal', ok: 0, r: role, n: ix(actor) }); await sleep(2600); await lose(actor, 'caught'); return false;
      }
    }
    return true;
  }

  async function blockRound(actor, act, t) {
    const roles = act === 'aid' ? ['D'] : act === 'kill' ? ['G'] : ['C', 'M'];
    const cands = act === 'aid' ? others(actor) : [t];
    for (const q of cands) {
      if (!live(q).length) continue;
      let br = null;
      if (q.h) br = (await ask(q, { k: 'q_block', n: ix(actor), a: act }, [...roles.map(r => ({ l: { k: 'b_do', r }, v: r })), { l: { k: 'b_no' }, v: 0 }], 1, 15, [roles.length]))[0] || null;
      else { const own = roles.find(r => live(q).some(c => c.r === r)); if (own && Math.random() < .7) br = own; else if (Math.random() < .08) br = roles[0]; }
      if (!br) continue;
      say({ k: 'block', n: ix(q), r: br }); ev({ k: 'claim', r: br });
      let ch;
      if (actor.h) ch = (await ask(actor, { k: 'q_counter', n: ix(q), r: br }, [{ l: { k: 'c_yes' }, v: 1 }, { l: { k: 'c_no' }, v: 0 }], 1, 15, [1]))[0];
      else ch = Math.random() < .2;
      if (!ch) return true;
      say({ k: 'call', n: ix(actor) });
      const c = live(q).find(x => x.r === br);
      if (c) { say({ k: 'shown', n: ix(q), r: br }); ev({ k: 'reveal', ok: 1, r: br, n: ix(q) }); await sleep(2600); swap(c); await lose(actor, 'badcall'); return true; }
      say({ k: 'bluffed', n: ix(q) }); ev({ k: 'reveal', ok: 0, r: br, n: ix(q) }); await sleep(2600); await lose(q, 'caught');
    }
    return false;
  }

  function botAct(p) {
    const en = others(p), tg = () => en[Math.random() * en.length | 0];
    if (p.coins >= 10 || (p.coins >= 7 && Math.random() < .8)) return { a: 'coup', t: tg() };
    const o = ['income', 'aid', 'tax', 'tax', 'steal', 'exch']; if (p.coins >= 3) o.push('kill', 'kill');
    let a = o[Math.random() * o.length | 0], t;
    if (a === 'steal') { const r = en.filter(x => x.coins > 0); if (r.length) t = r[Math.random() * r.length | 0]; else a = 'income'; }
    if (a === 'kill') t = tg();
    return { a, t };
  }

  async function humanAct(p) {
    const f = p.coins >= 10;
    const opts = [
      { l: { k: 'a_income' }, v: 'income', off: f }, { l: { k: 'a_aid' }, v: 'aid', off: f }, { l: { k: 'a_coup' }, v: 'coup', off: p.coins < 7 },
      { l: { k: 'a_tax' }, v: 'tax', off: f }, { l: { k: 'a_kill' }, v: 'kill', off: f || p.coins < 3 }, { l: { k: 'a_steal' }, v: 'steal', off: f }, { l: { k: 'a_exch' }, v: 'exch', off: f }];
    const a = (await ask(p, { k: 'q_act' }, opts))[0]; let t;
    if (['coup', 'kill', 'steal'].includes(a)) t = (await ask(p, { k: 'q_target' }, others(p).map(x => ({ l: { k: 'target', n: ix(x), c: x.coins }, v: x, off: a === 'steal' && !x.coins }))))[0];
    return { a, t };
  }

  async function play(p) {
    const { a, t } = p.h ? await humanAct(p) : botAct(p);
    say({ k: a, n: ix(p), t: t ? ix(t) : -1 });
    if (a === 'coup') { p.coins -= 7; await lose(t, 'coup'); return; }
    if (a === 'kill') p.coins -= 3;
    const role = { tax: 'D', kill: 'A', steal: 'C', exch: 'M' }[a];
    ev({ k: 'claim', r: role || null });
    if (role && !(await challengeRound(p, role, a, t))) { if (a === 'kill') p.coins += 3; return; }
    if (['aid', 'kill', 'steal'].includes(a) && await blockRound(p, a, t)) return;
    if (a === 'income') p.coins++; if (a === 'aid') p.coins += 2; if (a === 'tax') p.coins += 3;
    if (a === 'kill' && live(t).length) await lose(t, 'kill');
    if (a === 'steal') { const n = Math.min(2, t.coins); t.coins -= n; p.coins += n; }
    if (a === 'exch') {
      const k = live(p).length, pool = [...live(p).map(c => c.r), deck.pop(), deck.pop()]; let keep;
      if (p.h) keep = await ask(p, { k: 'q_exch', c: k }, pool.map((r, i) => ({ l: { k: 'role', r }, v: i })), k, 40, [...Array(k).keys()]);
      else keep = shuf([...pool.keys()]).slice(0, k);
      live(p).forEach((c, i) => c.r = pool[keep[i]]);
      pool.forEach((r, i) => { if (!keep.includes(i)) deck.push(r); }); shuf(deck);
    }
  }

  (async () => {
    say({ k: 'start' });
    try {
      while (P.filter(x => live(x).length).length > 1) {
        const p = P[turn];
        if (live(p).length) { g.push(); await play(p); g.push(); await sleep(p.h ? 300 : 900); }
        turn = (turn + 1) % P.length;
      }
      over = ix(P.find(x => live(x).length));
    } catch (e) { console.error(e); over = -2; }
    g.push(1);
  })();
  return g;
}

server.listen(PORT, () => console.log('BLUFF server running: http://localhost:' + PORT));
