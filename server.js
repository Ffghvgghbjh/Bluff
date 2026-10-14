// ბლეFი — ონლაინ სერვერი. თამაშის წესებს სერვერი ამოწმებს, ბრაუზერები მხოლოდ აჩვენებენ.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;
const SL = process.env.FAST ? 0.005 : 1;

const server = http.createServer((q, r) => {
  if (q.url === '/healthz') return r.end('ok');
  r.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  r.end(fs.readFileSync(path.join(__dirname, 'public', 'index.html')));
});
const wss = new WebSocketServer({ server });
const rooms = new Map();

const esc = s => String(s).replace(/[&<>"']/g, c => '&#' + c.charCodeAt(0) + ';');
const shuf = a => { for (let i = a.length - 1; i > 0; i--) { const j = Math.random() * (i + 1) | 0; [a[i], a[j]] = [a[j], a[i]]; } return a; };
const sleep = ms => new Promise(r => setTimeout(r, ms * SL));
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const randAns = (o, c) => shuf(o.map((x, i) => x.off ? -1 : i).filter(i => i >= 0)).slice(0, c);

const R = { D: { n: 'დიქტატორი', e: '👑', is: 'დიქტატორია' }, A: { n: 'ასასინი', e: '🗡️', is: 'ასასინია' }, C: { n: 'ქურდი', e: '💰', is: 'ქურდია' }, M: { n: 'მაგიკოსი', e: '🎩', is: 'მაგიკოსია' }, G: { n: 'მაფია', e: '🕶️', is: 'მაფიაა' } };
const who = (t, q) => t === q ? 'შენ' : t.n;
const desc = (a, t, q) => ({ tax: 'აიღებს 3 მონეტას', kill: `მოკლავს კარტს. სამიზნე: ${t && who(t, q)}`, steal: `მოიპარავს 2 მონეტას. სამიზნე: ${t && who(t, q)}`, exch: 'გაცვლის კარტებს' })[a];
const actLog = (p, a, t) => ({
  income: `${p.n} იღებს 1 მონეტას.`,
  aid: `${p.n} იღებს დახმარებას: +2 მონეტა.`,
  coup: `${p.n} აკეთებს გადატრიალებას. სამიზნე: ${t && t.n}.`,
  tax: `${p.n} ამბობს, რომ დიქტატორია, და იღებს 3 მონეტას.`,
  kill: `${p.n} ამბობს, რომ ასასინია. სამიზნე: ${t && t.n}.`,
  steal: `${p.n} ამბობს, რომ ქურდია. სამიზნე: ${t && t.n}.`,
  exch: `${p.n} ამბობს, რომ მაგიკოსია და ცვლის კარტებს.`
})[a];
const blockQ = (actor, act) => ({ aid: `${actor.n} იღებს დახმარებას (+2 მონეტა). გინდა დაბლოკო?`, kill: `${actor.n} გიპირებს შენი კარტის მოკვლას. გინდა დაბლოკო?`, steal: `${actor.n} გიპირებს შენი 2 მონეტის მოპარვას. გინდა დაბლოკო?` })[act];
const ACT = { income: 'იღებს მონეტას', aid: 'დახმარება (+2)', coup: 'გადატრიალება', tax: 'დიქტატორი (+3)', kill: 'მკვლელობა', steal: 'ქურდობა', exch: 'გაცვლა' };
const NAMES = ['ნიკა', 'ანა', 'გიო', 'მარიამი', 'დათო'];

// ---------- ოთახები ----------
function lob(rm) {
  const players = rm.pl.map(x => ({ n: x.nick, h: x.host, on: !!x.ws }));
  rm.pl.forEach(c => send(c.ws, { t: 'lobby', code: rm.code, players, host: c.host }));
}

function join(ws, m) {
  const code = String(m.room || '').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 8);
  const tok = String(m.tok || '').slice(0, 64);
  if (!code || !tok) return;
  let rm = rooms.get(code);
  if (!rm) {
    if (!m.create) return send(ws, { t: 'err', m: 'ოთახი ვერ მოიძებნა. შეამოწმე კოდი.' });
    rm = { code, pl: [], g: null };
    rooms.set(code, rm);
  }
  let c = rm.pl.find(x => x.tok === tok);
  if (!c) {
    if (rm.g) return send(ws, { t: 'err', m: 'თამაში უკვე დაიწყო.' });
    if (rm.pl.length >= 6) return send(ws, { t: 'err', m: 'ოთახი სავსეა (მაქსიმუმ 6).' });
    c = { tok, host: !rm.pl.length };
    rm.pl.push(c);
  }
  c.nick = esc(String(m.nick || '').trim().slice(0, 12) || 'მოთამაშე');
  if (c.ws && c.ws !== ws) c.ws.close();
  c.ws = ws; ws.rm = rm; rm.last = Date.now();
  if (rm.g) { rm.g.push(1); rm.g.resend(tok); } else lob(rm);
}

wss.on('connection', ws => {
  ws.alive = true;
  ws.on('pong', () => ws.alive = true);
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'join') return join(ws, m);
    const rm = ws.rm; if (!rm) return;
    rm.last = Date.now();
    const c = rm.pl.find(x => x.ws === ws);
    if (!c) return;
    if (m.t === 'start') {
      if (!c.host || rm.g) return;
      const nb = Math.max(0, Math.min(6 - rm.pl.length, +m.nb | 0)), tot = rm.pl.length + nb;
      if (tot < 2 || tot > 6) return send(ws, { t: 'err2', m: 'მოთამაშეები უნდა იყოს 2–6' });
      rm.g = runGame(rm, nb);
    }
    if (m.t === 'ans' && rm.g) rm.g.answer(c.tok, m);
  });
  ws.on('close', () => { const rm = ws.rm; if (!rm) return; const c = rm.pl.find(x => x.ws === ws); if (c) { c.ws = null; if (!rm.g) lob(rm); else rm.g.push(); } });
});
setInterval(() => {
  wss.clients.forEach(w => { if (!w.alive) return w.terminate(); w.alive = false; w.ping(); });
  for (const [k, rm] of rooms) if (Date.now() - (rm.last || 0) > 6 * 3600e3 && !rm.pl.some(x => x.ws)) rooms.delete(k);
}, 25000);

// ---------- თამაში ----------
function runGame(rm, nb) {
  const g = { pend: {}, qid: 0 };
  let evt = null, evn = 0;
  let deck = shuf(Object.keys(R).flatMap(r => [r, r, r])), turn = 0, log = [], over = '', waiting = '', bt;
  const P = shuf([...rm.pl.map(c => ({ n: c.nick, h: 1, tok: c.tok })), ...Array.from({ length: nb }, (_, i) => ({ n: NAMES[i] }))]);
  P.forEach(p => { p.coins = 2; p.cards = [{ r: deck.pop() }, { r: deck.pop() }]; });
  const ev = o => { evt = { id: ++evn, ...o }; g.push(1); };
  const live = p => p.cards.filter(c => !c.d);
  const others = p => { const r = []; for (let i = 1; i < P.length; i++) { const q = P[(P.indexOf(p) + i) % P.length]; if (live(q).length) r.push(q); } return r; };
  const conn = tok => (rm.pl.find(x => x.tok === tok) || {}).ws;

  const view = i => ({
    t: 'st', me: i, tn: turn, w: waiting, o: over, l: log.slice(0, 8), ev: evt, a: (Object.values(g.pend).find(e => e.p === P[i]) || { m: { id: 0 } }).m.id,
    p: P.map((p, j) => ({ n: p.n, c: p.coins, k: p.cards.map(c => c.d || j === i ? [c.r, c.d ? 1 : 0] : 0) }))
  });
  g.push = now => {
    clearTimeout(bt);
    bt = setTimeout(() => P.forEach((p, i) => p.tok && send(conn(p.tok), view(i))), now ? 0 : 80);
  };
  const say = t => { log.unshift(t); g.push(); };

  function ask(p, q, opts, count = 1, tl = 0, def = null) {
    return new Promise(res => {
      const id = ++g.qid; waiting = p.n;
      const m = { t: 'ask', id, q, o: opts.map(o => o.l), off: opts.map(o => o.off ? 1 : 0), c: count };
      const e = { p, m, opts, count, tl, at: Date.now() };
      e.res = a => { clearTimeout(e.timer); delete g.pend[id]; waiting = ''; res(a.map(i => opts[i].v)); g.push(); };
      e.timer = tl ? setTimeout(() => e.res(def || randAns(opts, count)), tl * 1000)
        : setTimeout(() => { p.h = 0; say(`${p.n} დიდხანს არ პასუხობს, ამიტომ მის ნაცვლად ბოტი თამაშობს.`); e.res(randAns(opts, count)); }, 60000);
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

  async function lose(p, why) {
    const l = live(p); if (!l.length) return;
    let c;
    if (p.h && l.length > 1) c = (await ask(p, `${why} აირჩიე კარტი, რომელსაც დაკარგავ:`, l.map(x => ({ l: R[x.r].e + ' ' + R[x.r].n, v: x })), 1, 30))[0];
    else c = l[Math.random() * l.length | 0];
    c.d = 1; say(`${p.n} კარგავს: ${R[c.r].e} ${R[c.r].n}`);
    if (!live(p).length) say(`☠️ ${p.n} თამაშიდან გავიდა.`);
  }
  const swap = c => { deck.push(c.r); shuf(deck); c.r = deck.pop(); };

  async function challengeRound(actor, role, what) {
    for (const q of others(actor)) {
      if (!live(q).length) continue;
      let ch;
      if (q.h) ch = (await ask(q, `${actor.n} ამბობს, რომ ${R[role].is} (${what(q)}). გჯერა?`, [{ l: '🤥 არ მჯერა, ვამოწმებ!', v: 1 }, { l: '👍 მჯერა, გაუშვი', v: 0 }], 1, 15, [1]))[0];
      else { await sleep(500); ch = Math.random() < .12; }
      if (ch) {
        say(`${q.n} ამბობს: „არ მჯერა!“`);
        const c = live(actor).find(x => x.r === role);
        if (c) { say(`${actor.n} აჩვენებს კარტს ${R[role].e} ${R[role].n} — მართლა ჰქონდა!`); ev({ k: 'reveal', ok: 1, r: role, n: actor.n }); await sleep(2600); swap(c); await lose(q, 'შენ დააეჭვე მოწინააღმდეგე, მაგრამ ის მართალი იყო.'); return true; }
        say(`${actor.n} იცრუა — ეს კარტი არ ჰქონდა!`); ev({ k: 'reveal', ok: 0, r: role, n: actor.n }); await sleep(2600); await lose(actor, 'ტყუილში გამოგიჭირეს.'); return false;
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
      if (q.h) br = (await ask(q, blockQ(actor, act), [...roles.map(r => ({ l: `🛡️ დავბლოკავ: ${R[r].e} ${R[r].n}`, v: r })), { l: 'არ ვბლოკავ', v: 0 }], 1, 15, [roles.length]))[0] || null;
      else { const own = roles.find(r => live(q).some(c => c.r === r)); if (own && Math.random() < .7) br = own; else if (Math.random() < .08) br = roles[0]; }
      if (!br) continue;
      say(`${q.n} ბლოკავს და ამბობს, რომ ${R[br].is}.`); ev({ k: 'claim', r: br });
      let ch;
      if (actor.h) ch = (await ask(actor, `${q.n} ამბობს, რომ ${R[br].is}, და ბლოკავს შენს სვლას. გჯერა?`, [{ l: '🤥 არ მჯერა, ვამოწმებ!', v: 1 }, { l: '👍 მჯერა', v: 0 }], 1, 15, [1]))[0];
      else ch = Math.random() < .2;
      if (!ch) return true;
      say(`${actor.n} ამბობს: „არ მჯერა!“`);
      const c = live(q).find(x => x.r === br);
      if (c) { say(`${q.n} აჩვენებს კარტს ${R[br].e} ${R[br].n} — მართლა ჰქონდა!`); ev({ k: 'reveal', ok: 1, r: br, n: q.n }); await sleep(2600); swap(c); await lose(actor, 'შენ დააეჭვე მოწინააღმდეგე, მაგრამ ის მართალი იყო.'); return true; }
      say(`${q.n} იცრუა — ეს კარტი არ ჰქონდა!`); ev({ k: 'reveal', ok: 0, r: br, n: q.n }); await sleep(2600); await lose(q, 'ტყუილში გამოგიჭირეს.');
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
    const opts = [{ l: '🪙 აიღე 1 მონეტა', v: 'income', off: f }, { l: '💵 დახმარება: +2 მონეტა', v: 'aid', off: f }, { l: '💥 გადატრიალება (7 მონეტა)', v: 'coup', off: p.coins < 7 }, { l: '👑 დიქტატორი: +3 მონეტა', v: 'tax', off: f }, { l: '🗡️ ასასინი: მკვლელობა (3 მონეტა)', v: 'kill', off: f || p.coins < 3 }, { l: '💰 ქურდი: 2 მონეტის მოპარვა', v: 'steal', off: f }, { l: '🎩 მაგიკოსი: კარტების გაცვლა', v: 'exch', off: f }];
    const a = (await ask(p, 'შენი სვლაა. აირჩიე მოქმედება:', opts))[0]; let t;
    if (['coup', 'kill', 'steal'].includes(a)) t = (await ask(p, 'აირჩიე სამიზნე:', others(p).map(x => ({ l: `${x.n} — ${x.coins} მონეტა`, v: x, off: a === 'steal' && !x.coins }))))[0];
    return { a, t };
  }

  async function play(p) {
    const { a, t } = p.h ? await humanAct(p) : botAct(p);
    say(actLog(p, a, t));
    if (a === 'coup') { p.coins -= 7; await lose(t, 'შენზე გადატრიალება გააკეთეს.'); return; }
    if (a === 'kill') p.coins -= 3;
    const role = { tax: 'D', kill: 'A', steal: 'C', exch: 'M' }[a];
    ev({ k: 'claim', r: role || null });
    if (role && !(await challengeRound(p, role, q => desc(a, t, q)))) { if (a === 'kill') p.coins += 3; return; }
    if (['aid', 'kill', 'steal'].includes(a) && await blockRound(p, a, t)) return;
    if (a === 'income') p.coins++; if (a === 'aid') p.coins += 2; if (a === 'tax') p.coins += 3;
    if (a === 'kill' && live(t).length) await lose(t, 'ასასინმა შენს კარტს დაარტყა.');
    if (a === 'steal') { const n = Math.min(2, t.coins); t.coins -= n; p.coins += n; }
    if (a === 'exch') {
      const k = live(p).length, pool = [...live(p).map(c => c.r), deck.pop(), deck.pop()]; let keep;
      if (p.h) keep = await ask(p, `მაგიკოსი: აირჩიე ${k} კარტი, რომელიც შეინარჩუნებ (დანარჩენი დასტაში დაბრუნდება):`, pool.map((r, i) => ({ l: R[r].e + ' ' + R[r].n, v: i })), k, 40, [...Array(k).keys()]);
      else keep = shuf([...pool.keys()]).slice(0, k);
      live(p).forEach((c, i) => c.r = pool[keep[i]]);
      pool.forEach((r, i) => { if (!keep.includes(i)) deck.push(r); }); shuf(deck);
    }
  }

  (async () => {
    say('თამაში დაიწყო! სვლა საათის ისრის მიმართულებით გადადის.');
    try {
      while (P.filter(x => live(x).length).length > 1) {
        const p = P[turn];
        if (live(p).length) { g.push(); await play(p); g.push(); await sleep(p.h ? 300 : 900); }
        turn = (turn + 1) % P.length;
      }
      over = P.find(x => live(x).length).n;
    } catch (e) { console.error(e); over = 'შეცდომა — თავიდან დაიწყეთ'; }
    g.push(1);
  })();
  return g;
}

server.listen(PORT, () => console.log('ბლეFი გაშვებულია: http://localhost:' + PORT));
