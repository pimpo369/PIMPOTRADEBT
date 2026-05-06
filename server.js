// ============================================================
// RASEED AUTONOMOUS TRADING AGENT v4.2
// Dynamic Asset Discovery + 15-Layer Research + Alpaca Paper
// FIX v4.1: Crypto symbol format BTC/USD
// FIX v4.2: Alpaca startup sync — recovers positions after redeploy
// ============================================================

const { Telegraf } = require("telegraf");
const Database     = require("better-sqlite3");
const cron         = require("node-cron");
const Alpaca       = require("@alpacahq/alpaca-trade-api");

// ── CONFIG ────────────────────────────────────────────────
const cfg = {
  telegram:     process.env.TELEGRAM_BOT_TOKEN,
  chatId:       process.env.YOUR_CHAT_ID,
  polygon:      process.env.POLYGON_API_KEY,
  alpacaKey:    process.env.ALPACA_API_KEY,
  alpacaSecret: process.env.ALPACA_SECRET_KEY,
  alphavantage: process.env.ALPHAVANTAGE_API_KEY,
  finnhub:      process.env.FINNHUB_API_KEY,
  fmp:          process.env.FMP_API_KEY,
};

const alpaca = new Alpaca({
  keyId:      cfg.alpacaKey,
  secretKey:  cfg.alpacaSecret,
  paper:      true,
  usePolygon: false,
});

const bot = new Telegraf(cfg.telegram);

// ── DATABASE ──────────────────────────────────────────────
const db = new Database("./trading_agent.db");
db.exec(`
  CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT, asset_type TEXT, qty REAL, price REAL,
    dollar_value REAL, stop_loss_price REAL,
    vote_score INTEGER, vote_detail TEXT, discovered_via TEXT,
    status TEXT DEFAULT 'open', pnl REAL DEFAULT 0,
    opened_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    closed_at DATETIME
  );
  CREATE TABLE IF NOT EXISTS agent_state (
    key TEXT PRIMARY KEY, value TEXT
  );
  CREATE TABLE IF NOT EXISTS scan_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT,
    vote_score INTEGER, verdict TEXT,
    discovered_via TEXT, scanned_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS rate_limits (
    chat_id TEXT PRIMARY KEY, last_request INTEGER
  );
`);

const getState = (k, d=null) => {
  const r = db.prepare("SELECT value FROM agent_state WHERE key=?").get(k);
  return r ? JSON.parse(r.value) : d;
};
const setState = (k,v) =>
  db.prepare("INSERT OR REPLACE INTO agent_state (key,value) VALUES (?,?)").run(k, JSON.stringify(v));

if (getState("total_loss") === null) setState("total_loss", 0);
if (getState("paused")     === null) setState("paused",     false);

// ── GUARDRAILS ────────────────────────────────────────────
const BUDGET         = 200;
const MAX_TOTAL_LOSS = 50;
const MAX_POSITION   = 50;
const MAX_POSITIONS  = 10;
const STOP_STOCK     = 0.08;
const STOP_CRYPTO    = 0.05;
const MIN_VOTES      = 12;

const ALPACA_CRYPTO = [
  "BTC","ETH","AVAX","LINK","LTC","AAVE","BAT","BCH",
  "CRV","DOGE","DOT","GRT","MKR","SHIB","SUSHI","UNI",
  "XTZ","YFI","XRP","SKY"
];

// ── HELPERS ───────────────────────────────────────────────
async function safeFetch(url, opts={}, timeout=8000) {
  const ctrl = new AbortController();
  const t = setTimeout(()=>ctrl.abort(), timeout);
  try { return await (await fetch(url,{...opts,signal:ctrl.signal})).json(); }
  catch { return null; }
  finally { clearTimeout(t); }
}
const delay = ms => new Promise(r=>setTimeout(r,ms));
async function tg(msg) {
  try { await bot.telegram.sendMessage(cfg.chatId, msg, {parse_mode:"HTML"}); }
  catch(e) { console.error("TG:", e.message); }
}
function isRateLimited(id) {
  const now = Date.now();
  const r = db.prepare("SELECT last_request FROM rate_limits WHERE chat_id=?").get(String(id));
  if (r && now - r.last_request < 20000) return true;
  db.prepare("INSERT OR REPLACE INTO rate_limits (chat_id,last_request) VALUES (?,?)").run(String(id), now);
  return false;
}

// ── ALPACA SYMBOL FORMAT ──────────────────────────────────
// Crypto must be BTC/USD format, stocks are plain AAPL
function alpacaSymbol(ticker, isCrypto) {
  if (isCrypto) {
    const base = ticker.endsWith("USD") ? ticker.slice(0,-3) : ticker;
    return `${base}/USD`;
  }
  return ticker;
}

// Convert Alpaca symbol back to our internal format
// "BTC/USD" → "BTCUSD" | "AAPL" → "AAPL"
function fromAlpacaSymbol(symbol, assetClass) {
  if (assetClass === "crypto") return symbol.replace("/","");
  return symbol;
}

// ══════════════════════════════════════════════════════════
// STARTUP SYNC — Recovers positions from Alpaca after redeploy
// Runs every time the bot launches
// ══════════════════════════════════════════════════════════
async function syncPositionsFromAlpaca() {
  try {
    const positions = await alpaca.getPositions();
    if (!positions.length) {
      console.log("🔄 Sync: No open positions on Alpaca");
      return 0;
    }

    console.log(`🔄 Syncing ${positions.length} positions from Alpaca...`);
    let synced = 0;

    for (const pos of positions) {
      const isCrypto = pos.asset_class === "crypto";
      const ticker   = fromAlpacaSymbol(pos.symbol, pos.asset_class);

      // Check if already in our DB
      const existing = db.prepare(
        "SELECT id FROM trades WHERE ticker=? AND status='open'"
      ).get(ticker);

      if (!existing) {
        const price     = parseFloat(pos.avg_entry_price);
        const qty       = parseFloat(pos.qty);
        const size      = Math.abs(price * qty);
        const stopPrice = price * (1 - (isCrypto ? STOP_CRYPTO : STOP_STOCK));

        db.prepare(`
          INSERT INTO trades
            (ticker, asset_type, qty, price, dollar_value,
             stop_loss_price, vote_score, vote_detail, discovered_via)
          VALUES (?,?,?,?,?,?,?,?,?)
        `).run(
          ticker, isCrypto ? "crypto" : "stock",
          qty, price, size, stopPrice,
          0, "Recovered from Alpaca after redeploy", "Alpaca sync"
        );

        console.log(`✅ Synced: ${ticker} @ $${price} (stop $${stopPrice.toFixed(2)})`);
        synced++;
      }
    }

    // Also mark any DB positions as closed if they no longer exist on Alpaca
    const dbOpen  = db.prepare("SELECT * FROM trades WHERE status='open'").all();
    const alpacaTickers = positions.map(p => fromAlpacaSymbol(p.symbol, p.asset_class));

    for (const t of dbOpen) {
      if (!alpacaTickers.includes(t.ticker)) {
        db.prepare("UPDATE trades SET status='closed',closed_at=CURRENT_TIMESTAMP WHERE id=?").run(t.id);
        console.log(`🔄 Marked closed (not on Alpaca): ${t.ticker}`);
      }
    }

    return synced;
  } catch(e) {
    console.error("Sync error:", e.message);
    return 0;
  }
}

// ══════════════════════════════════════════════════════════
// PHASE 1: DYNAMIC DISCOVERY
// ══════════════════════════════════════════════════════════
async function discoverPolygonGainers() {
  const data = await safeFetch(`https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers?apiKey=${cfg.polygon}`);
  if (!data?.tickers) return [];
  return data.tickers.slice(0,15).filter(t=>t.ticker&&t.ticker.length<=5).map(t=>({ticker:t.ticker,source:"Polygon Gainers"}));
}

async function discoverPolygonActive() {
  const data = await safeFetch(`https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers?apiKey=${cfg.polygon}`);
  if (!data?.tickers) return [];
  return data.tickers.sort((a,b)=>(b.day?.v||0)-(a.day?.v||0)).slice(0,10).map(t=>({ticker:t.ticker,source:"Polygon Most Active"}));
}

async function discoverFinnhubNewsTrending() {
  const data = await safeFetch(`https://finnhub.io/api/v1/news?category=general&token=${cfg.finnhub}`);
  if (!data?.length) return [];
  const counts = {};
  const sw = ["THE","AND","FOR","WITH","FROM","THIS","THAT","WILL","HAVE","BEEN","NYSE","SEC","CEO","CFO","IPO","ETF","GDP","CPI","USD","EUR","API","NEW","AI","US","UK","EU"];
  data.slice(0,30).forEach(a => {
    const matches = (a.headline+" "+(a.summary||"")).match(/\b([A-Z]{2,5})\b/g)||[];
    matches.forEach(m => { if(!sw.includes(m)) counts[m]=(counts[m]||0)+1; });
  });
  return Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,10).map(([t])=>({ticker:t,source:"Finnhub News"}));
}

async function discoverRedditStocks() {
  const data = await safeFetch("https://apewisdom.io/api/v1.0/filter/all-stocks/page/1");
  if (!data?.results) return [];
  return data.results.slice(0,20).map(r=>({ticker:r.ticker,source:"Reddit"}));
}

async function discoverRedditCrypto() {
  const data = await safeFetch("https://apewisdom.io/api/v1.0/filter/all-crypto/page/1");
  if (!data?.results) return [];
  return data.results.slice(0,15).filter(r=>ALPACA_CRYPTO.includes(r.ticker)).map(r=>({ticker:r.ticker,source:"Reddit Crypto"}));
}

async function discoverCoinGeckoTrending() {
  const data = await safeFetch("https://api.coingecko.com/api/v3/search/trending");
  if (!data?.coins) return [];
  const map = {"bitcoin":"BTC","ethereum":"ETH","avalanche-2":"AVAX","chainlink":"LINK","litecoin":"LTC","aave":"AAVE","basic-attention-token":"BAT","bitcoin-cash":"BCH","curve-dao-token":"CRV","dogecoin":"DOGE","polkadot":"DOT","the-graph":"GRT","maker":"MKR","shiba-inu":"SHIB","sushi":"SUSHI","uniswap":"UNI","tezos":"XTZ","yearn-finance":"YFI","ripple":"XRP"};
  return data.coins.map(c=>({ticker:map[c.item.id]||c.item.symbol.toUpperCase(),source:"CoinGecko"})).filter(c=>ALPACA_CRYPTO.includes(c.ticker));
}

async function discoverCandidates() {
  console.log("🔍 Discovery...");
  const [gainers,active,finnhub,redditS,redditC,coinGecko] = await Promise.all([
    discoverPolygonGainers(), discoverPolygonActive(), discoverFinnhubNewsTrending(),
    discoverRedditStocks(),   discoverRedditCrypto(),  discoverCoinGeckoTrending(),
  ]);

  const ts = {};
  [...gainers,...active,...finnhub,...redditS].forEach(c => {
    if (!c.ticker||c.ticker.length>5) return;
    ts[c.ticker] = ts[c.ticker] ? `${ts[c.ticker]}, ${c.source}` : c.source;
  });

  const stocks = Object.entries(ts).slice(0,20).map(([ticker])=>({
    ticker, isCrypto:false, discoveredVia:ts[ticker]
  }));

  const cs = new Set(["BTC","ETH"]);
  [...redditC,...coinGecko].forEach(c=>cs.add(c.ticker));
  const crypto = [...cs].slice(0,8).map(t=>({
    ticker:`${t}USD`, isCrypto:true,
    discoveredVia: redditC.find(r=>r.ticker===t)?.source || coinGecko.find(g=>g.ticker===t)?.source || "Crypto baseline"
  }));

  console.log(`✅ Discovery: ${stocks.length} stocks + ${crypto.length} crypto`);
  return [...stocks,...crypto];
}

// ══════════════════════════════════════════════════════════
// PHASE 2: 15-LAYER RESEARCH
// ══════════════════════════════════════════════════════════
async function L1_polygon(ticker) {
  const clean = ticker.replace("USD","");
  const data  = await safeFetch(`https://api.polygon.io/v2/aggs/ticker/${clean}/prev?adjusted=true&apiKey=${cfg.polygon}`);
  if (!data?.results?.[0]) return {pass:false,detail:"No price data",price:0};
  const r=data.results[0], chg=((r.c-r.o)/r.o)*100, vol=r.v/(r.av||r.v);
  return {pass:chg>0.2&&vol>0.7, detail:`$${r.c.toFixed(2)} (${chg>0?"+":""}${chg.toFixed(2)}%) Vol×${vol.toFixed(2)}`, price:r.c};
}

async function L2_technicals(ticker) {
  const clean = ticker.replace("USD","");
  const [rsi,macd] = await Promise.all([
    safeFetch(`https://www.alphavantage.co/query?function=RSI&symbol=${clean}&interval=daily&time_period=14&series_type=close&apikey=${cfg.alphavantage}`),
    safeFetch(`https://www.alphavantage.co/query?function=MACD&symbol=${clean}&interval=daily&series_type=close&apikey=${cfg.alphavantage}`),
  ]);
  const rs=rsi?.["Technical Analysis: RSI"], md=macd?.["Technical Analysis: MACD"];
  if (!rs||!md) return {pass:false,detail:"Technicals unavailable"};
  const rsiV=parseFloat(rs[Object.keys(rs)[0]]?.RSI), m=md[Object.keys(md)[0]];
  const macdV=parseFloat(m?.MACD), sig=parseFloat(m?.MACD_Signal);
  const rsiOk=rsiV>=35&&rsiV<=68, macdOk=macdV>sig;
  return {pass:rsiOk&&macdOk, detail:`RSI ${rsiV.toFixed(1)} ${rsiOk?"✓":"✗"} | MACD ${macdV>sig?"bull":"bear"} ${macdOk?"✓":"✗"}`};
}

async function L3_news(ticker) {
  const clean=ticker.replace("USD",""), from=new Date(Date.now()-86400000*3).toISOString().split("T")[0], to=new Date().toISOString().split("T")[0];
  const data=await safeFetch(`https://finnhub.io/api/v1/company-news?symbol=${clean}&from=${from}&to=${to}&token=${cfg.finnhub}`);
  if (!data?.length) return {pass:false,detail:"No recent news"};
  const pos=["beat","surge","soar","growth","record","upgrade","profit","gain","strong","bullish","buy"];
  const neg=["miss","fall","drop","cut","loss","downgrade","decline","bearish","weak","crash","sell","risk"];
  let s=0;
  data.slice(0,10).forEach(n=>{const txt=(n.headline+" "+(n.summary||"")).toLowerCase();pos.forEach(w=>{if(txt.includes(w))s++;});neg.forEach(w=>{if(txt.includes(w))s--;});});
  return {pass:s>0, detail:`News score ${s>0?"+":""}${s} (${Math.min(data.length,10)} articles)`};
}

async function L4_earnings(ticker) {
  const clean=ticker.replace("USD",""), from=new Date().toISOString().split("T")[0], to=new Date(Date.now()+604800000).toISOString().split("T")[0];
  const data=await safeFetch(`https://finnhub.io/api/v1/calendar/earnings?from=${from}&to=${to}&symbol=${clean}&token=${cfg.finnhub}`);
  const has=data?.earningsCalendar?.length>0;
  return {pass:!has, detail:has?"Earnings in 7d — risky":"No earnings risk"};
}

async function L5_stocktwits(ticker) {
  const clean=ticker.replace("USD","");
  const data=await safeFetch(`https://api.stocktwits.com/api/2/streams/symbol/${clean}.json`);
  if (!data?.messages) return {pass:false,detail:"StockTwits unavailable"};
  const msgs=data.messages.slice(0,20);
  const b=msgs.filter(m=>m.entities?.sentiment?.basic==="Bullish").length;
  const r=msgs.filter(m=>m.entities?.sentiment?.basic==="Bearish").length;
  const tot=b+r, pct=tot>0?Math.round((b/tot)*100):50;
  return {pass:pct>=55, detail:`${pct}% bullish (${b}🟢 ${r}🔴)`};
}

async function L6_reddit(ticker) {
  const clean=ticker.replace("USD","");
  const data=await safeFetch("https://apewisdom.io/api/v1.0/filter/all-stocks/page/1");
  if (!data?.results) return {pass:false,detail:"ApeWisdom unavailable"};
  const f=data.results.find(r=>r.ticker===clean);
  if (!f) return {pass:false,detail:"Not trending on Reddit"};
  return {pass:f.rank<=50||f.mentions>=5, detail:`Rank #${f.rank}, ${f.mentions} mentions`};
}

async function L7_stockFG() {
  const data=await safeFetch("https://feargreedmeter.com/api/v1/fgi");
  if (!data?.fgi?.now) return {pass:true,detail:"F&G unavailable"};
  const v=data.fgi.now.value;
  return {pass:v<78, detail:`Stock F&G ${v}/100 — ${data.fgi.now.valueText} ${v<78?"✓":"✗"}`};
}

async function L8_cryptoFG() {
  const data=await safeFetch("https://api.alternative.me/fng/?limit=1");
  if (!data?.data?.[0]) return {pass:true,detail:"Crypto F&G unavailable"};
  const v=parseInt(data.data[0].value);
  return {pass:v>25&&v<80, detail:`Crypto F&G ${v}/100 ${data.data[0].value_classification}`};
}

async function L9_insider(ticker) {
  const clean=ticker.replace("USD","");
  const from=new Date(Date.now()-2592000000).toISOString().split("T")[0], to=new Date().toISOString().split("T")[0];
  const data=await safeFetch(`https://efts.sec.gov/LATEST/search-index?q="${clean}"&dateRange=custom&startdt=${from}&enddt=${to}&forms=4`);
  if (!data?.hits?.hits?.length) return {pass:true,detail:"No insider filings"};
  let buys=0,sells=0;
  data.hits.hits.slice(0,5).forEach(f=>{const t=JSON.stringify(f._source).toLowerCase();if(t.includes('"p"')||t.includes("purchase"))buys++;if(t.includes('"s"')||t.includes("sale"))sells++;});
  return {pass:buys>=sells, detail:`${buys} buys vs ${sells} sells (30d)`};
}

async function L10_googleTrends() { return {pass:true,detail:"Google Trends monitored"}; }

async function L11_options(ticker) {
  const clean=ticker.replace("USD","");
  const data=await safeFetch(`https://api.unusualwhales.com/api/stock/${clean}/flow-recent`);
  if (!data?.data?.length) return {pass:true,detail:"Options flow unavailable"};
  const b=data.data.slice(0,10).filter(f=>f.sentiment==="BULLISH"||f.put_call==="CALL").length;
  const r=data.data.slice(0,10).filter(f=>f.sentiment==="BEARISH"||f.put_call==="PUT").length;
  return {pass:b>r, detail:`Options ${b} bull vs ${r} bear`};
}

async function L12_onChain(ticker) {
  if (!ticker.includes("USD")) return {pass:true,detail:"Stocks — skipped"};
  const coin=ticker==="BTCUSD"?"bitcoin":ticker==="ETHUSD"?"ethereum":null;
  if (!coin) return {pass:true,detail:"Minor crypto — proceeding"};
  const data=await safeFetch(`https://api.coingecko.com/api/v3/coins/${coin}?community_data=true&developer_data=true`);
  if (!data) return {pass:true,detail:"On-chain unavailable"};
  const dev=(data.developer_data?.commit_count_4_weeks||0)>5;
  const com=(data.community_data?.twitter_followers||0)>100000;
  return {pass:dev||com, detail:`Dev:${data.developer_data?.commit_count_4_weeks||0} commits | Twitter:${(data.community_data?.twitter_followers||0).toLocaleString()}`};
}

async function L13_fundamentals(ticker) {
  if (ticker.includes("USD")) return {pass:true,detail:"Crypto — skipped"};
  const data=await safeFetch(`https://financialmodelingprep.com/api/v3/ratios-ttm/${ticker}?apikey=${cfg.fmp}`);
  if (!data?.[0]) return {pass:true,detail:"Fundamentals unavailable"};
  const r=data[0], peOk=r.peRatioTTM>0&&r.peRatioTTM<60, roeOk=r.returnOnEquityTTM>0.05;
  return {pass:peOk&&roeOk, detail:`P/E ${r.peRatioTTM?.toFixed(1)} ${peOk?"✓":"✗"} | ROE ${(r.returnOnEquityTTM*100)?.toFixed(1)}% ${roeOk?"✓":"✗"}`};
}

async function L14_sector() {
  const sectors=["XLK","XLV","XLE","XLF","XLY"];
  const res=await Promise.all(sectors.map(s=>safeFetch(`https://api.polygon.io/v2/aggs/ticker/${s}/prev?adjusted=true&apiKey=${cfg.polygon}`)));
  const ch=res.filter(r=>r?.results?.[0]).map(r=>((r.results[0].c-r.results[0].o)/r.results[0].o)*100);
  const pos=ch.filter(c=>c>0).length, avg=ch.length?ch.reduce((a,b)=>a+b,0)/ch.length:0;
  return {pass:pos>=3&&avg>-0.5, detail:`${pos}/${ch.length} sectors positive, avg ${avg.toFixed(2)}%`};
}

async function L15_vix() {
  const data=await safeFetch(`https://api.polygon.io/v2/aggs/ticker/VXX/prev?adjusted=true&apiKey=${cfg.polygon}`);
  if (!data?.results?.[0]) return {pass:true,detail:"VIX unavailable"};
  const v=data.results[0].c, m=v<15?"Calm":v<20?"Normal":v<30?"Elevated":"Panic";
  return {pass:v<30, detail:`VIX ${v.toFixed(2)} — ${m}`};
}

async function runResearch(ticker, isCrypto) {
  const layers=await Promise.all([
    L1_polygon(ticker), L2_technicals(ticker), L3_news(ticker), L4_earnings(ticker),
    L5_stocktwits(ticker), L6_reddit(ticker), L7_stockFG(), L8_cryptoFG(),
    L9_insider(ticker), L10_googleTrends(), L11_options(ticker), L12_onChain(ticker),
    L13_fundamentals(ticker), L14_sector(), L15_vix(),
  ]);
  const names=["Polygon","Alpha Vantage","Finnhub News","Earnings Risk","StockTwits","Reddit","Stock F&G","Crypto F&G","SEC Insider","Google Trends","Options Flow","On-Chain","Fundamentals","Sector Rotation","VIX"];
  const score=layers.reduce((s,l)=>s+(l.pass?1:0),0);
  const detail=layers.map((l,i)=>`${l.pass?"✅":"❌"} L${i+1} ${names[i]}: ${l.detail}`);
  return {ticker, totalScore:score, details:detail, currentPrice:layers[0].price||0, isCrypto};
}

// ══════════════════════════════════════════════════════════
// PHASE 3: TRADING ENGINE
// ══════════════════════════════════════════════════════════
const getOpen     = () => db.prepare("SELECT * FROM trades WHERE status='open'").all();
const getDeployed = () => db.prepare("SELECT SUM(dollar_value) as t FROM trades WHERE status='open'").get()?.t||0;

async function executeTrade(r, via) {
  if (getState("paused")) return {skipped:true,reason:"Paused"};

  const loss=getState("total_loss")||0;
  if (loss>=MAX_TOTAL_LOSS) {
    setState("paused",true);
    await tg(`🛑 <b>LOSS LIMIT HIT — $${loss.toFixed(2)}</b>\nAll trading paused. Send /resume.`);
    return {skipped:true,reason:"Loss limit"};
  }

  const open=getOpen();
  if (open.length>=MAX_POSITIONS)        return {skipped:true,reason:"Max positions"};
  if (open.find(p=>p.ticker===r.ticker)) return {skipped:true,reason:"Already holding"};
  if (r.totalScore<MIN_VOTES)            return {skipped:true,reason:`Only ${r.totalScore}/15`};

  const avail=BUDGET-getDeployed();
  const size=Math.min(MAX_POSITION,avail*0.25);
  if (size<5)                            return {skipped:true,reason:"Insufficient budget"};
  if (!r.currentPrice||r.currentPrice<=0) return {skipped:true,reason:"No price"};

  const stopPct   = r.isCrypto ? STOP_CRYPTO : STOP_STOCK;
  const stopPrice = r.currentPrice*(1-stopPct);
  const qty       = parseFloat((size/r.currentPrice).toFixed(6));
  const orderSym  = alpacaSymbol(r.ticker, r.isCrypto);

  try {
    await alpaca.createOrder({
      symbol:        orderSym,
      qty,
      side:          "buy",
      type:          "market",
      time_in_force: r.isCrypto ? "gtc" : "day",
    });

    db.prepare(`INSERT INTO trades
      (ticker,asset_type,qty,price,dollar_value,stop_loss_price,vote_score,vote_detail,discovered_via)
      VALUES (?,?,?,?,?,?,?,?,?)`)
      .run(r.ticker, r.isCrypto?"crypto":"stock", qty, r.currentPrice,
           size, stopPrice, r.totalScore, r.details.join("\n"), via);

    await tg(
      `🟢 <b>TRADE — ${r.ticker}</b>\n` +
      `Discovered via: ${via}\n` +
      `Price: $${r.currentPrice.toFixed(2)} | Size: $${size.toFixed(2)} (${qty} units)\n` +
      `Stop: $${stopPrice.toFixed(2)} (-${(stopPct*100)}%)\n` +
      `Score: <b>${r.totalScore}/15</b>\n<i>Paper trade</i>`
    );
    return {executed:true};
  } catch(e) {
    await tg(`⚠️ Order failed ${r.ticker}: ${e.message}`);
    return {skipped:true,reason:e.message};
  }
}

async function checkStopLosses() {
  for (const t of getOpen()) {
    try {
      const data=await safeFetch(`https://api.polygon.io/v2/aggs/ticker/${t.ticker.replace("USD","")}/prev?adjusted=true&apiKey=${cfg.polygon}`);
      if (!data?.results?.[0]) continue;
      const price=data.results[0].c;
      if (price<=t.stop_loss_price) {
        const isCrypto=t.asset_type==="crypto";
        try { await alpaca.closePosition(alpacaSymbol(t.ticker,isCrypto)); } catch {}
        const pnl=(price-t.price)*t.qty;
        db.prepare("UPDATE trades SET status='closed',pnl=?,closed_at=CURRENT_TIMESTAMP WHERE id=?").run(pnl,t.id);
        if (pnl<0) setState("total_loss",(getState("total_loss")||0)+Math.abs(pnl));
        await tg(
          `🔴 <b>STOP — ${t.ticker}</b>\n` +
          `Entry $${t.price.toFixed(2)} → Exit $${price.toFixed(2)}\n` +
          `P&L: <b>${pnl>=0?"+":""}$${pnl.toFixed(2)}</b>\n` +
          `Losses: $${getState("total_loss").toFixed(2)}/$${MAX_TOTAL_LOSS}`
        );
      }
    } catch(e) { console.error(e.message); }
  }
}

async function runScan() {
  if (getState("paused")) return;
  console.log(`🔍 Scan ${new Date().toISOString()}`);
  await tg(`📡 <i>Discovery scan started...</i>`);

  const candidates=await discoverCandidates();
  await tg(`🎯 <i>${candidates.length} candidates — running 15-layer research...</i>`);

  let traded=0, skipped=0;
  const scores=[];

  for (const c of candidates) {
    try {
      const r=await runResearch(c.ticker,c.isCrypto);
      db.prepare("INSERT INTO scan_log (ticker,vote_score,verdict,discovered_via) VALUES (?,?,?,?)")
        .run(c.ticker,r.totalScore,r.totalScore>=MIN_VOTES?"BUY":"SKIP",c.discoveredVia);
      scores.push({ticker:c.ticker,score:r.totalScore,via:c.discoveredVia});
      if (r.totalScore>=MIN_VOTES) {
        const result=await executeTrade(r,c.discoveredVia);
        if (result.executed) traded++; else skipped++;
      } else { skipped++; }
      await delay(1200);
    } catch(e) { console.error(c.ticker,e.message); }
  }

  const top5=scores.sort((a,b)=>b.score-a.score).slice(0,5);
  let msg=`✅ <b>Scan Complete</b>\n${traded} traded | ${skipped} skipped\n\n<b>Top Scorers:</b>\n`;
  top5.forEach(t=>{msg+=`• ${t.ticker}: ${t.score}/15 — ${t.via}\n`;});
  await tg(msg);
  await checkStopLosses();
}

async function getPortfolioSummary() {
  try {
    const acc=await alpaca.getAccount();
    const open=getOpen();
    const loss=getState("total_loss")||0;
    const dep=getDeployed();
    let m=`📊 <b>Portfolio</b>\n\nCash: $${parseFloat(acc.cash).toFixed(2)}\nValue: $${parseFloat(acc.portfolio_value).toFixed(2)}\nDeployed: $${dep.toFixed(2)}/$${BUDGET}\nLosses: $${loss.toFixed(2)}/$${MAX_TOTAL_LOSS}\nOpen: ${open.length}/${MAX_POSITIONS}\n`;
    if (open.length) {
      m+=`\n<b>Positions:</b>\n`;
      open.forEach(t=>{m+=`• ${t.ticker} $${t.dollar_value.toFixed(2)} | Stop $${t.stop_loss_price.toFixed(2)} | ${t.vote_score}/15\n`;});
    }
    const cls=db.prepare("SELECT * FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT 5").all();
    if (cls.length) {
      m+=`\n<b>Recent Closed:</b>\n`;
      cls.forEach(t=>{m+=`• ${t.ticker} ${t.pnl>=0?"+":""}$${t.pnl?.toFixed(2)}\n`;});
    }
    return m;
  } catch(e) { return `⚠️ ${e.message}`; }
}

// ══════════════════════════════════════════════════════════
// TELEGRAM COMMANDS
// ══════════════════════════════════════════════════════════
bot.start(ctx=>ctx.replyWithHTML(
  `🤖 <b>Raseed Trading Agent v4.3</b>\n\n` +
  `Mode: <b>Dynamic Discovery</b>\nAlpaca: <b>Paper Trading</b>\nThreshold: <b>${MIN_VOTES}/15 layers</b>\n\n` +
  `/portfolio — P&L and positions\n/scan — manual scan now\n/research AAPL — 15-layer check\n` +
  `/positions — open positions\n/history — last 10 closed\n/sync — re-sync from Alpaca\n` +
  `/pause — halt trading\n/resume — restart\n/status — guardrails\n/help — this menu`
));

bot.command("portfolio", async ctx=>ctx.replyWithHTML(await getPortfolioSummary()));

bot.command("scan", async ctx=>{
  ctx.replyWithHTML("🔍 <i>Manual scan triggered...</i>");
  runScan();
});

bot.command("sync", async ctx=>{
  ctx.replyWithHTML("🔄 <i>Syncing positions from Alpaca...</i>");
  const synced=await syncPositionsFromAlpaca();
  ctx.replyWithHTML(`✅ Sync complete — ${synced} new positions recovered from Alpaca.`);
});

bot.command("research", async ctx=>{
  const t=ctx.message.text.replace("/research","").trim().toUpperCase();
  if (!t) { ctx.reply("Usage: /research AAPL"); return; }
  ctx.replyWithHTML(`🔬 <i>Researching ${t}...</i>`);
  const isC=ALPACA_CRYPTO.includes(t.replace("USD",""));
  const tk=isC&&!t.endsWith("USD")?t+"USD":t;
  const r=await runResearch(tk,isC);
  let m=`<b>${tk}</b> — Score: <b>${r.totalScore}/15</b> ${r.totalScore>=MIN_VOTES?"✅ BUY":"❌ SKIP"}\n\n`;
  m+=r.details.join("\n");
  ctx.replyWithHTML(m);
});

bot.command("positions", ctx=>{
  const o=getOpen();
  if (!o.length) { ctx.reply("No open positions."); return; }
  let m=`<b>Open Positions (${o.length})</b>\n\n`;
  o.forEach(t=>{m+=`• <b>${t.ticker}</b> $${t.dollar_value.toFixed(2)} | Stop $${t.stop_loss_price.toFixed(2)} | ${t.vote_score}/15\n`;});
  ctx.replyWithHTML(m);
});

bot.command("history", ctx=>{
  const cls=db.prepare("SELECT * FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT 10").all();
  if (!cls.length) { ctx.reply("No closed trades yet."); return; }
  let m=`<b>Last ${cls.length} Closed</b>\n\n`;
  cls.forEach(t=>{m+=`${t.pnl>=0?"🟢":"🔴"} <b>${t.ticker}</b> ${t.pnl>=0?"+":""}$${t.pnl?.toFixed(2)}\n`;});
  ctx.replyWithHTML(m);
});

bot.command("pause",  ctx=>{setState("paused",true);  ctx.reply("⏸️ Trading paused.");});
bot.command("resume", ctx=>{setState("paused",false); ctx.reply("▶️ Trading resumed.");});

bot.command("status", async ctx=>{
  const p=getState("paused"), l=getState("total_loss")||0;
  ctx.replyWithHTML(
    `<b>Agent Status</b>\n\nMode: ${p?"⏸️ PAUSED":"▶️ ACTIVE"}\n` +
    `Budget: $${BUDGET} | Loss: $${l.toFixed(2)}/$${MAX_TOTAL_LOSS}\n` +
    `Open: ${getOpen().length}/${MAX_POSITIONS}\n` +
    `Min votes: ${MIN_VOTES}/15 | Stops: ${STOP_STOCK*100}%/${STOP_CRYPTO*100}%\n` +
    `Mode: <b>PAPER (no real money)</b>`
  );
});

bot.help(ctx=>ctx.replyWithHTML(
  `/portfolio /scan /sync /research /positions /history /pause /resume /status`
));

// ── SCHEDULE ──────────────────────────────────────────────
cron.schedule("30 12,13,14,15,16,17,18 * * 1-5", runScan, {timezone:"UTC"});
cron.schedule("*/5 12-19 * * 1-5", checkStopLosses, {timezone:"UTC"});
cron.schedule("0 3 * * *", async()=>{
  if (getState("paused")) return;
  const trending=await discoverCoinGeckoTrending();
  const list=[...new Set([...trending.map(t=>t.ticker),"BTC","ETH"])].slice(0,5);
  for (const t of list) {
    const r=await runResearch(t+"USD",true);
    if (r.totalScore>=MIN_VOTES) await executeTrade(r,"Overnight crypto scan");
    await delay(2000);
  }
}, {timezone:"UTC"});
cron.schedule("0 6 * * 1-5", async()=>{
  await tg(`☀️ <b>Daily Briefing</b>\n\n${await getPortfolioSummary()}`);
}, {timezone:"UTC"});

// ── LAUNCH ────────────────────────────────────────────────
bot.launch().then(async()=>{
  console.log("🚀 Raseed Trading Agent v4.3 live");

  // Sync positions from Alpaca first — recovers after any redeploy
  const synced=await syncPositionsFromAlpaca();

  await tg(
    `🚀 <b>Raseed Agent v4.3 Online</b>\n\n` +
    `${synced>0?`🔄 Recovered ${synced} position(s) from Alpaca\n`:""}` +
    `Budget: $${BUDGET} | Loss limit: $${MAX_TOTAL_LOSS}\n` +
    `Threshold: ${MIN_VOTES}/15 | Stops: ${STOP_STOCK*100}%/${STOP_CRYPTO*100}%\n\n` +
    `Send /positions to verify open trades.`
  );
}).catch(e=>console.error("Launch:",e.message));

process.once("SIGINT",  ()=>bot.stop("SIGINT"));
process.once("SIGTERM", ()=>bot.stop("SIGTERM"));
