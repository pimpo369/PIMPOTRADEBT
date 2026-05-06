// ============================================================
// RASEED AUTONOMOUS TRADING AGENT v4.1
// Dynamic Asset Discovery + 15-Layer Research + Alpaca Paper
// FIX v4.1: Alpaca crypto symbol format corrected to BTC/USD
// ============================================================
// HOW IT WORKS EACH SCAN CYCLE:
//   1. DISCOVER  — screens entire market for top candidates
//   2. RESEARCH  — runs all 15 layers on each candidate
//   3. RANK      — sorts candidates by vote score
//   4. EXECUTE   — buys top scorers that pass all guardrails
//
// Alpha Vantage is used ONLY in L2 (RSI/MACD technicals)
//
// GUARDRAILS:
//   Budget: $200 | Loss limit: $50 | Stocks 8% / Crypto 5% stop
//   Max position: $50 | Max positions: 5 | Min votes: 12/15
// ============================================================

const { Telegraf } = require("telegraf");
const Database     = require("better-sqlite3");
const cron         = require("node-cron");
const Alpaca       = require("@alpacahq/alpaca-trade-api");

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
  keyId: cfg.alpacaKey, secretKey: cfg.alpacaSecret,
  paper: true, usePolygon: false,
});

const bot = new Telegraf(cfg.telegram);
const db  = new Database("./trading_agent.db");

db.exec(`
  CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT, asset_type TEXT, qty REAL, price REAL,
    dollar_value REAL, stop_loss_price REAL,
    vote_score INTEGER, vote_detail TEXT, discovered_via TEXT,
    status TEXT DEFAULT 'open', pnl REAL DEFAULT 0,
    opened_at DATETIME DEFAULT CURRENT_TIMESTAMP, closed_at DATETIME
  );
  CREATE TABLE IF NOT EXISTS agent_state (key TEXT PRIMARY KEY, value TEXT);
  CREATE TABLE IF NOT EXISTS scan_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT,
    vote_score INTEGER, verdict TEXT, discovered_via TEXT,
    scanned_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS rate_limits (chat_id TEXT PRIMARY KEY, last_request INTEGER);
`);

const getState = (k,d=null) => { const r=db.prepare("SELECT value FROM agent_state WHERE key=?").get(k); return r?JSON.parse(r.value):d; };
const setState = (k,v) => db.prepare("INSERT OR REPLACE INTO agent_state (key,value) VALUES (?,?)").run(k,JSON.stringify(v));

if (getState("total_loss")===null) setState("total_loss",0);
if (getState("paused")===null)     setState("paused",false);

const BUDGET=200, MAX_TOTAL_LOSS=50, MAX_POSITION=50, MAX_POSITIONS=5;
const STOP_STOCK=0.08, STOP_CRYPTO=0.05, MIN_VOTES=12;

const ALPACA_CRYPTO=["BTC","ETH","AVAX","LINK","LTC","AAVE","BAT","BCH","CRV","DOGE","DOT","GRT","MKR","SHIB","SUSHI","UNI","XTZ","YFI","XRP","SKY"];

async function safeFetch(url,opts={},timeout=8000){const ctrl=new AbortController();const t=setTimeout(()=>ctrl.abort(),timeout);try{return await(await fetch(url,{...opts,signal:ctrl.signal})).json();}catch{return null;}finally{clearTimeout(t);}}
const delay=ms=>new Promise(r=>setTimeout(r,ms));
async function tg(msg){try{await bot.telegram.sendMessage(cfg.chatId,msg,{parse_mode:"HTML"});}catch(e){console.error("TG:",e.message);}}

// KEY FIX: Alpaca crypto must be BTC/USD not BTCUSD
function alpacaSymbol(ticker,isCrypto){
  if(isCrypto){const base=ticker.endsWith("USD")?ticker.slice(0,-3):ticker;return `${base}/USD`;}
  return ticker;
}

async function discoverPolygonGainers(){const data=await safeFetch(`https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers?apiKey=${cfg.polygon}`);if(!data?.tickers)return[];return data.tickers.slice(0,15).filter(t=>t.ticker&&t.ticker.length<=5).map(t=>({ticker:t.ticker,source:"Polygon Gainers"}));}
async function discoverPolygonActive(){const data=await safeFetch(`https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers?apiKey=${cfg.polygon}`);if(!data?.tickers)return[];return data.tickers.sort((a,b)=>(b.day?.v||0)-(a.day?.v||0)).slice(0,10).map(t=>({ticker:t.ticker,source:"Polygon Most Active"}));}
async function discoverFinnhubNewsTrending(){const data=await safeFetch(`https://finnhub.io/api/v1/news?category=general&token=${cfg.finnhub}`);if(!data?.length)return[];const counts={};const sw=["THE","AND","FOR","WITH","FROM","THIS","THAT","WILL","HAVE","BEEN","NYSE","SEC","CEO","CFO","IPO","ETF","GDP","CPI","USD","EUR","API","NEW","AI","US","UK","EU"];data.slice(0,30).forEach(a=>{const matches=(a.headline+" "+(a.summary||"")).match(/\b([A-Z]{2,5})\b/g)||[];matches.forEach(m=>{if(!sw.includes(m))counts[m]=(counts[m]||0)+1;});});return Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,10).map(([t])=>({ticker:t,source:"Finnhub News"}));}
async function discoverRedditStocks(){const data=await safeFetch("https://apewisdom.io/api/v1.0/filter/all-stocks/page/1");if(!data?.results)return[];return data.results.slice(0,20).map(r=>({ticker:r.ticker,source:"Reddit"}));}
async function discoverRedditCrypto(){const data=await safeFetch("https://apewisdom.io/api/v1.0/filter/all-crypto/page/1");if(!data?.results)return[];return data.results.slice(0,15).filter(r=>ALPACA_CRYPTO.includes(r.ticker)).map(r=>({ticker:r.ticker,source:"Reddit Crypto"}));}
async function discoverCoinGeckoTrending(){const data=await safeFetch("https://api.coingecko.com/api/v3/search/trending");if(!data?.coins)return[];const map={"bitcoin":"BTC","ethereum":"ETH","avalanche-2":"AVAX","chainlink":"LINK","litecoin":"LTC","aave":"AAVE","basic-attention-token":"BAT","bitcoin-cash":"BCH","curve-dao-token":"CRV","dogecoin":"DOGE","polkadot":"DOT","the-graph":"GRT","maker":"MKR","shiba-inu":"SHIB","sushi":"SUSHI","uniswap":"UNI","tezos":"XTZ","yearn-finance":"YFI","ripple":"XRP"};return data.coins.map(c=>({ticker:map[c.item.id]||c.item.symbol.toUpperCase(),source:"CoinGecko"})).filter(c=>ALPACA_CRYPTO.includes(c.ticker));}

async function discoverCandidates(){
  console.log("🔍 Discovery...");
  const[gainers,active,finnhub,redditS,redditC,coinGecko]=await Promise.all([discoverPolygonGainers(),discoverPolygonActive(),discoverFinnhubNewsTrending(),discoverRedditStocks(),discoverRedditCrypto(),discoverCoinGeckoTrending()]);
  const ts={};[...gainers,...active,...finnhub,...redditS].forEach(c=>{if(!c.ticker||c.ticker.length>5)return;ts[c.ticker]=ts[c.ticker]?`${ts[c.ticker]}, ${c.source}`:c.source;});
  const stocks=Object.entries(ts).slice(0,20).map(([ticker])=>({ticker,isCrypto:false,discoveredVia:ts[ticker]}));
  const cs=new Set(["BTC","ETH"]);[...redditC,...coinGecko].forEach(c=>cs.add(c.ticker));
  const crypto=[...cs].slice(0,8).map(t=>({ticker:`${t}USD`,isCrypto:true,discoveredVia:redditC.find(r=>r.ticker===t)?.source||coinGecko.find(g=>g.ticker===t)?.source||"Crypto baseline"}));
  return[...stocks,...crypto];
}

async function L1_polygon(ticker){const clean=ticker.replace("USD","");const data=await safeFetch(`https://api.polygon.io/v2/aggs/ticker/${clean}/prev?adjusted=true&apiKey=${cfg.polygon}`);if(!data?.results?.[0])return{pass:false,detail:"No price data",price:0};const
