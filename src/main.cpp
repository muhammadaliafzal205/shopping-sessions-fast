// Shopping Sessions pipeline: hand-written parallel CSV parser + sessionizer + Parquet writer.
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>
#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <emmintrin.h>

typedef uint64_t u64; typedef int64_t i64; typedef uint32_t u32; typedef uint16_t u16; typedef uint8_t u8;
static const i64 DAYMS = 86400000LL, GAPMS = 1800000LL, WEEKMS = 604800000LL;
static const u32 EMPTY = 0xFFFFFFFFu;

// ---------------------------------------------------------------- basics
static inline u64 packs(const char* p, size_t n) { return (u64)(uintptr_t)p | ((u64)n << 48); }
static inline const char* pptr(u64 v) { return (const char*)(uintptr_t)(v & 0xFFFFFFFFFFFFULL); }
static inline size_t plen(u64 v) { return (size_t)(v >> 48); }
static inline i64 fdiv(i64 a, i64 b) { i64 q = a / b; if ((a % b != 0) && ((a < 0) != (b < 0))) q--; return q; }
static inline u64 rd8(const char* p) { u64 v; memcpy(&v, p, 8); return v; }
static inline u64 wmix(u64 a, u64 b) { __uint128_t r = (__uint128_t)a * b; return (u64)r ^ (u64)(r >> 64); }
static inline u64 hashstr(const char* p, size_t n, u64 seed) {
    const u64 K0 = 0xa0761d6478bd642fULL, K1 = 0xe7037ed1a0b428dbULL, K2 = 0x8ebc6af09c88c6e3ULL;
    u64 h = seed ^ ((u64)n * K2);
    while (n >= 8) { h = wmix(h ^ rd8(p), K1); p += 8; n -= 8; }
    if (n) { u64 w = 0; memcpy(&w, p, n); h = wmix(h ^ w ^ K2, K0); }
    return wmix(h ^ K0, K1);
}
static const u64 SEED_ID = 0x243F6A8885A308D3ULL, SEED_E1 = 0x13198A2E03707344ULL, SEED_E2 = 0xA4093822299F31D0ULL;
static inline u64 hid(const char* p, size_t n) { return hashstr(p, n, SEED_ID); }

struct Str { const char* p; u32 n; };

// Split a line into exactly nf fields following the CSV dialect; false = malformed.
// SSE2 fast path: 16 bytes at a time, comma/quote bitmasks; scalar tail (never reads past e).
static inline bool splitline(const char* s, const char* e, int nf, Str* out) {
    int k = 0; const char* fs = s; unsigned qc = 0;
    const char* p = s;
    const __m128i vc = _mm_set1_epi8(','), vq = _mm_set1_epi8('"');
    for (; p + 16 <= e; p += 16) {
        __m128i x = _mm_loadu_si128((const __m128i*)p);
        unsigned mc = (unsigned)_mm_movemask_epi8(_mm_cmpeq_epi8(x, vc));
        qc += (unsigned)__builtin_popcount((unsigned)_mm_movemask_epi8(_mm_cmpeq_epi8(x, vq)));
        while (mc) {
            int b = __builtin_ctz(mc); mc &= mc - 1;
            const char* c = p + b;
            if (k >= nf - 1) return false;
            out[k].p = fs; out[k].n = (u32)(c - fs); ++k; fs = c + 1;
        }
    }
    for (; p < e; ++p) {
        char c = *p;
        if (c == ',') {
            if (k >= nf - 1) return false;
            out[k].p = fs; out[k].n = (u32)(p - fs); ++k; fs = p + 1;
        } else if (c == '"') qc++;
    }
    if (k != nf - 1) return false;
    out[k].p = fs; out[k].n = (u32)(e - fs);
    if (qc) {
        // valid iff every quote belongs to a field that is wrapped in exactly one pair of quotes:
        // a wrapped field holds >= 2 quotes, so total == 2 * (#wrapped) forces exactly 2 each and 0 elsewhere.
        unsigned wrapped = 0;
        for (int i = 0; i < nf; i++) {
            Str& f = out[i];
            if (f.n >= 2 && f.p[0] == '"' && f.p[f.n - 1] == '"') { f.p++; f.n -= 2; ++wrapped; }
        }
        if (qc != 2 * wrapped) return false;
    }
    return true;
}

// ---------------------------------------------------------------- timestamps
static inline bool isd(char c) { return (unsigned char)(c - '0') < 10; }
static inline int d2(const char* p) { return (p[0] - '0') * 10 + (p[1] - '0'); }
static inline i64 days_from_civil(i64 y, unsigned m, unsigned d) {
    y -= m <= 2;
    i64 era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (i64)doe - 719468;
}
static bool parse_ts(const char* p, size_t n, i64& out) {
    if ((n == 20 || n == 25) && p[4] == '-') {
        static const int pos[] = {0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18};
        for (int i : pos) if (!isd(p[i])) return false;
        if (p[7] != '-' || p[10] != 'T' || p[13] != ':' || p[16] != ':') return false;
        int y = d2(p) * 100 + d2(p + 2), mo = d2(p + 5), d = d2(p + 8), h = d2(p + 11), mi = d2(p + 14), s = d2(p + 17);
        if (mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || s > 59) return false;
        static const int dm[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        int dim = dm[mo - 1];
        if (mo == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) dim = 29;
        if (d > dim) return false;
        i64 off = 0;
        if (n == 20) { if (p[19] != 'Z') return false; }
        else {
            char sg = p[19];
            if (sg != '+' && sg != '-') return false;
            if (!isd(p[20]) || !isd(p[21]) || p[22] != ':' || !isd(p[23]) || !isd(p[24])) return false;
            int oh = d2(p + 20), om = d2(p + 23);
            if (oh > 23 || om > 59) return false;
            off = (i64)(oh * 60 + om) * 60000LL; if (sg == '-') off = -off;
        }
        out = days_from_civil(y, (unsigned)mo, (unsigned)d) * DAYMS + (i64)((h * 60 + mi) * 60 + s) * 1000LL - off;
        return true;
    }
    if (n == 0) return false;
    for (size_t i = 0; i < n; i++) if (!isd(p[i])) return false;
    size_t z = 0; while (z < n && p[z] == '0') z++;
    if (n - z > 18) return false;
    i64 v = 0; for (size_t i = z; i < n; i++) v = v * 10 + (p[i] - '0');
    out = v; return true;
}

// ---------------------------------------------------------------- hash tables
struct Slot { u64 k; u32 v; u32 pad; };
struct HT {
    std::vector<Slot> s; u64 mask = 0;
    void init(size_t n) { size_t cap = 16; while (cap < 2 * n) cap <<= 1; s.assign(cap, Slot{0, EMPTY, 0}); mask = cap - 1; }
    u32 get(u64 k) const { size_t i = k & mask; for (;;) { const Slot& x = s[i]; if (x.v == EMPTY) return EMPTY; if (x.k == k) return x.v; i = (i + 1) & mask; } }
    void put(u64 k, u32 v) { size_t i = k & mask; for (;;) { Slot& x = s[i]; if (x.v == EMPTY) { x.k = k; x.v = v; return; } if (x.k == k) { x.v = v; return; } i = (i + 1) & mask; } }
};

// ---------------------------------------------------------------- files
struct MFile { const char* d; size_t n; };
static MFile mapfile(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return {nullptr, 0};
    struct stat st; fstat(fd, &st);
    if (st.st_size == 0) { close(fd); return {nullptr, 0}; }
    void* m = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (m == MAP_FAILED) return {nullptr, 0};
    madvise(m, (size_t)st.st_size, MADV_WILLNEED);  // start asynchronous read-ahead
    return {(const char*)m, (size_t)st.st_size};
}
static std::vector<std::string> list_csv(const std::string& dir) {
    std::vector<std::string> v;
    DIR* d = opendir(dir.c_str());
    if (!d) return v;
    while (dirent* e = readdir(d)) {
        std::string nm = e->d_name;
        if (nm.size() > 4 && nm.compare(nm.size() - 4, 4, ".csv") == 0) v.push_back(dir + "/" + nm);
    }
    closedir(d);
    std::sort(v.begin(), v.end());
    return v;
}
// calls f(lineStart, lineEnd) for each data line whose first byte offset lies in [s,e); header skipped.
template <class F> static inline void for_lines(const char* base, size_t size, size_t s, size_t e, F&& f) {
    size_t pos = s;
    if (s == 0) {
        const char* nl = (const char*)memchr(base, '\n', size);
        if (!nl) return;
        pos = (size_t)(nl + 1 - base);
    } else if (base[s - 1] != '\n') {
        const char* nl = (const char*)memchr(base + s, '\n', size - s);
        if (!nl) return;
        pos = (size_t)(nl + 1 - base);
    }
    while (pos < e) {
        const char* ls = base + pos;
        const char* nl = (const char*)memchr(ls, '\n', size - pos);
        const char* le = nl ? nl : base + size;
        f(ls, le);
        if (!nl) break;
        pos = (size_t)(nl + 1 - base);
    }
}

template <class F> static void run_parallel(int T, size_t ntasks, F&& fn) {
    std::atomic<size_t> next{0};
    auto body = [&](int tid) { for (;;) { size_t t = next.fetch_add(1); if (t >= ntasks) break; fn(tid, t); } };
    std::vector<std::thread> th;
    for (int i = 1; i < T; i++) th.emplace_back(body, i);
    body(0);
    for (auto& t : th) t.join();
}

// ---------------------------------------------------------------- records
struct EvRec { u64 h1; u64 uh; u64 nm; i64 ts; i64 ingest; u32 h2; u32 price; u16 cat; u8 type; u8 ok; };
struct SvRec { u64 pkey; u64 nm; i64 ts; u32 price; u16 cat; u8 type; u8 pad; };
struct ClRec { u64 uh; i64 ts; u64 cid; u16 camp; };
struct LinkRec { u64 ha, hb, pa, pb; };
static const int DB = 9, D = 1 << DB;      // dedup partitions
static const int PB = 10, P = 1 << PB;     // person partitions

struct URow { u64 nm; i64 rev; u32 day; u32 sessions; u32 events; int32_t top; };

struct Worker {
    std::vector<EvRec> evb[D];
    std::vector<ClRec> clk;
    std::vector<LinkRec> lk;
    std::vector<SvRec> evp[P];
    std::vector<ClRec> clp[P];
    // dedup scratch
    struct DS { u64 h1; u32 h2; u32 ref; };
    std::vector<DS> tab; std::vector<const EvRec*> cand;
    // person scratch
    std::vector<SvRec> ev, ev2; std::vector<ClRec> cl; std::vector<u32> hist;
    std::vector<u32> cnt, apos, active, apos2;
    std::vector<i64> catrev; std::vector<int> touched;
    std::vector<u64> st_sess, st_purch, st_assist; std::vector<i64> st_rev;
    std::vector<URow> rows;
};

// ---------------------------------------------------------------- globals
static i64 WS, WE, CWS;
static HT prodT, campT;
static std::vector<std::string> catNames;   // category names incl. unknown at back
static int UNKCAT;
static std::vector<std::string> campIds, campChan;
static HT linkT; static std::vector<u64> linkPk, linkNm;   // person key / name of each linked id
static int NT = 8, ND = 1; static i64 DAY0 = 0;
static std::vector<Worker> W;
static std::vector<int> catRank;

static bool read_manifest(const std::string& path) {
    MFile m = mapfile(path);
    if (!m.d) return false;
    std::string s(m.d, m.n);
    auto getv = [&](const char* key, i64& out) -> bool {
        size_t k = s.find(key); if (k == std::string::npos) return false;
        size_t c = s.find(':', k); if (c == std::string::npos) return false;
        size_t q1 = s.find('"', c); if (q1 == std::string::npos) return false;
        size_t q2 = s.find('"', q1 + 1); if (q2 == std::string::npos) return false;
        return parse_ts(s.data() + q1 + 1, q2 - q1 - 1, out);
    };
    return getv("\"window_start\"", WS) && getv("\"window_end\"", WE);
}

struct Task { int kind; const char* base; size_t size, s, e; };

// ---------------------------------------------------------------- parse handlers
static inline void handle_event(Worker& w, const char* ls, const char* le) {
    Str f[7];
    if (!splitline(ls, le, 7, f)) return;
    if (f[0].n == 0) return;
    EvRec r;
    r.h1 = hashstr(f[0].p, f[0].n, SEED_E1);
    r.h2 = (u32)(hashstr(f[0].p, f[0].n, SEED_E2) >> 32);
    i64 ing; bool ingbad = !parse_ts(f[3].p, f[3].n, ing);
    if (ingbad) ing = INT64_MAX;
    r.ingest = ing; r.ok = 0; r.uh = 0; r.nm = 0; r.ts = 0; r.price = 0; r.cat = 0; r.type = 0;
    i64 ts;
    if (!ingbad && f[1].n && f[5].n && parse_ts(f[2].p, f[2].n, ts) && ts >= WS && ts < WE && ing - ts <= DAYMS) {
        int type = -1;
        if (f[4].n == 4) { if (!memcmp(f[4].p, "view", 4)) type = 0; else if (!memcmp(f[4].p, "cart", 4)) type = 1; }
        else if (f[4].n == 8 && !memcmp(f[4].p, "purchase", 8)) type = 2;
        if (type >= 0) {
            bool good = true; u32 price = 0;
            if (type == 2) {
                const char* q = f[6].p; u32 n = f[6].n; good = n > 0;
                for (u32 i = 0; i < n && good; i++) if (!isd(q[i])) good = false;
                if (good) {
                    u32 z = 0; while (z < n && q[z] == '0') z++;
                    if (n - z > 8) good = false;
                    else { u64 v = 0; for (u32 i = z; i < n; i++) v = v * 10 + (u64)(q[i] - '0'); if (v < 1 || v > 10000000) good = false; else price = (u32)v; }
                }
            }
            if (good) {
                r.ok = 1; r.type = (u8)type; r.ts = ts; r.price = price;
                r.uh = hid(f[1].p, f[1].n); r.nm = packs(f[1].p, f[1].n);
                if (type == 2) { u32 c = prodT.get(hid(f[5].p, f[5].n)); r.cat = (u16)(c == EMPTY ? UNKCAT : (int)c); }
            }
        }
    }
    w.evb[r.h1 >> (64 - DB)].push_back(r);
}
static inline void handle_click(Worker& w, const char* ls, const char* le) {
    Str f[4];
    if (!splitline(ls, le, 4, f)) return;
    if (f[0].n == 0 || f[1].n == 0) return;
    i64 ts; if (!parse_ts(f[3].p, f[3].n, ts) || ts < CWS || ts >= WE) return;
    u32 c = campT.get(hid(f[2].p, f[2].n)); if (c == EMPTY) return;
    ClRec r; r.uh = hid(f[1].p, f[1].n); r.ts = ts; r.cid = packs(f[0].p, f[0].n); r.camp = (u16)c;
    w.clk.push_back(r);
}
static inline void handle_link(Worker& w, const char* ls, const char* le) {
    Str f[2];
    if (!splitline(ls, le, 2, f)) return;
    if (f[0].n == 0 || f[1].n == 0) return;
    w.lk.push_back(LinkRec{hid(f[0].p, f[0].n), hid(f[1].p, f[1].n), packs(f[0].p, f[0].n), packs(f[1].p, f[1].n)});
}

// ---------------------------------------------------------------- links (union-find)
static bool strless(u64 a, u64 b) {
    size_t la = plen(a), lb = plen(b); int c = memcmp(pptr(a), pptr(b), la < lb ? la : lb);
    return c < 0 || (c == 0 && la < lb);
}
static void build_links() {
    size_t total = 0; for (auto& w : W) total += w.lk.size();
    std::vector<u64> ihash, istr; std::vector<u32> par;
    ihash.reserve(total + 16); istr.reserve(total + 16);
    HT t; t.init(total + 16);
    size_t cap = t.s.size();
    auto grow = [&]() {
        HT n2; n2.s.assign(cap * 2, Slot{0, EMPTY, 0}); n2.mask = cap * 2 - 1;
        for (auto& x : t.s) if (x.v != EMPTY) n2.put(x.k, x.v);
        t = std::move(n2); cap *= 2;
    };
    auto intern = [&](u64 h, u64 str) -> u32 {
        size_t i = h & t.mask;
        for (;;) {
            Slot& x = t.s[i];
            if (x.v == EMPTY) {
                u32 id = (u32)ihash.size(); x.k = h; x.v = id; ihash.push_back(h); istr.push_back(str);
                if (ihash.size() * 2 > cap) grow();
                return id;
            }
            if (x.k == h) return x.v;
            i = (i + 1) & t.mask;
        }
    };
    std::vector<std::pair<u32, u32>> edges; edges.reserve(total);
    for (auto& w : W) for (auto& l : w.lk) { u32 a = intern(l.ha, l.pa), b = intern(l.hb, l.pb); edges.push_back({a, b}); }
    size_t n = ihash.size();
    par.resize(n); for (size_t i = 0; i < n; i++) par[i] = (u32)i;
    auto find = [&](u32 x) { while (par[x] != x) { par[x] = par[par[x]]; x = par[x]; } return x; };
    for (auto& e : edges) { u32 a = find(e.first), b = find(e.second); if (a != b) { if (a < b) par[b] = a; else par[a] = b; } }
    std::vector<u32> mn(n, EMPTY);
    for (size_t i = 0; i < n; i++) { u32 r = find((u32)i); if (mn[r] == EMPTY || strless(istr[i], istr[mn[r]])) mn[r] = (u32)i; }
    linkPk.resize(n); linkNm.resize(n);
    for (size_t i = 0; i < n; i++) { u32 m = mn[find((u32)i)]; linkPk[i] = ihash[m]; linkNm[i] = istr[m]; }
    linkT = std::move(t);
    for (auto& w : W) { std::vector<LinkRec>().swap(w.lk); }
}

// ---------------------------------------------------------------- parquet writer
struct TW {
    std::string b; int last = 0; std::vector<int> stk;
    void varint(u64 v) { while (v >= 0x80) { b.push_back((char)(v | 0x80)); v >>= 7; } b.push_back((char)v); }
    static u64 zz(i64 v) { return ((u64)v << 1) ^ (u64)(v >> 63); }
    void hdr(int id, int type) { int d = id - last; if (d > 0 && d <= 15) b.push_back((char)((d << 4) | type)); else { b.push_back((char)type); varint(zz(id)); } last = id; }
    void i32(int id, i64 v) { hdr(id, 5); varint(zz(v)); }
    void i64f(int id, i64 v) { hdr(id, 6); varint(zz(v)); }
    void str(int id, const std::string& s) { hdr(id, 8); varint(s.size()); b += s; }
    void sbegin(int id) { hdr(id, 12); stk.push_back(last); last = 0; }
    void ebegin() { stk.push_back(last); last = 0; }
    void send() { b.push_back(0); last = stk.back(); stk.pop_back(); }
    void lbegin(int id, int et, size_t n) { hdr(id, 9); if (n < 15) b.push_back((char)((n << 4) | et)); else { b.push_back((char)(0xF0 | et)); varint(n); } }
};
struct ColDesc { const char* name; int type; int conv; bool opt; int logical; };
static void put32(std::string& b, u32 v) { b.append((const char*)&v, 4); }
static void put64(std::string& b, u64 v) { b.append((const char*)&v, 8); }

struct PqFile {
    FILE* f = nullptr; std::vector<ColDesc> cols; i64 off = 0, nrows = 0;
    struct CC { i64 off, size, nvals; };
    struct RGm { std::vector<CC> cc; i64 rows, bytes; };
    std::vector<RGm> rgs;
    bool open(const std::string& path, std::vector<ColDesc> c) {
        f = fopen(path.c_str(), "wb"); if (!f) return false; cols = std::move(c);
        static char* buf = nullptr; (void)buf;
        setvbuf(f, nullptr, _IOFBF, 1 << 20);
        fwrite("PAR1", 1, 4, f); off = 4; return true;
    }
    void rowgroup(const std::vector<std::string>& bodies, i64 rows) {
        RGm g; g.rows = rows; g.bytes = 0;
        for (size_t i = 0; i < cols.size(); i++) {
            TW h; h.i32(1, 0); h.i32(2, (i64)bodies[i].size()); h.i32(3, (i64)bodies[i].size());
            h.sbegin(5); h.i32(1, rows); h.i32(2, 0); h.i32(3, 3); h.i32(4, 3); h.send();
            h.b.push_back(0);
            fwrite(h.b.data(), 1, h.b.size(), f); fwrite(bodies[i].data(), 1, bodies[i].size(), f);
            i64 sz = (i64)(h.b.size() + bodies[i].size());
            g.cc.push_back(CC{off, sz, rows}); off += sz; g.bytes += sz;
        }
        rgs.push_back(std::move(g)); nrows += rows;
    }
    void close() {
        TW m; m.i32(1, 1);
        m.lbegin(2, 12, cols.size() + 1);
        m.ebegin(); m.str(4, "schema"); m.i32(5, (i64)cols.size()); m.send();
        for (auto& c : cols) {
            m.ebegin(); m.i32(1, c.type); m.i32(3, c.opt ? 1 : 0); m.str(4, c.name);
            if (c.conv >= 0) m.i32(6, c.conv);
            if (c.logical) { m.sbegin(10); m.sbegin(c.logical); m.send(); m.send(); }
            m.send();
        }
        m.i64f(3, nrows);
        m.lbegin(4, 12, rgs.size());
        for (auto& g : rgs) {
            m.ebegin();
            m.lbegin(1, 12, cols.size());
            for (size_t i = 0; i < cols.size(); i++) {
                m.ebegin();
                m.i64f(2, g.cc[i].off);
                m.sbegin(3);
                m.i32(1, cols[i].type);
                m.lbegin(2, 5, 2); m.varint(TW::zz(0)); m.varint(TW::zz(3));
                m.lbegin(3, 8, 1); m.varint(strlen(cols[i].name)); m.b += cols[i].name;
                m.i32(4, 0);
                m.i64f(5, g.cc[i].nvals); m.i64f(6, g.cc[i].size); m.i64f(7, g.cc[i].size); m.i64f(9, g.cc[i].off);
                m.send();
                m.send();
            }
            m.i64f(2, g.bytes); m.i64f(3, g.rows);
            m.send();
        }
        m.str(6, "shopping-sessions-cpp");
        m.b.push_back(0);
        fwrite(m.b.data(), 1, m.b.size(), f);
        u32 len = (u32)m.b.size(); fwrite(&len, 4, 1, f); fwrite("PAR1", 1, 4, f);
        fclose(f); f = nullptr;
    }
};
static const ColDesc C_STR = {"", 6, 0, false, 1};
static ColDesc cd(const char* n, int type, int conv, bool opt, int logical) { return ColDesc{n, type, conv, opt, logical}; }
static const size_t RG_ROWS = 1000000;

static void write_user_file(const std::string& path, const std::vector<URow>& rows) {
    PqFile pf;
    if (!pf.open(path, {cd("user_id", 6, 0, false, 1), cd("day", 1, 6, false, 6), cd("sessions", 2, -1, false, 0), cd("events", 2, -1, false, 0),
                        cd("revenue_cents", 2, -1, false, 0), cd("top_category", 6, 0, true, 1)})) { fprintf(stderr, "cannot write %s\n", path.c_str()); exit(2); }
    for (size_t a = 0; a < rows.size(); a += RG_ROWS) {
        size_t b = std::min(rows.size(), a + RG_ROWS), n = b - a;
        std::vector<std::string> bodies(6);
        bodies[0].reserve(n * 16); for (size_t i = a; i < b; i++) { const URow& r = rows[i]; put32(bodies[0], (u32)plen(r.nm)); bodies[0].append(pptr(r.nm), plen(r.nm)); }
        for (size_t i = a; i < b; i++) put32(bodies[1], rows[i].day);
        for (size_t i = a; i < b; i++) put64(bodies[2], rows[i].sessions);
        for (size_t i = a; i < b; i++) put64(bodies[3], rows[i].events);
        for (size_t i = a; i < b; i++) put64(bodies[4], (u64)rows[i].rev);
        std::string lv; size_t groups = (n + 7) / 8; std::string bits(groups, '\0'); std::string vals;
        for (size_t i = a; i < b; i++) if (rows[i].top >= 0) {
            size_t k = i - a; bits[k >> 3] |= (char)(1u << (k & 7));
            const std::string& nm = catNames[rows[i].top]; put32(vals, (u32)nm.size()); vals += nm;
        }
        TW hv; hv.varint((groups << 1) | 1);
        put32(bodies[5], (u32)(hv.b.size() + bits.size())); bodies[5] += hv.b; bodies[5] += bits; bodies[5] += vals;
        pf.rowgroup(bodies, (i64)n);
    }
    pf.close();
}

static bool TIMING = false; static double T0 = 0;
static double nowsec() { timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec + t.tv_nsec * 1e-9; }
static void phase(const char* n) { if (TIMING) { double t = nowsec(); fprintf(stderr, "[%7.3fs] %s\n", t - T0, n); } }

// ---------------------------------------------------------------- main
int main(int argc, char** argv) {
    const char* e; T0 = nowsec(); TIMING = getenv("TIMING") != nullptr;
    std::string data = (e = getenv("DATA_DIR")) ? e : (argc > 2 ? argv[1] : "/data");
    std::string out = (e = getenv("OUTPUT_DIR")) ? e : (argc > 2 ? argv[2] : "/output");
    NT = (e = getenv("NCPU")) ? atoi(e) : (int)std::thread::hardware_concurrency();
    if (NT < 1) NT = 1;
    if (!read_manifest(data + "/manifest.json")) { fprintf(stderr, "bad manifest\n"); return 2; }
    CWS = WS - WEEKMS;
    DAY0 = fdiv(WS, DAYMS); ND = (int)(fdiv(WE - 1, DAYMS) - DAY0 + 1); if (ND < 1) ND = 1;

    // products
    {
        MFile m = mapfile(data + "/products.csv");
        std::vector<std::pair<u64, std::string>> pr; std::vector<std::string> cats;
        std::vector<std::pair<std::string, int>> cmap;
        if (m.d) for_lines(m.d, m.n, 0, m.n, [&](const char* ls, const char* le) {
            Str f[2]; if (!splitline(ls, le, 2, f) || f[0].n == 0) return;
            std::string c(f[1].p, f[1].n); pr.push_back({hid(f[0].p, f[0].n), c}); });
        std::vector<std::string> names; for (auto& x : pr) names.push_back(x.second);
        std::sort(names.begin(), names.end()); names.erase(std::unique(names.begin(), names.end()), names.end());
        catNames = names;
        std::string unk = "unknown"; while (std::binary_search(names.begin(), names.end(), unk)) unk += "_";
        catNames.push_back(unk); UNKCAT = (int)catNames.size() - 1;
        prodT.init(pr.size() + 1);
        for (auto& x : pr) prodT.put(x.first, (u32)(std::lower_bound(names.begin(), names.end(), x.second) - names.begin()));
        catRank.assign(catNames.size(), 0);
        std::vector<int> ord(catNames.size()); for (size_t i = 0; i < ord.size(); i++) ord[i] = (int)i;
        std::sort(ord.begin(), ord.end(), [&](int a, int b) { return catNames[a] < catNames[b]; });
        for (size_t i = 0; i < ord.size(); i++) catRank[ord[i]] = (int)i;
    }
    // campaigns
    {
        MFile m = mapfile(data + "/campaigns.csv");
        if (m.d) for_lines(m.d, m.n, 0, m.n, [&](const char* ls, const char* le) {
            Str f[2]; if (!splitline(ls, le, 2, f) || f[0].n == 0) return;
            campIds.push_back(std::string(f[0].p, f[0].n)); campChan.push_back(std::string(f[1].p, f[1].n)); });
        campT.init(campIds.size() + 1);
        for (size_t i = 0; i < campIds.size(); i++) campT.put(hid(campIds[i].data(), campIds[i].size()), (u32)i);
    }
    const int NC = (int)campIds.size();  // organic == NC

    // tasks
    std::vector<Task> linkTasks, mainTasks;
    size_t evBytes = 0;
    size_t CH = 4u << 20;
    if ((e = getenv("CHUNK_BYTES"))) CH = (size_t)atoll(e);  // test hook
    auto add = [&](std::vector<Task>& v, int kind, const std::string& path) {
        MFile m = mapfile(path); if (!m.d) return;
        if (kind == 0) evBytes += m.n;
        for (size_t s = 0; s < m.n; s += CH) v.push_back(Task{kind, m.d, m.n, s, std::min(m.n, s + CH)});
    };
    for (auto& p : list_csv(data + "/links")) add(linkTasks, 2, p);
    for (auto& p : list_csv(data + "/events")) add(mainTasks, 0, p);
    for (auto& p : list_csv(data + "/clicks")) add(mainTasks, 1, p);

    phase("setup+mmap");
    W = std::vector<Worker>(NT);
    size_t estEv = evBytes / 70 + 1000;
    size_t perBucket = (size_t)((double)estEv * 1.15 / ((double)NT * D)) + 16;
    for (auto& w : W) for (int d = 0; d < D; d++) w.evb[d].reserve(perBucket);

    // links first (small), then events+clicks in parallel with union-find
    run_parallel(NT, linkTasks.size(), [&](int tid, size_t t) {
        const Task& k = linkTasks[t]; Worker& w = W[tid];
        for_lines(k.base, k.size, k.s, k.e, [&](const char* a, const char* b) { handle_link(w, a, b); });
    });
    phase("links parsed");
    std::thread ufth(build_links);
    run_parallel(NT, mainTasks.size(), [&](int tid, size_t t) {
        const Task& k = mainTasks[t]; Worker& w = W[tid];
        if (k.kind == 0) for_lines(k.base, k.size, k.s, k.e, [&](const char* a, const char* b) { handle_event(w, a, b); });
        else for_lines(k.base, k.size, k.s, k.e, [&](const char* a, const char* b) { handle_click(w, a, b); });
    });
    ufth.join();
    phase("events+clicks parsed, links built");

    // dedup events + resolve persons; resolve clicks
    size_t estSv = (size_t)((double)estEv * 1.1 / ((double)NT * P)) + 16;
    for (auto& w : W) { for (int p = 0; p < P; p++) { w.evp[p].reserve(estSv); } }
    const size_t CSLICE = 1 << 16;
    std::vector<std::pair<int, size_t>> clickSlices;
    for (int t = 0; t < NT; t++) for (size_t s = 0; s < W[t].clk.size(); s += CSLICE) clickSlices.push_back({t, s});
    auto resolve = [&](u64 uh, u64 nm0, u64& pkey, u64& nm) {
        u32 li = linkT.s.empty() ? EMPTY : linkT.get(uh);
        if (li == EMPTY) { pkey = uh; nm = nm0; } else { pkey = linkPk[li]; nm = linkNm[li]; }
    };
    run_parallel(NT, (size_t)D + clickSlices.size(), [&](int tid, size_t task) {
        Worker& w = W[tid];
        if (task < (size_t)D) {
            int d = (int)task; size_t n = 0;
            for (int t = 0; t < NT; t++) n += W[t].evb[d].size();
            if (!n) return;
            size_t cap = 16; while (cap < 2 * n) cap <<= 1;
            w.tab.assign(cap, Worker::DS{0, 0, 0}); w.cand.clear(); w.cand.reserve(n);
            size_t mask = cap - 1;
            for (int t = 0; t < NT; t++) for (const EvRec& r : W[t].evb[d]) {
                size_t pos = r.h1 & mask;
                for (;;) {
                    Worker::DS& s = w.tab[pos];
                    if (s.ref == 0) { w.cand.push_back(&r); s.h1 = r.h1; s.h2 = r.h2; s.ref = (u32)w.cand.size(); break; }
                    if (s.h1 == r.h1 && s.h2 == r.h2) { const EvRec*& c = w.cand[s.ref - 1]; if (r.ingest < c->ingest) c = &r; break; }
                    pos = (pos + 1) & mask;
                }
            }
            for (const EvRec* c : w.cand) if (c->ok) {
                u64 pkey, nm; resolve(c->uh, c->nm, pkey, nm);
                w.evp[pkey >> (64 - PB)].push_back(SvRec{pkey, nm, c->ts, c->price, c->cat, c->type, 0});
            }
            for (int t = 0; t < NT; t++) std::vector<EvRec>().swap(W[t].evb[d]);
        } else {
            auto& sl = clickSlices[task - D]; auto& src = W[sl.first].clk;
            size_t b = std::min(src.size(), sl.second + CSLICE);
            for (size_t i = sl.second; i < b; i++) {
                ClRec r = src[i]; u64 pkey, nm; resolve(r.uh, 0, pkey, nm); r.uh = pkey;
                w.clp[pkey >> (64 - PB)].push_back(r);
            }
        }
    });
    for (auto& w : W) std::vector<ClRec>().swap(w.clk);
    phase("dedup + person resolve");

    // per-person work
    size_t NS = (size_t)(NC + 1) * ND;
    for (auto& w : W) {
        w.st_sess.assign(NS, 0); w.st_purch.assign(NS, 0); w.st_assist.assign(NS, 0); w.st_rev.assign(NS, 0);
        w.cnt.assign(NC + 1, 0); w.apos.assign(NC + 1, 0); w.catrev.assign(catNames.size(), 0);
    }
    run_parallel(NT, (size_t)P, [&](int tid, size_t p) {
        Worker& w = W[tid];
        std::vector<SvRec>& ev = w.ev; std::vector<ClRec>& cl = w.cl; ev.clear(); cl.clear();
        for (int t = 0; t < NT; t++) {
            auto& a = W[t].evp[p]; ev.insert(ev.end(), a.begin(), a.end()); std::vector<SvRec>().swap(a);
            auto& c = W[t].clp[p]; cl.insert(cl.end(), c.begin(), c.end()); std::vector<ClRec>().swap(c);
        }
        if (ev.empty()) return;
        {
            // Bucket by the pkey bits just below the partition bits (counting sort, one scatter pass), then sort each
            // small bucket by (pkey, ts). A person's events share a pkey, so they land in one bucket; ~30 elements per
            // std::sort instead of ~10^4 per partition. Same final order as one global (pkey, ts) sort.
            const size_t n = ev.size();
            int bits = 4; while (bits < 16 && ((size_t)1 << (bits + 3)) < n) bits++;
            const size_t nb = (size_t)1 << bits;
            const int sh = 64 - PB - bits;
            w.hist.assign(nb + 1, 0);
            for (size_t x = 0; x < n; x++) w.hist[((ev[x].pkey >> sh) & (nb - 1)) + 1]++;
            for (size_t b = 0; b < nb; b++) w.hist[b + 1] += w.hist[b];
            w.ev2.resize(n);
            { std::vector<u32>& pos = w.apos2; pos.assign(w.hist.begin(), w.hist.end() - 1);
              for (size_t x = 0; x < n; x++) w.ev2[pos[(ev[x].pkey >> sh) & (nb - 1)]++] = ev[x]; }
            ev.swap(w.ev2);
            auto cmp = [](const SvRec& a, const SvRec& b) { return a.pkey != b.pkey ? a.pkey < b.pkey : a.ts < b.ts; };
            for (size_t b = 0; b < nb; b++) {
                size_t lo = w.hist[b], hi = w.hist[b + 1];
                if (hi - lo > 1) std::sort(ev.begin() + lo, ev.begin() + hi, cmp);
            }
        }
        std::sort(cl.begin(), cl.end(), [](const ClRec& a, const ClRec& b) {
            if (a.uh != b.uh) return a.uh < b.uh;
            if (a.ts != b.ts) return a.ts < b.ts;
            size_t la = plen(a.cid), lb = plen(b.cid); int c = memcmp(pptr(a.cid), pptr(b.cid), la < lb ? la : lb);
            return c < 0 || (c == 0 && la < lb);
        });
        { // drop duplicate click ids (adjacent after sort)
            size_t o = 0;
            for (size_t i = 0; i < cl.size(); i++) {
                if (o > 0 && cl[i].uh == cl[o - 1].uh && cl[i].ts == cl[o - 1].ts && plen(cl[i].cid) == plen(cl[o - 1].cid) &&
                    !memcmp(pptr(cl[i].cid), pptr(cl[o - 1].cid), plen(cl[i].cid))) continue;
                cl[o++] = cl[i];
            }
            cl.resize(o);
        }
        const size_t en = ev.size(), cn = cl.size();
        size_t i = 0, ci = 0;
        while (i < en) {
            u64 pk = ev[i].pkey; size_t j = i; while (j < en && ev[j].pkey == pk) j++;
            while (ci < cn && cl[ci].uh < pk) ci++;
            size_t cs = ci, cj = ci; while (cj < cn && cl[cj].uh == pk) cj++;
            size_t lo = cs, hi = cs;
            u64 nm = ev[i].nm;
            i64 curday = -1; u32 rs = 0, re = 0; i64 rrev = 0;
            auto flush = [&]() {
                if (curday < 0) return;
                int top = -1; i64 best = 0;
                for (int c : w.touched) {
                    i64 v = w.catrev[c];
                    if (v > best || (v == best && top >= 0 && catRank[c] < catRank[top])) { best = v; top = c; }
                    w.catrev[c] = 0;
                }
                w.touched.clear();
                w.rows.push_back(URow{nm, rrev, (u32)(DAY0 + curday), rs, re, top});
                curday = -1;
            };
            size_t k = i;
            while (k < j) {
                i64 start = ev[k].ts, prev = start; size_t m = k; i64 rev = 0;
                i64 dayidx = fdiv(start, DAYMS) - DAY0;
                if (dayidx != curday) { flush(); curday = dayidx; rs = 0; re = 0; rrev = 0; }
                while (m < j && ev[m].ts - prev <= GAPMS) {
                    prev = ev[m].ts;
                    if (ev[m].type == 2) {
                        rev += ev[m].price;
                        if (w.catrev[ev[m].cat] == 0) w.touched.push_back(ev[m].cat);
                        w.catrev[ev[m].cat] += ev[m].price;
                    }
                    m++;
                }
                // credit
                int camp = NC;
                if (cj > cs) {
                    while (hi < cj && cl[hi].ts <= start) {
                        u16 c = cl[hi].camp; if (w.cnt[c]++ == 0) { w.apos[c] = (u32)w.active.size(); w.active.push_back(c); } hi++;
                    }
                    while (lo < hi && cl[lo].ts < start - WEEKMS) {
                        u16 c = cl[lo].camp;
                        if (--w.cnt[c] == 0) { u32 last = w.active.back(); w.active[w.apos[c]] = last; w.apos[last] = w.apos[c]; w.active.pop_back(); }
                        lo++;
                    }
                    if (hi > lo) {
                        size_t x = hi - 1; i64 T = cl[x].ts; while (x > lo && cl[x - 1].ts == T) x--;
                        camp = cl[x].camp;
                        for (u32 c : w.active) w.st_assist[(size_t)c * ND + dayidx]++;
                    }
                }
                size_t si = (size_t)camp * ND + dayidx;
                w.st_sess[si]++; if (rev > 0) w.st_purch[si]++; w.st_rev[si] += rev;
                rs++; re += (u32)(m - k); rrev += rev;
                k = m;
            }
            flush();
            for (size_t x = lo; x < hi; x++) w.cnt[cl[x].camp] = 0;
            w.active.clear();
            ci = cj; i = j;
        }
    });

    phase("sessionize + aggregate");
    // outputs
    std::string ud = out + "/user_daily", cdp = out + "/campaign_daily";
    mkdir(out.c_str(), 0777); mkdir(ud.c_str(), 0777); mkdir(cdp.c_str(), 0777);
    run_parallel(NT, (size_t)NT, [&](int tid, size_t t) {
        (void)tid;
        if (W[t].rows.empty() && t != 0) return;
        char nm[64]; snprintf(nm, sizeof nm, "/part-%zu.parquet", t);
        write_user_file(ud + nm, W[t].rows);
    });
    {
        std::vector<u64> sess(NS, 0), purch(NS, 0), asst(NS, 0); std::vector<i64> rev(NS, 0);
        for (auto& w : W) for (size_t i = 0; i < NS; i++) { sess[i] += w.st_sess[i]; purch[i] += w.st_purch[i]; asst[i] += w.st_assist[i]; rev[i] += w.st_rev[i]; }
        struct CR { int c; int d; };
        std::vector<CR> rows;
        for (int c = 0; c <= NC; c++) for (int d = 0; d < ND; d++) { size_t i = (size_t)c * ND + d; if (sess[i] || asst[i]) rows.push_back({c, d}); }
        PqFile pf;
        if (!pf.open(cdp + "/part-0.parquet", {cd("campaign_id", 6, 0, false, 1), cd("channel", 6, 0, false, 1), cd("day", 1, 6, false, 6),
                cd("sessions", 2, -1, false, 0), cd("purchasing_sessions", 2, -1, false, 0), cd("revenue_cents", 2, -1, false, 0), cd("assisted_sessions", 2, -1, false, 0)})) return 2;
        if (!rows.empty()) {
            size_t n = rows.size(); std::vector<std::string> b(7);
            for (auto& r : rows) {
                const std::string& id = r.c == NC ? std::string("organic") : campIds[r.c];
                const std::string& ch = r.c == NC ? std::string("organic") : campChan[r.c];
                size_t i = (size_t)r.c * ND + r.d;
                put32(b[0], (u32)id.size()); b[0] += id; put32(b[1], (u32)ch.size()); b[1] += ch;
                put32(b[2], (u32)(DAY0 + r.d)); put64(b[3], sess[i]); put64(b[4], purch[i]); put64(b[5], (u64)rev[i]); put64(b[6], asst[i]);
            }
            pf.rowgroup(b, (i64)n);
        }
        pf.close();
    }
    phase("written");
    return 0;
}
