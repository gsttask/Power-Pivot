# ==============================================================
# Sales Cube Dashboard — FastAPI + DuckDB  v4.0
# Multi-user session support, read-only shared DuckDB connection
# ==============================================================
import os, io, socket, platform, glob, uuid, time
import duckdb, pandas as pd
from fastapi import FastAPI, HTTPException, Cookie, Response, UploadFile, File
from fastapi.responses import FileResponse, StreamingResponse
import shutil
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional
import uvicorn
import json
from passlib.context import CryptContext

# Password hashing setup
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
USER_DB_FILE = "users.json"

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

import asyncio
from contextlib import asynccontextmanager
import shutil

async def auto_delete_uploads():
    while True:
        await asyncio.sleep(2 * 3600)  # 2 Ghante (2 * 3600 seconds) wait karega
        upload_folder = os.path.abspath("./_Uploads")
        if os.path.exists(upload_folder):
            try:
                for filename in os.listdir(upload_folder):
                    file_path = os.path.join(upload_folder, filename)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                print("🧹 Auto-Cleanup: 2 Ghante ho gaye, _Uploads folder ka purana data delete ho gaya!")
                
                SHARED['loaded_files'].clear()
                SHARED['excel_dfs'].clear()
                SHARED['vlookup_df'] = None
                
            except Exception as e:
                print(f"⚠️ Auto-Cleanup Error: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(auto_delete_uploads())
    yield 
    task.cancel()
    upload_folder = os.path.abspath("./_Uploads")
    if os.path.exists(upload_folder):
        try:
            shutil.rmtree(upload_folder)
            os.makedirs(upload_folder)
        except:
            pass

# Naya FastAPI instance jisme lifespan add kiya gaya hai
app = FastAPI(title="Sales Cube", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
                   allow_headers=["*"], expose_headers=["Set-Cookie"])

# ══════════════════════════════════════════════════════════════
#  SHARED STATE — one DuckDB connection, read-only, all users share
# ══════════════════════════════════════════════════════════════
SHARED = {
    "con":          None,   # single DuckDB connection (read-only queries)
    "columns":      [],
    "col_types":    {},
    "row_count":    0,
    "loaded_files": set(),
    "current_folder": None,
    "excel_dfs":    [],     # (name, df) for excel files
    "vlookup_df":   None,
}
# --- User Database Initializer ---
if not os.path.exists(USER_DB_FILE):
    with open(USER_DB_FILE, "w") as f:
        json.dump({
            "admin": {"password": pwd_context.hash("admin123"), "role": "admin", "active": True}
        }, f)

def load_users():
    with open(USER_DB_FILE, "r") as f: return json.load(f)

def save_users(users):
    with open(USER_DB_FILE, "w") as f: json.dump(users, f)

from datetime import datetime
import os

LOG_DB_FILE = "logs.json"

def add_log(username, action):
    logs = []
    if os.path.exists(LOG_DB_FILE):
        try:
            with open(LOG_DB_FILE, "r") as f: 
                logs = json.load(f)
        except: 
            pass
    
    logs.insert(0, {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user": username,
        "action": action
    })
    
    # Sirf last 100 logs save rahenge taaki file badi na ho
    with open(LOG_DB_FILE, "w") as f: 
        json.dump(logs[:100], f, indent=4)

# ══════════════════════════════════════════════════════════════
#  PER-USER SESSIONS — filters, pivots, selections isolated
# ══════════════════════════════════════════════════════════════
SESSIONS: Dict[str, dict] = {}
SESSION_TTL = 4 * 3600  # 4 hours idle timeout

def get_session(session_id: Optional[str]) -> tuple:
    """Return (session_id, session_dict). Creates new if needed."""
    now = time.time()
    # Cleanup expired sessions
    expired = [k for k, v in SESSIONS.items() if now - v['last'] > SESSION_TTL]
    for k in expired:
        del SESSIONS[k]

    if session_id and session_id in SESSIONS:
        SESSIONS[session_id]['last'] = now
        return session_id, SESSIONS[session_id]

    # New session
    sid = str(uuid.uuid4())
    SESSIONS[sid] = {
        'last':           now,
        'active_filters': {},
        'val_filters':    {},
        'date_formats':   {},
    }
    return sid, SESSIONS[sid]

def set_session_cookie(response: Response, sid: str):
    response.set_cookie("scube_sid", sid, max_age=SESSION_TTL,
                        samesite="lax", httponly=False)

# ══════════════════════════════════════════════════════════════
#  MODELS
# ══════════════════════════════════════════════════════════════
class LoadReq(BaseModel):
    file_path: str = ""
    file_paths: List[str] = []

class LoadFolderReq(BaseModel):
    folder_path: str

class ScanReq(BaseModel):
    folder_path: str

class FilterValsReq(BaseModel):
    column: str
    search: str = ""

class PivotReq(BaseModel):
    rows: List[str] = []
    cols: List[str] = []
    vals: List[str] = []
    filters: Dict[str, List[str]] = {}
    val_filters: Dict[str, Dict] = {}
    date_formats: Dict[str, str] = {}

class ConvertReq(BaseModel):
    files: List[str]
    sheets: List[str] = []
    columns: List[str] = []
    output_path: str
    merge_all: bool = False

class ConvertDownloadReq(BaseModel):
    file: str
    sheets: List[str] = []
    columns: List[str] = []
    filename: str = "converted"

class VlookupReq(BaseModel):
    lookup_file: str
    main_key: str
    lookup_key: str
    add_columns: List[str]

class LoginReq(BaseModel):
    username: str
    password: str

class UserManageReq(BaseModel):
    username: str
    active: bool

class CreateUserReq(BaseModel):
    username: str
    password: str
    role: str = "user"

class DeleteUserReq(BaseModel):
    username: str

# ══════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════
def norm(p):
    return p.strip().strip('"').strip("'").replace("\\", "/")

def is_parquet(p):
    return os.path.splitext(p)[1].lower() in ('.parquet', '.pq')

def qc(c): return f'"{c}"'

def build_where(filters, val_filters):
    parts = []
    for col, vals in filters.items():
        if vals:
            safe   = [str(v).replace("'", "''") for v in vals]
            quoted = ", ".join(f"'{v}'" for v in safe)
            # NAYI LINE: Yahan TRIM() laga diya gaya hai
            parts.append(f'TRIM(CAST({qc(col)} AS VARCHAR)) IN ({quoted})')
    for col, vf in val_filters.items():
        parts.append(f'TRY_CAST({qc(col)} AS DOUBLE) {vf["op"]} {vf["val"]}')
    return ("WHERE " + " AND ".join(parts)) if parts else ""

def read_excel_robust(path, sheet_name=0):
    ext = os.path.splitext(path)[1].lower()
    engine = 'pyxlsb' if ext == '.xlsb' else ('calamine' if ext in ('.xlsx', '.xls') else 'openpyxl')
    
    try:
        # Pehle bina header ke data read karo
        df_temp = pd.read_excel(path, sheet_name=sheet_name, engine=engine, header=None)
        
        # Blank rows hata do
        df_temp = df_temp.dropna(how='all').reset_index(drop=True)
        if df_temp.empty:
            return pd.DataFrame()

        # Pehli 10 rows check karke true header detect karo
        max_cols = df_temp.shape[1]
        header_idx = 0
        for i in range(min(10, len(df_temp))):
            non_null_count = df_temp.iloc[i].notna().sum()
            if non_null_count >= (max_cols * 0.5): 
                header_idx = i
                break
                
        # Sahi header index ke sath file wapas read karo
        df = pd.read_excel(path, sheet_name=sheet_name, engine=engine, header=header_idx)
        return df
        
    except Exception as e:
        # Agar error aaye to normal purane tarike se read karo
        return pd.read_excel(path, sheet_name=sheet_name)

def read_excel_df(path):
    # Backend ke doosre parts ke liye bridge
    return read_excel_robust(path, sheet_name=0)

def standardize_df(df):
    df.columns = [str(c).strip().upper().replace(' ', '_') for c in df.columns]
    return df

def _write_parquet(df, path):
    if HAS_POLARS:    pl.from_pandas(df).write_parquet(path, compression='snappy')
    elif HAS_PYARROW: pq.write_table(pa.Table.from_pandas(df), path, compression='snappy')
    else:             df.to_parquet(path, index=False)

def _rebuild_shared():
    """Rebuild shared DuckDB connection from SHARED state."""
    con = duckdb.connect()  # in-memory, will reference files by path

    if SHARED['vlookup_df'] is not None:
        con.register('sales_data', SHARED['vlookup_df'])
    else:
        parquet_files = sorted([f for f in SHARED['loaded_files'] if is_parquet(f)])
        parts = []

        if parquet_files:
            escaped = [f.replace("'", "''") for f in parquet_files]
            pq_list = ", ".join(f"'{f}'" for f in escaped)
            if len(parquet_files) == 1:
                parts.append(f"SELECT * FROM read_parquet('{escaped[0]}')")
            else:
                parts.append(f"SELECT * FROM read_parquet([{pq_list}], union_by_name=true)")

        for name, df in SHARED['excel_dfs']:
            con.register(name, df)
            parts.append(f"SELECT * FROM {name}")

        if not parts:
            raise ValueError("No data loaded")

        view_sql = " UNION ALL ".join(parts)
        con.execute(f"CREATE VIEW sales_data AS {view_sql}")

    desc      = con.execute("DESCRIBE sales_data").fetchdf()
    cols      = desc['column_name'].tolist()
    ctypes    = dict(zip(desc['column_name'], desc['column_type']))
    row_count = con.execute("SELECT COUNT(*) FROM sales_data").fetchone()[0]

    SHARED['con']       = con
    SHARED['columns']   = cols
    SHARED['col_types'] = ctypes
    SHARED['row_count'] = row_count

def _shared_response(loaded=None, errors=None):
    r = {
        "status":    "ok",
        "columns":   SHARED['columns'],
        "col_types": SHARED['col_types'],
        "row_count": SHARED['row_count'],
    }
    if loaded is not None: r["loaded_files"] = loaded
    if errors:             r["errors"]       = errors
    return r

# ══════════════════════════════════════════════════════════════
#  LOAD FILE
# ══════════════════════════════════════════════════════════════
@app.post("/api/load")
async def load_file(req: LoadReq, response: Response,
                    scube_sid: Optional[str] = Cookie(None)):
    sid, _ = get_session(scube_sid)
    set_session_cookie(response, sid)

    valid_paths = []
    for p in req.file_paths:
        p_norm = norm(p)
        if os.path.exists(p_norm):
            valid_paths.append(p_norm)

    if not valid_paths:
        raise HTTPException(400, "No valid files found on server.")

    try:
        SHARED['vlookup_df']   = None
        SHARED['excel_dfs']    = []
        SHARED['loaded_files'] = set()
        SHARED['current_folder'] = None

        # Saari files ko list mein se ek-ek karke backend read karega
        for i, path in enumerate(valid_paths):
            if not is_parquet(path):
                df = standardize_df(read_excel_df(path))
                SHARED['excel_dfs'].append((f'excel_{i}', df))
            SHARED['loaded_files'].add(path)

        _rebuild_shared()
        return _shared_response([os.path.basename(p) for p in valid_paths])
    except Exception as e:
        raise HTTPException(400, str(e))

# ══════════════════════════════════════════════════════════════
#  LOAD FOLDER
# ══════════════════════════════════════════════════════════════
@app.post("/api/load-folder")
async def load_folder(req: LoadFolderReq, response: Response,
                      scube_sid: Optional[str] = Cookie(None)):
    sid, _ = get_session(scube_sid)
    set_session_cookie(response, sid)

    folder = norm(req.folder_path)
    if not os.path.isdir(folder):
        raise HTTPException(400, f"Folder not found: {folder}")

    SUPPORTED = ['.parquet', '.pq', '.xlsx', '.xls', '.xlsb']
    files = []
    for ext in SUPPORTED:
        files.extend(glob.glob(os.path.join(folder, f'*{ext}')))
    files = sorted(set(files))
    if not files: raise HTTPException(400, "No supported files in folder")

    SHARED['vlookup_df']     = None
    SHARED['excel_dfs']      = []
    SHARED['loaded_files']   = set()
    SHARED['current_folder'] = folder

    loaded, errors, excel_idx = [], [], 0
    for f in files:
        try:
            if is_parquet(f):
                SHARED['loaded_files'].add(f)
            else:
                df = standardize_df(read_excel_df(f))
                SHARED['excel_dfs'].append((f'excel_{excel_idx}', df))
                SHARED['loaded_files'].add(f)
                excel_idx += 1
            loaded.append(f)
        except Exception as e:
            errors.append(f"{os.path.basename(f)}: {str(e)}")

    if not loaded: raise HTTPException(400, "Failed to load any files")
    _rebuild_shared()
    return _shared_response([os.path.basename(f) for f in loaded], errors)

# ══════════════════════════════════════════════════════════════
#  SCAN NEW FILES
# ══════════════════════════════════════════════════════════════
@app.post("/api/scan-new")
async def scan_new(req: ScanReq):
    folder = norm(req.folder_path)
    if not os.path.isdir(folder): raise HTTPException(400, "Folder not found")

    SUPPORTED = ['.parquet', '.pq', '.xlsx', '.xls', '.xlsb']
    all_files = []
    for ext in SUPPORTED:
        all_files.extend(glob.glob(os.path.join(folder, f'*{ext}')))

    new_files = [f for f in sorted(set(all_files)) if f not in SHARED['loaded_files']]
    if not new_files:
        return {"status": "no_new", "message": "No new files found", "added": 0}

    added, errors, excel_idx = 0, [], len(SHARED['excel_dfs'])
    for f in new_files:
        try:
            if is_parquet(f):
                SHARED['loaded_files'].add(f)
            else:
                df = standardize_df(read_excel_df(f))
                SHARED['excel_dfs'].append((f'excel_{excel_idx}', df))
                SHARED['loaded_files'].add(f)
                excel_idx += 1
            added += 1
        except Exception as e:
            errors.append(f"{os.path.basename(f)}: {str(e)}")

    if added:
        _rebuild_shared()
        return {"status": "ok", "added": added,
                "columns": SHARED['columns'], "col_types": SHARED['col_types'],
                "row_count": SHARED['row_count'], "errors": errors}
    return {"status": "error", "added": 0, "errors": errors}

# ══════════════════════════════════════════════════════════════
#  FILTER VALS
# ══════════════════════════════════════════════════════════════
@app.post("/api/filter-values")
async def filter_values(req: FilterValsReq):
    if not SHARED['con']: raise HTTPException(400, "No file loaded")
    
    # 1. Search term ke aage-peeche ke extra spaces hatayein
    search_term = req.search.strip() if req.search else ""
    
    if search_term:
        # 2. TRIM: Parquet file ke column ki hidden spaces hatayega
        # 3. ILIKE: Case-insensitive search karega (Capital/Small letter ek barabar)
        sch = f"AND TRIM(CAST({qc(req.column)} AS VARCHAR)) ILIKE '%{search_term}%'"
    else:
        sch = ""
        
    # 4. LIMIT ko 2000 se seedha 10,000 kar diya gaya hai (Capping Issue Solved)
    sql = (f"SELECT DISTINCT TRIM(CAST({qc(req.column)} AS VARCHAR)) "
           f"FROM sales_data WHERE {qc(req.column)} IS NOT NULL {sch} ORDER BY 1 LIMIT 10000")
           
    return {"values": [r[0] for r in SHARED['con'].execute(sql).fetchall()]}

# ══════════════════════════════════════════════════════════════
#  PIVOT  — uses request-provided filters (from user's browser state)
# ══════════════════════════════════════════════════════════════
@app.post("/api/pivot")
async def pivot(req: PivotReq):
    if not SHARED['con']: raise HTTPException(400, "No file loaded")

    where    = build_where(req.filters, req.val_filters)
    all_dims = req.rows + req.cols
    col_types = SHARED['col_types']
    select_parts = []
    
    for c in all_dims:
        ctype = col_types.get(c, '').upper()
        if 'DATE' in ctype or 'TIMESTAMP' in ctype:
            select_parts.append(f"strftime({qc(c)}, '%d-%m-%Y') as {qc(c)}")
        else:
            select_parts.append(f'CAST({qc(c)} AS VARCHAR) as {qc(c)}')
            
    for v in req.vals:
        select_parts.append(f'SUM(TRY_CAST({qc(v)} AS DOUBLE)) as {qc(v)}')
        
    if not select_parts: raise HTTPException(400, "Nothing selected")

    grp_parts = []
    for c in all_dims:
        ctype = col_types.get(c, '').upper()
        if 'DATE' in ctype or 'TIMESTAMP' in ctype:
            grp_parts.append(f"strftime({qc(c)}, '%d-%m-%Y')")
        else:
            grp_parts.append(f'CAST({qc(c)} AS VARCHAR)')
            
    group_by = (f"GROUP BY {', '.join(grp_parts)}" if grp_parts else "")
    sql = f"SELECT {', '.join(select_parts)} FROM sales_data {where} {group_by} LIMIT 100000"
    
    df = SHARED['con'].execute(sql).df()

    for col, fmt in req.date_formats.items():
        if col in df.columns:
            try:
                dt = pd.to_datetime(df[col], errors='coerce')
                if fmt == "FY":
                    df[col] = dt.apply(lambda x: f"FY {x.year}-{str(x.year+1)[-2:]}" if pd.notna(x) and x.month > 3 else (f"FY {x.year-1}-{str(x.year)[-2:]}" if pd.notna(x) else ''))
                else:
                    df[col] = dt.dt.strftime(fmt).fillna('')
            except: pass

    for v in req.vals:
        if v in df.columns:
            df[v] = pd.to_numeric(df[v], errors='coerce').fillna(0)

    if req.cols and req.rows and req.vals:
        piv = pd.pivot_table(df, index=req.rows, columns=req.cols, values=req.vals,
                             aggfunc='sum', fill_value=0, margins=True, margins_name='Grand Total')
        piv.columns = [" | ".join(str(x) for x in c if str(x)) if isinstance(c, tuple) else str(c) for c in piv.columns]
        piv = piv.reset_index()
    elif req.rows and req.vals:
        total_row = {}
        for c in df.columns:
            if c in req.vals: total_row[c] = pd.to_numeric(df[c], errors='coerce').sum()
            elif c == req.rows[0]: total_row[c] = 'Grand Total'
            else: total_row[c] = ''
        piv = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
    elif req.vals:
        total_row = {v: pd.to_numeric(df[v], errors='coerce').sum() for v in req.vals if v in df.columns}
        total_row['Summary'] = 'Grand Total'
        piv = pd.DataFrame([total_row])
        piv = piv[['Summary'] + [v for v in req.vals if v in df.columns]]
    else:
        piv = df

    for c in piv.select_dtypes(include=['float', 'float64']).columns:
        piv[c] = piv[c].round(2)
        
    piv = piv.fillna('')
    return {"columns": list(piv.columns), "data": piv.head(10000).values.tolist(), "total_rows": len(piv)}

# ══════════════════════════════════════════════════════════════
#  RAW DATA
# ══════════════════════════════════════════════════════════════
@app.get("/api/raw")
async def raw_data():
    if not SHARED['con']: raise HTTPException(400, "No file loaded")
    df    = SHARED['con'].execute("SELECT * FROM sales_data LIMIT 100").df().fillna('').astype(str)
    total = SHARED['con'].execute("SELECT COUNT(*) FROM sales_data").fetchone()[0]
    return {"columns": list(df.columns), "data": df.values.tolist(), "total_rows": total}

# ══════════════════════════════════════════════════════════════
#  EXPORT
# ══════════════════════════════════════════════════════════════
@app.post("/api/export")
async def export(req: PivotReq):
    result = await pivot(req)
    df     = pd.DataFrame(result['data'], columns=result['columns'])
    buf    = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name='Pivot')
    buf.seek(0)
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=sales_pivot.xlsx"})

# ══════════════════════════════════════════════════════════════
#  EXCEL2PARQUET
# ══════════════════════════════════════════════════════════════
@app.post("/api/get-sheets")
async def get_sheets(req: LoadReq):
    path = norm(req.file_path)
    if not os.path.exists(path): raise HTTPException(400, "File not found")
    try:
        xls = pd.ExcelFile(path)
        sheet_info = {}
        for sheet in xls.sheet_names:
            try:
                df = read_excel_robust(path, sheet_name=sheet)
                cols      = list(df.columns)
                types     = {c: str(df[c].dtype) for c in cols}
                row_count = len(df)
                sheet_info[sheet] = {"columns": cols, "types": types, "row_count": row_count}
            except:
                sheet_info[sheet] = {"columns": [], "types": {}, "row_count": 0}

        sheet_columns = {s: sheet_info[s]["columns"] for s in xls.sheet_names}
        first_cols    = sheet_columns.get(xls.sheet_names[0], []) if xls.sheet_names else []
        return {
            "sheets":       xls.sheet_names,
            "sheet_columns": sheet_columns,
            "sheet_info":   sheet_info,
            "columns":      first_cols        # backward compat
        }
    except Exception as e:
        raise HTTPException(400, str(e))

@app.post("/api/convert")
async def convert_to_parquet(req: ConvertReq):
    results = {"success": [], "errors": []}
    merged_dfs = []
    for fp in req.files:
        fp = norm(fp)
        if not os.path.exists(fp):
            results["errors"].append(f"{fp}: not found"); continue
        try:
            if is_parquet(fp):
                dfs_to_process = [pd.read_parquet(fp) if not HAS_POLARS
                                  else pl.read_parquet(fp).to_pandas()]
            else:
                xls    = pd.ExcelFile(fp)
                sheets = [s for s in (req.sheets or xls.sheet_names) if s in xls.sheet_names]
                dfs_to_process = []
                for sheet in sheets:
                    try:    df = pd.read_excel(fp, sheet_name=sheet, engine='calamine')
                    except: df = pd.read_excel(fp, sheet_name=sheet, engine='openpyxl')
                    if req.columns:
                        vc = [c for c in req.columns if c in df.columns]
                        if vc: df = df[vc]
                    df = df.fillna("").astype(str)
                    dfs_to_process.append(df)
            for df in dfs_to_process:
                if req.merge_all:
                    merged_dfs.append(df)
                else:
                    stem = os.path.splitext(os.path.basename(fp))[0]
                    out  = os.path.join(req.output_path, f"{stem}.parquet")
                    _write_parquet(df, out)
                    results["success"].append(os.path.basename(out))
        except Exception as e:
            results["errors"].append(f"{os.path.basename(fp)}: {str(e)}")

    if req.merge_all and merged_dfs:
        try:
            # --- SMART DTYPE MISMATCH FIX (From Desktop App) ---
            all_cols = set()
            for df in merged_dfs:
                all_cols.update(df.columns)
            
            for col in all_cols:
                col_types = set(str(df[col].dtype) for df in merged_dfs if col in df.columns)
                if len(col_types) > 1:
                    # Agar column types alag hain, toh conflict se bachne ke liye sabko string bana do
                    for df in merged_dfs:
                        if col in df.columns:
                            df[col] = df[col].astype(str)
            # ---------------------------------------------------

            final_df = pd.concat(merged_dfs, ignore_index=True)
            
            # Smart Output Naming: Agar user ne sirf folder diya hai, toh file ka naam khud set karo
            out_path = req.output_path
            if os.path.isdir(out_path):
                out_path = os.path.join(out_path, "Master_Merged.parquet")
            elif not out_path.endswith('.parquet'):
                out_path += '.parquet'

            _write_parquet(final_df, out_path)
            results["success"].append(os.path.basename(out_path))
        except Exception as e:
            results["errors"].append(f"Merge error: {str(e)}")
            
    return results

# ══════════════════════════════════════════════════════════════
#  CONVERT & DOWNLOAD  — streams parquet to browser (no server path)
# ══════════════════════════════════════════════════════════════
@app.post("/api/convert-download")
async def convert_download(req: ConvertDownloadReq):
    fp = norm(req.file)
    if not os.path.exists(fp): raise HTTPException(400, "File not found")
    try:
        xls    = pd.ExcelFile(fp)
        sheets = [s for s in (req.sheets or xls.sheet_names) if s in xls.sheet_names]
        dfs    = []
        for sheet in sheets:
            try:    df = pd.read_excel(fp, sheet_name=sheet, engine='calamine')
            except: df = pd.read_excel(fp, sheet_name=sheet, engine='openpyxl')
            if req.columns:
                vc = [c for c in req.columns if c in df.columns]
                if vc: df = df[vc]
            df = df.fillna("").astype(str)
            dfs.append(df)

        final_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        buf = io.BytesIO()
        if HAS_POLARS:    pl.from_pandas(final_df).write_parquet(buf, compression='snappy')
        elif HAS_PYARROW: pq.write_table(pa.Table.from_pandas(final_df), buf, compression='snappy')
        else:             final_df.to_parquet(buf, index=False)
        buf.seek(0)

        safe = (req.filename.strip() or "converted")
        if not safe.endswith('.parquet'): safe += '.parquet'
        return StreamingResponse(buf,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{safe}"'})
    except Exception as e:
        raise HTTPException(400, str(e))

# =================================================================
#  DEDICATED PARQUET APPENDER (NEW)
# =================================================================
class AppendParquetReq(BaseModel):
    files: List[str]
    filename: str = "Master_Appended"

@app.post("/api/append-parquet")
async def append_parquet(req: AppendParquetReq):
    if not req.files: raise HTTPException(400, "No files provided")
    dfs = []
    try:
        # 1. Read all Parquet files
        for fp in req.files:
            fp = norm(fp)
            if not os.path.exists(fp): continue
            if is_parquet(fp):
                df = pl.read_parquet(fp).to_pandas() if HAS_POLARS else pd.read_parquet(fp)
                dfs.append(df)
        
        if not dfs: raise HTTPException(400, "No valid Parquet data found")

        # 2. SMART DTYPE FIX (Prevents crash if column types mismatch)
        if len(dfs) > 1:
            all_cols = set()
            for df in dfs: all_cols.update(df.columns)
            for col in all_cols:
                col_types = set(str(df[col].dtype) for df in dfs if col in df.columns)
                if len(col_types) > 1:
                    for df in dfs:
                        if col in df.columns: df[col] = df[col].astype(str)

        # 3. Append (Merge) Data
        final_df = pd.concat(dfs, ignore_index=True)
        
        # 4. Prepare for Download
        buf = io.BytesIO()
        if HAS_POLARS:    pl.from_pandas(final_df).write_parquet(buf, compression='snappy')
        elif HAS_PYARROW: pq.write_table(pa.Table.from_pandas(final_df), buf, compression='snappy')
        else:             final_df.to_parquet(buf, index=False)
        buf.seek(0)

        safe = (req.filename.strip() or "Master_Appended")
        if not safe.endswith('.parquet'): safe += '.parquet'
        return StreamingResponse(buf,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{safe}"'})
    except Exception as e:
        raise HTTPException(400, str(e))

# ══════════════════════════════════════════════════════════════
#  VLOOKUP PRO
# ══════════════════════════════════════════════════════════════
@app.post("/api/vlookup-cols")
async def vlookup_cols(req: LoadReq):
    path = norm(req.file_path)
    if not os.path.exists(path): raise HTTPException(400, "File not found")
    try:
        df = (pl.read_parquet(path).to_pandas() if HAS_POLARS and is_parquet(path)
              else pd.read_parquet(path) if is_parquet(path)
              else read_excel_df(path))
        return {"columns": list(df.columns), "row_count": len(df)}
    except Exception as e:
        raise HTTPException(400, str(e))

@app.post("/api/vlookup")
async def vlookup(req: VlookupReq):
    if not SHARED['con']: raise HTTPException(400, "No main data loaded")
    path = norm(req.lookup_file)
    if not os.path.exists(path): raise HTTPException(400, "Lookup file not found")
    try:
        main_df = SHARED['con'].execute("SELECT * FROM sales_data").df()
        ldf     = (pl.read_parquet(path).to_pandas() if HAS_POLARS and is_parquet(path)
                   else pd.read_parquet(path) if is_parquet(path)
                   else read_excel_df(path))
        keep = [req.lookup_key] + [c for c in req.add_columns if c != req.lookup_key]
        ldf  = ldf[[c for c in keep if c in ldf.columns]]
        merged = main_df.merge(ldf, left_on=req.main_key, right_on=req.lookup_key,
                               how='left', suffixes=('', '_lkp'))
        SHARED['vlookup_df']   = merged
        SHARED['excel_dfs']    = []
        SHARED['loaded_files'] = set()
        _rebuild_shared()
        return {"status": "ok", "columns": SHARED['columns'],
                "col_types": SHARED['col_types'], "row_count": SHARED['row_count'],
                "added_columns": [c for c in req.add_columns if c != req.lookup_key]}
    except Exception as e:
        raise HTTPException(400, str(e))

# ══════════════════════════════════════════════════════════════
#  FILE BROWSER
# ══════════════════════════════════════════════════════════════
@app.get("/api/browse")
async def browse(path: str = "", mode: str = "all"):
    try:
        if not path:
            if platform.system() == "Windows":
                import string as _s
                drives = [f"{d}:/" for d in _s.ascii_uppercase if os.path.exists(f"{d}:/")]
                return {"path": "", "items": [{"name": d, "type": "drive", "path": d}
                                               for d in drives], "parent": ""}
            else:
                path = "/"
        path = path.replace("\\", "/")
        if not os.path.exists(path): raise HTTPException(400, "Path not found")

        items, PARQUET_EXT, EXCEL_EXT = [], {'.parquet', '.pq'}, {'.xlsx', '.xls', '.xlsb'}
        try:
            for e in sorted(os.scandir(path), key=lambda x: (not x.is_dir(), x.name.lower())):
                if e.is_dir() and not e.name.startswith('.'):
                    items.append({"name": e.name, "type": "folder",
                                  "path": e.path.replace("\\", "/")})
                elif e.is_file():
                    ext = os.path.splitext(e.name)[1].lower()
                    sz  = e.stat().st_size
                    ss  = f"{sz/1048576:.1f} MB" if sz > 1048576 else f"{sz/1024:.1f} KB"
                    if mode in ('all', 'parquet') and ext in PARQUET_EXT:
                        items.append({"name": e.name, "type": "parquet",
                                      "path": e.path.replace("\\", "/"), "size": ss})
                    elif mode in ('all', 'excel') and ext in EXCEL_EXT:
                        items.append({"name": e.name, "type": "excel",
                                      "path": e.path.replace("\\", "/"), "size": ss})
        except PermissionError:
            pass

        parent = os.path.dirname(path.rstrip('/')).replace("\\", "/")
        if parent == path.rstrip('/'): parent = ""
        return {"path": path, "items": items, "parent": parent}
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/api/status")
async def status():
    return {
        "loaded":         SHARED['con'] is not None,
        "row_count":      SHARED['row_count'],
        "active_sessions": len(SESSIONS),
        "loaded_files":   [os.path.basename(f) for f in SHARED['loaded_files']],
        "current_folder": SHARED['current_folder'],
    }
# ══════════════════════════════════════════════════════════════
#  FILE UPLOAD (For Remote Server Deployment)
# ══════════════════════════════════════════════════════════════
import os
import shutil

UPLOAD_DIR = os.path.abspath("./_Uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.post("/api/upload")
async def upload_files(files: List[UploadFile] = File(...), scube_sid: Optional[str] = Cookie(None)):
    try:
        uploaded_names = []
        for file in files:
            # File corrupt hone se bachane ke liye naam ke aage chhota sa random code lagaya
            unique_name = f"{uuid.uuid4().hex[:6]}_{file.filename}"
            file_path = os.path.join(UPLOAD_DIR, unique_name)
            
            with open(file_path, "wb") as buffer:
                import shutil
                shutil.copyfileobj(file.file, buffer)
            uploaded_names.append(unique_name)
            
        return {"status": "success", "filenames": uploaded_names}
        
    except Exception as e:
        raise HTTPException(500, f"Upload failed: {str(e)}")
# --------------------------------------------------------------
# AUTHENTICATION & ADMIN ROUTES
# --------------------------------------------------------------

@app.post("/api/login")
async def login(req: LoginReq, response: Response):
    users = load_users()
    u = users.get(req.username)
    
    if not u or not pwd_context.verify(req.password, u["password"]):
        raise HTTPException(401, "Invalid username or password")
    
    if not u["active"]:
        raise HTTPException(403, "Account deactivated. Contact Admin.")
    
    sid = str(uuid.uuid4())
    SESSIONS[sid] = {
        'last': time.time(),
        'user': req.username,
        'role': u["role"]
    }
    set_session_cookie(response, sid)
    
    add_log(req.username, "Logged In") # <--- YEH NAYI LINE ADD KARNI HAI
    
    return {"status": "ok", "role": u["role"], "username": req.username}

@app.get("/api/admin/list-users")
async def list_users(scube_sid: Optional[str] = Cookie(None)):
    sid, sess = get_session(scube_sid)
    if sess.get('role') != 'admin': 
        raise HTTPException(403, "Admin access required")
    users = load_users()
    return [{"username": k, "role": v["role"], "active": v["active"]} for k, v in users.items()]

@app.post("/api/admin/toggle-user")
async def toggle_user(req: UserManageReq, scube_sid: Optional[str] = Cookie(None)):
    sid, sess = get_session(scube_sid)
    if sess.get('role') != 'admin':
        raise HTTPException(403, "Admin access required")
    
    users = load_users()
    if req.username in users:
        users[req.username]["active"] = req.active
        save_users(users)
        return {"status": "success"}
    raise HTTPException(404, "User not found")
@app.post("/api/admin/delete-user")
async def delete_user(req: DeleteUserReq, scube_sid: Optional[str] = Cookie(None)):
    sid, sess = get_session(scube_sid)
    if sess.get('role') != 'admin': 
        raise HTTPException(403, "Admin access required")
    if req.username == 'admin': 
        raise HTTPException(400, "Cannot delete main admin")
    
    users = load_users()
    if req.username in users:
        del users[req.username]
        save_users(users)
        return {"status": "success"}
    raise HTTPException(404, "User not found")
@app.get("/api/admin/logs")
async def get_logs(scube_sid: Optional[str] = Cookie(None)):
    sid, sess = get_session(scube_sid)
    if sess.get('role') != 'admin': 
        raise HTTPException(403, "Admin access required")
    
    if not os.path.exists(LOG_DB_FILE): 
        return []
    with open(LOG_DB_FILE, "r") as f: 
        return json.load(f)
@app.post("/api/admin/create-user")
async def create_user(req: CreateUserReq, scube_sid: Optional[str] = Cookie(None)):
    sid, sess = get_session(scube_sid)
    if sess.get('role') != 'admin':
        raise HTTPException(403, "Admin access required")
    
    users = load_users()
    if req.username in users:
        raise HTTPException(400, "Username already exists")
    
    users[req.username] = {
        "password": pwd_context.hash(req.password),
        "role": req.role,
        "active": True
    }
    save_users(users)
    return {"status": "success"}
@app.get("/")
async def serve():
    response = FileResponse("dashboard.html")
    # Ye headers browser ko majboor karenge ki wo hamesha fresh HTML load kare
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    try:    local_ip = socket.gethostbyname(socket.gethostname())
    except: local_ip = "127.0.0.1"
    print(f"\n{'='*55}")
    print(f"  Sales Cube Dashboard  —  Ready")
    print(f"  Local  :  http://localhost:8000")
    print(f"  Network:  http://{local_ip}:8000")
    print(f"  Active Sessions: shared across all users")
    print(f"{'='*55}\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
