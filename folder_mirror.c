

// %comspec% /k "C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Auxiliary\Build\vcvars64.bat"
// cl /Zi /DDEBUG folder_mirror.c shell32.lib && folder_mirror.exe

#include <windows.h>
#include <stdlib.h>
#include <stdio.h>
#include <shellapi.h>
#include <math.h>
#include <wchar.h>

#ifdef DEBUG 
#ifndef _DEBUG
#define _DEBUG
#endif
#endif

#ifdef _DEBUG
#define FOLDER_MIRROR_DEBUG_WRAPPER(a) a
#else
#define FOLDER_MIRROR_DEBUG_WRAPPER(a)   
#endif


#define THROWX(msg, i)                                                       \
{                                                                            \
  printf("ERROR:%s(%d):%s\n", __FILE__, __LINE__, msg);                      \
  /*__asm{ int 3 };*/                                                        \
  __debugbreak();                                                              \
}                                                                            \

#define goto_exit_with_failure(getlasterror_p) \
{ \
  if(1) { \
    if(getlasterror_p) { \
      printf("%s(%d):%d\n", __FILE__, __LINE__, *((int *) (getlasterror_p))); \
    } else { \
      printf("%s(%d)\n", __FILE__, __LINE__); \
    } \
  } \
  goto exit_with_failure; \
} \

#define GenericGrowMacro1(type, arr__p, act_len__p, req_len, min_initial_len)  \
{ \
  size_t req_len_ = req_len, new_len_; \
  if(*(act_len__p) == 0) { \
    new_len_ = max(min_initial_len, req_len_ * 2); \
    *(arr__p) = malloc(new_len_ * sizeof(type)); \
    if(!*(arr__p)) goto_exit_with_failure(NULL); \
    *(act_len__p) = new_len_;     \
  } else if (*(act_len__p) < req_len_) { \
    type * tmp_;       \
    new_len_ = max(*(act_len__p), req_len_) * 2;  \
    tmp_ = realloc(*(arr__p), new_len_ * sizeof(type)); \
    if(!tmp_) goto_exit_with_failure(NULL); \
    *(arr__p) = tmp_; \
    *(act_len__p) = new_len_;     \
  } \
}

struct RateSmoother_s {
  double *window_x;   
  double *window_y;   
  int window_size;
  int observation_count;
};
typedef struct RateSmoother_s * RateSmoother;

#define RateSmoother_Add(o, x, y) \
{ \
  (o)->window_x[(o)->observation_count % (o)->window_size] = x; \
  (o)->window_y[(o)->observation_count % (o)->window_size] = y; \
  (o)->observation_count++; \
}

typedef DWORD (* CALLBACK cbxCopyFile_copyProgress)(
         LARGE_INTEGER totalSize, LARGE_INTEGER totalTransferred,
         LARGE_INTEGER streamSize, LARGE_INTEGER streamTransferred,
         DWORD streamNo, DWORD callbackReason, HANDLE src, HANDLE dst,
         LPVOID data);

static LARGE_INTEGER QueryPerformanceCounter_Freq = {0};

#define QueryPerformanceCounter_Beg(v_p)  \
{  \
  if(QueryPerformanceCounter_Freq.QuadPart == 0) \
    if(!QueryPerformanceFrequency(&QueryPerformanceCounter_Freq))  \
      THROWX("!QueryPerformanceFrequency(&QueryPerformanceCounter_Freq)", 1)  \
  QueryPerformanceCounter(v_p);  \
}

#define QueryPerformanceCounter_End(v, secs_p, sum_secs_p, sumsq_secs_p, curr__p) \
{ \
  LARGE_INTEGER tmp__; \
  QueryPerformanceCounter(&tmp__); \
  if(curr__p) { \
    *((LARGE_INTEGER *) (curr__p)) = tmp__; \
  } \
  *(secs_p) = (double)(tmp__.QuadPart - (v).QuadPart) / QueryPerformanceCounter_Freq.QuadPart; \
  if(sum_secs_p) { \
    *((double *) (sum_secs_p)) += *(secs_p); \
  } \
  if(sumsq_secs_p) { \
    *((double *) (sumsq_secs_p)) += (*(secs_p)) * (*(secs_p)); \
  } \
}
         
/* 
CopyFileExW         https://docs.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-copyfileexw         If the function fails, the return value is zero
CreateDirectoryExW  https://docs.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-CreateDirectoryExW  If the function succeeds, the return value is nonzero.
FindFirstFileW      https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-FindFirstFilew      If the function fails or fails to locate files from the search string in the lpFileName parameter, the return value is INVALID_HANDLE_VALUE
FindNextFileW       https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-FindNextFilew       If the function succeeds, the return value is nonzero
FindClose           https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-FindClose           If the function succeeds, the return value is nonzero.
*/          

#define WinAPICallWrapper_funcs(_ff_) \
_ff_(CopyFileExW        , BOOL  , 0, 3, (ERROR_FILE_NOT_FOUND, ERROR_REQUEST_ABORTED, ERROR_ACCESS_DENIED)) \
_ff_(CreateDirectoryExW , BOOL  , 0, 2, (ERROR_ALREADY_EXISTS, ERROR_PATH_NOT_FOUND)) \
_ff_(FindFirstFileW     , HANDLE, INVALID_HANDLE_VALUE, 1, (ERROR_FILE_NOT_FOUND)) \
_ff_(FindNextFileW      , BOOL  , 0, 99, ()) \
_ff_(FindClose          , BOOL  , 0, 99, ()) \
_ff_(FileTimeToSystemTime, BOOL , 0, 99, ()) \
\
_ff_(xCompareFilesAreIndentical, BOOL  , 0, 99, ()) \

#define z2020_11_05_11_42(ff, ret_type, failure_val, num_common_getlasterrors, common_getlasterrors) \
static const ret_type WinAPICallWrapper_FailureVals_##ff = failure_val;

WinAPICallWrapper_funcs(z2020_11_05_11_42)

struct WinAPICallWrapperStats_Elem_s {
  #define WinAPICallWrapperStats_Elem_fields(_ff_, gg1) \
  _ff_(gg1, double  , sum_secs    , 15,   "f") \
  _ff_(gg1, __int64 , call_count  , 10, "lld") \
  _ff_(gg1, __int64 , fail_count  , 10, "lld") \
  \
  /*_ff_(gg1, double  , sumsq_secs  , 15, ".6g") */ \
  /*_ff_(gg1, __int64 , error_count ,  2, "lld")*/ \
  
  #define z2020_11_05_15_20(gg1, type, name, width_fmt_1, fmt_1) type name;
  WinAPICallWrapperStats_Elem_fields(z2020_11_05_15_20, gg1)
};

#define WinAPICallWrapperStats_Elem_expand_commas(gg1, type, name, width_fmt_1, fmt_1) , (gg1)->name  
#define WinAPICallWrapperStats_Elem_expand_format(gg1, type, name, width_fmt_1, fmt_1) " %" #width_fmt_1 fmt_1

struct WinAPICallWrapperStats_s {
  #define z2020_11_05_11_48(ff, ret_type, failure_val, num_common_getlasterrors, common_getlasterrors) \
  struct WinAPICallWrapperStats_Elem_s ff;
  WinAPICallWrapper_funcs(z2020_11_05_11_48)  
  int qpc_disabled;
};
typedef struct WinAPICallWrapperStats_s * WinAPICallWrapperStats;

#define WinAPICallWrapperMacro(f, args, ret_p, stats_p, getlasterror_p, ERROR_HANDLER) \
{ \
  LARGE_INTEGER beg__; \
  double secs__; \
  (stats_p)->f.call_count++; \
  if(!(stats_p)->qpc_disabled) QueryPerformanceCounter_Beg(&beg__); \
  *(ret_p) = f args; \
  if(!(stats_p)->qpc_disabled) QueryPerformanceCounter_End(beg__, &secs__, &(stats_p)->f.sum_secs, NULL /*&(stats_p)->f.sumsq_secs*/, NULL); \
  if(getlasterror_p) { \
    if(*(ret_p) != WinAPICallWrapper_FailureVals_##f) { \
      *((int *) getlasterror_p) = 0; \
    } else { \
      (stats_p)->f.fail_count++; \
      *((int *) getlasterror_p) = GetLastError(); /* https://docs.microsoft.com/en-us/windows/win32/debug/system-error-codes--0-499- */ \
      if(*((int *) getlasterror_p) == 0) { \
        THROWX("*(getlasterror_p) == 0", 1) \
      } \
      ERROR_HANDLER; \
    } \
  } \
}
 
typedef int (*cbHeartbeatThread_Callback)(void * cbCtx, struct HeartbeatThread_s * o, unsigned int call_count);

struct HeartbeatThread_s {
  HANDLE hThread;
  HANDLE hStopEvent;
  DWORD dwThreadId;
  DWORD callbackIntervalMilliseconds;
  cbHeartbeatThread_Callback cbCallback; 
  void * vCallbackCtx;
};
typedef struct HeartbeatThread_s * HeartbeatThread;
 
struct BytesStack_s {
  char * stck;
  size_t *begs;
  size_t stck_max_size;
  size_t stck_count;
  size_t begs_max_size;
  size_t begs_count;
};
typedef struct BytesStack_s * BytesStack;

#define BytesStack_Peek(o) ((o)->begs_count <= 0 ? NULL : (o)->stck + (o)->begs[(o)->begs_count - 1])

struct PathStorage_s {
  //char *asci;
  wchar_t *wide;
  //size_t asci_elems;
  size_t wide_elems;
};
typedef struct PathStorage_s * PathStorage;

#define PathStorage_SetNull(o, pos) \
{ \
  /*(o)->asci[pos] = '\0';*/ \
  (o)->wide[pos] = 0; \
}

#define PathStorage_strlen(o) (wcslen((o)->wide))

struct RecursiveScan2RecursionLevel_s {
  WIN32_FIND_DATAW findData; // https://docs.microsoft.com/en-us/windows/win32/api/minwinbase/ns-minwinbase-win32_find_dataa
  HANDLE findHandle;
  int iter_ok;
};
typedef struct RecursiveScan2RecursionLevel_s * RecursiveScan2RecursionLevel;

typedef int (*cbRecursiveScan2_VisitFile)(void *cbCtx, struct RecursiveScan2_s *o, RecursiveScan2RecursionLevel rl, PathStorage path, size_t strlen_path, wchar_t *name, __int64 size, __int64 modt);
typedef int (*cbRecursiveScan2_BeforeRecursion)(void *cbCtx, struct RecursiveScan2_s *o, RecursiveScan2RecursionLevel rl, PathStorage path, size_t strlen_path, size_t strlen_last_folder_s, int * user_signal);
typedef int (*cbRecursiveScan2_AfterRecursion)(void *cbCtx, struct RecursiveScan2_s *o, RecursiveScan2RecursionLevel rl, PathStorage path, size_t strlen_path, size_t strlen_last_folder_s);  
  
struct RecursiveScan2_s {
  struct PathStorage_s path;
  void *cbCtx;
  cbRecursiveScan2_VisitFile cbVisitFile;
  cbRecursiveScan2_BeforeRecursion cbBeforeRecursion;
  cbRecursiveScan2_AfterRecursion cbAfterRecursion;
  int cbRet;
  struct WinAPICallWrapperStats_s win_api_stats;
};
typedef struct RecursiveScan2_s * RecursiveScan2;

#define RecursiveScan2RecursionLevel_CurrentModifiedTime(o) (*(__int64 *) &((o)->findData.ftLastWriteTime)) // https://www.silisoftware.com/tools/date.php
#define RecursiveScan2RecursionLevel_CurrentFileSize(o) (((__int64)((*(unsigned int *) &((o)->findData.nFileSizeHigh))) << 8 * sizeof(unsigned int)) + (__int64)((*(unsigned int *) &((o)->findData.nFileSizeLow))))
#define RecursiveScan2RecursionLevel_CurrentName(o) ((o)->findData.cFileName)
#define RecursiveScan2RecursionLevel_CurrentIsDir(o) (((o)->findData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0)

typedef int (*cbRecursiveMirrorScan2_PossiblyMissingDir)(void * cbCtx, struct RecursiveMirrorScan2_s * o, struct RecursiveScan2_s * src, size_t strlen_path_src, struct RecursiveScan2_s * dst, size_t strlen_path_dst, wchar_t * src_dir_name, int *folder_already_exists);
typedef int (*cbRecursiveMirrorScan2_CommonFile)(void * cbCtx, struct RecursiveMirrorScan2_s * o, struct RecursiveScan2_s * src, size_t strlen_path_src, struct RecursiveScan2_s * dst, size_t strlen_path_dst, __int64 size_src, __int64 size_dst, __int64 modt_src, __int64 modt_dst);
typedef int (*cbRecursiveMirrorScan2_MismatchedFileNameAndDirName)(void * cbCtx, struct RecursiveMirrorScan2_s * o, struct RecursiveScan2_s * src, size_t strlen_path_src, struct RecursiveScan2_s * dst, size_t strlen_path_dst, int is_dir_src, int is_dir_dst, __int64 size_src, __int64 size_dst);
typedef int (*cbRecursiveMirrorScan2_PossiblyMissingFile)(void * cbCtx, struct RecursiveMirrorScan2_s * o, struct RecursiveScan2_s * src, size_t strlen_path_src, struct RecursiveScan2_s * dst, size_t strlen_path_dst, __int64 size_src);

struct RecursiveMirrorScan2_s {
  BytesStack common_subdirs_stack;
  BytesStack possibly_missing_subdirs_stack;
  void *cbCtx;
  cbRecursiveMirrorScan2_PossiblyMissingDir cbPossiblyMissingDir;
  cbRecursiveMirrorScan2_CommonFile cbCommonFile;
  cbRecursiveMirrorScan2_MismatchedFileNameAndDirName cbMismatchedFileNameAndDirName;
  cbRecursiveMirrorScan2_PossiblyMissingFile cbPossiblyMissingFile;
};
typedef struct RecursiveMirrorScan2_s * RecursiveMirrorScan2;

struct TotalFileSystemStats_s {
  #define TotalFileSystemStats_fields(_ff_) \
  _ff_(cbCopyProgress_TransferredCurr) \
  _ff_(cbCopyProgress_Transferred) \
  _ff_(cbCopyProgress_Size) \
  _ff_(CompareReadBytes) \
  _ff_(SourceBytes) \
  _ff_(Files) \
  _ff_(CopyBytes)
  
  #define TotalFileSystemStats_fields_exp1(name) LARGE_INTEGER name;
  TotalFileSystemStats_fields(TotalFileSystemStats_fields_exp1)
};
typedef struct TotalFileSystemStats_s * TotalFileSystemStats;


struct CopyDirRecursion_s {
  RecursiveScan2 dst_ref;
  
  struct WinAPICallWrapperStats_s win_api_stats;
  struct TotalFileSystemStats_s tfss;
  
  size_t strlen_path_dst;
  int skip_copy;
};
typedef struct CopyDirRecursion_s * CopyDirRecursion;

         
struct SyncSrc2Dst_s {
  struct CopyDirRecursion_s cdr;  
  
  struct WinAPICallWrapperStats_s win_api_stats;
  struct TotalFileSystemStats_s tfss;
  
  LARGE_INTEGER qpfc_beg;
  RecursiveScan2 src_ref;
  RecursiveScan2 dst_ref;
  struct ParallelFileComparer_s * pfc;
  int use_modified_times_otherwise_compare_files;
  int skip_copy;
  char *comp_buff1;
  char *comp_buff2;
  size_t comp_buff1_max_size;
  size_t comp_buff2_max_size;
  
  struct RateSmoother_s rs_1;
  struct RateSmoother_s rs_2;
  struct RateSmoother_s rs_3;
};
typedef struct SyncSrc2Dst_s * SyncSrc2Dst;   

typedef int (*cbSemaphoredWorkerThread_RunWork)(void * cbCtx, struct SemaphoreDriver_s * driver, struct SemaphoredWorkerThread_s * o, void *workCtx, unsigned int index_Zb, unsigned int call_count);
typedef int (*cbSemaphoredWorkerThread_ObserveWorkAllocation)(void * cbCtx, struct SemaphoreDriver_s * driver, struct SemaphoredWorkerThread_s * o, void *initial_workCtx, unsigned int index_Zb, void **actual_workCtx, int *bAutoFreeWorkCtxOnCompletion);

struct SemaphoredWorkerThread_s {
  struct SemaphoreDriver_s * driver;
  HANDLE hThread;
  HANDLE hSignalWorkerEvent;
  DWORD dwThreadId;  
  void *workCtx;
  int bAutoFreeWorkCtxOnCompletion;
  unsigned int index_Zb;  
  unsigned int waiting_workers_indices_index;
  unsigned int working_workers_indices_index;
  int continue_looping;
};
typedef struct SemaphoredWorkerThread_s * SemaphoredWorkerThread;

struct SemaphoreDriver_s {
  HANDLE hSemaphore;
  HANDLE hAvailableWorkerEvent;
  CRITICAL_SECTION state_lock;
  DWORD sleepIntervalMilliseconds;  
  void *cbCtx;
  cbSemaphoredWorkerThread_RunWork cbRunWork;  
  cbSemaphoredWorkerThread_ObserveWorkAllocation cbObserveWorkAllocation;
  struct SemaphoredWorkerThread_s * workers;
  int* waiting_workers_indices;  
  int* working_workers_indices;  
  int workers_count;  
  int waiting_workers_count;
  int working_workers_count;
  int i;
};
typedef struct SemaphoreDriver_s * SemaphoreDriver;

struct ParallelFileComparerJob_s {
  wchar_t *path1;
  wchar_t *name1;
  wchar_t *path2;
  wchar_t *name2;
  size_t size1;
  size_t size2;
};
typedef struct ParallelFileComparerJob_s * ParallelFileComparerJob;

struct ParallelFileComparisonWorkspace_s {
  struct PathStorage_s path1;
  struct PathStorage_s path2;
  size_t size1;
  size_t size2;  
  char *comp_buff1;
  char *comp_buff2;
  size_t comp_buff1_max_size;
  size_t comp_buff2_max_size;  
  
  struct WinAPICallWrapperStats_s win_api_stats;
  struct TotalFileSystemStats_s tfss;
  
};
typedef struct ParallelFileComparisonWorkspace_s * ParallelFileComparisonWorkspace;

typedef int (*cbParallelFileComparer_ObserveComparisonResult)(void * cbCtx, struct ParallelFileComparer_s * pfc, wchar_t *src_file, wchar_t *dst_file, __int64 size_src, __int64 size_dst, int files_are_identical);

struct ParallelFileComparer_s {
  int max_parallel_comparisons;
  int total_queued_jobs;
  struct ParallelFileComparisonWorkspace_s * workspaces;
  struct SemaphoreDriver_s smd;
  int non_identical_auto_copy_mode;
};
typedef struct ParallelFileComparer_s * ParallelFileComparer;

#define ParallelFileComparer_IterateStats(pfc, HANDLER) \
{ \
  int i__; \
  for(i__ = 0; i__ < (pfc)->max_parallel_comparisons; i__++) { \
    struct WinAPICallWrapperStats_s * win_api_stats_iter = &(pfc)->workspaces[i__].win_api_stats; \
    struct TotalFileSystemStats_s * tfss_iter = &(pfc)->workspaces[i__].tfss; \
    HANDLER; \
  } \
}

#define SYSTEMTIME_fields(_ff_) \
_ff_(wYear        , 4, "-") \
_ff_(wMonth       , 2, "-") \
_ff_(wDay         , 2, "T") \
_ff_(wHour        , 2, ":") \
_ff_(wMinute      , 2, ":") \
_ff_(wSecond      , 2, ".") \
_ff_(wMilliseconds, 3, "") \

#define SYSTEMTIME_fields_bytes(field, format_width, suffix) + format_width

const char * FILETIME_format(FILETIME * ft, char buffer[1 + SYSTEMTIME_fields(SYSTEMTIME_fields_bytes)]) {
  int ret;
  SYSTEMTIME st;
  struct WinAPICallWrapperStats_s win_api_stats = {0};
  WinAPICallWrapperMacro(FileTimeToSystemTime, (ft, &st), &ret, &win_api_stats, NULL, NULL);
  #define SYSTEMTIME_fields_fmt(field, format_width, suffix) "%0" #format_width "d" suffix
  #define SYSTEMTIME_fields_dat(field, format_width, suffix) , st.field
  sprintf(buffer, SYSTEMTIME_fields(SYSTEMTIME_fields_fmt) SYSTEMTIME_fields(SYSTEMTIME_fields_dat));
  return buffer;
}

int RateSmoother_GetValue(RateSmoother o, double *rate, int *rate_undefined) {
  if(o->observation_count <= 1) {
    *rate_undefined = 1;
  } else {
    int temp = o->observation_count - 1;
    int i1 = (temp >= o->window_size ? ((temp + 1) % o->window_size) : 0);
    int i2 = temp % o->window_size;
    double x1 = o->window_x[i1];
    double x2 = o->window_x[i2];
    double y1 = o->window_y[i1];
    double y2 = o->window_y[i2];
    if(x1 == x2) {
      *rate_undefined = 1;
    } else {
      *rate_undefined = 0;
      if(y1 == y2) {
        *rate = 0.0;
      } else {
        *rate = (y2 - y1) / (x2 - x1);
      }
    }
  }
  return 0;
exit_with_failure:;
  return 1;   
}

void RateSmoother_Free(RateSmoother o) { 
  if(o->window_x) free(o->window_x); 
  if(o->window_y) free(o->window_y); 
  memset(o, 0, sizeof(*o));
}

int RateSmoother_Init(RateSmoother o, int size) {
  int zero_count;
  memset(o, 0, sizeof(*o));
  o->window_size = size;
  zero_count = 0; GenericGrowMacro1(double , &o->window_x, &zero_count, size, size);  
  zero_count = 0; GenericGrowMacro1(double , &o->window_y, &zero_count, size, size);  
  memset(o->window_x, 0, sizeof(*o->window_x) * o->window_size);
  memset(o->window_y, 0, sizeof(*o->window_y) * o->window_size);
  return 0;
exit_with_failure:;
  RateSmoother_Free(o);
  return 1;    
}

int RateSmoother_Reset(RateSmoother o) {
  memset(o->window_x, 0, sizeof(*o->window_x) * o->window_size);
  memset(o->window_y, 0, sizeof(*o->window_y) * o->window_size);
  o->observation_count = 0;
  return 0;
exit_with_failure:;
  return 1;      
}

int WinAPICallWrapperStats_CopyFile(WinAPICallWrapperStats o, cbxCopyFile_copyProgress cbCopyProgress, void * vCopyProgressCtx, wchar_t *src_file, wchar_t *dst_file, __int64 size_src, LONGLONG *totalCopyBytes_QuadPart) {
  int m_stop = 0, ret;
  int getlasterror;
  
  //printf("EXPORT \"%ls\" \"%ls\"\n", src_file, dst_file);
  //return 0;
  
  WinAPICallWrapperMacro(CopyFileExW, (src_file, dst_file, cbCopyProgress, vCopyProgressCtx, &m_stop, 0), &ret, o, &getlasterror, 
    if(getlasterror == ERROR_ACCESS_DENIED) {
      printf("%s(%d):ERROR_ACCESS_DENIED:'%ls'\n", __FILE__, __LINE__, src_file);        
      goto exit_with_warning;
    }         
    if(getlasterror == ERROR_INVALID_NAME) {
      printf("%s(%d):ERROR_INVALID_NAME:'%ls'\n", __FILE__, __LINE__, src_file);
      goto exit_with_warning;
    }               
    if(getlasterror == ERROR_CANT_ACCESS_FILE) {
      printf("%s(%d):ERROR_CANT_ACCESS_FILE:'%ls'\n", __FILE__, __LINE__, src_file);
      goto exit_with_warning;
    }         
    if(getlasterror == ERROR_DISK_FULL) {
      printf("%s(%d):ERROR_DISK_FULL:'%ls'\n", __FILE__, __LINE__, src_file);
      goto exit_with_failure;
    }         
    goto_exit_with_failure(&getlasterror);  
  );
  *totalCopyBytes_QuadPart += size_src;  
exit_with_warning:;
  return 0;
exit_with_failure:;
  return 1;    
}


void TotalFileSystemStats_Free(TotalFileSystemStats o) { 
  memset(o, 0, sizeof(*o));
}

int TotalFileSystemStats_Init(TotalFileSystemStats o) {
  memset(o, 0, sizeof(*o));
  return 0;
exit_with_failure:;
  TotalFileSystemStats_Free(o);
  return 1;    
}

int TotalFileSystemStats_Reset(TotalFileSystemStats o) {
  memset(o, 0, sizeof(*o));
  return 0;
exit_with_failure:;
  return 1;    
}

int TotalFileSystemStats_Add(TotalFileSystemStats o, TotalFileSystemStats to_add_from) {
  #define TotalFileSystemStats_fields_exp2(name) o->name.QuadPart += to_add_from->name.QuadPart;
  TotalFileSystemStats_fields(TotalFileSystemStats_fields_exp2)
  return 0;
exit_with_failure:;
  return 1;    
}

DWORD CALLBACK TotalFileSystemStats_cbCopyProgress(
         LARGE_INTEGER totalSize, LARGE_INTEGER totalTransferred,
         LARGE_INTEGER streamSize, LARGE_INTEGER streamTransferred,
         DWORD streamNo, DWORD callbackReason, HANDLE src, HANDLE dst,
         LPVOID data) 
{
  TotalFileSystemStats ctx = data;  
  if(callbackReason == CALLBACK_STREAM_SWITCH) {
    ctx->cbCopyProgress_Size.QuadPart += totalSize.QuadPart;
  }
  ctx->cbCopyProgress_TransferredCurr.QuadPart = totalTransferred.QuadPart;
  //printf("asxasx %lld %lld %lld %lld %d %d\n", totalSize.QuadPart, totalTransferred.QuadPart, streamSize.QuadPart, streamTransferred.QuadPart, streamNo, callbackReason);
  return PROGRESS_CONTINUE;  // https://docs.microsoft.com/en-us/windows/win32/api/winbase/nc-winbase-lpprogress_routine
}

void TotalFileSystemStats_cbCopyProgress_Bef(TotalFileSystemStats o) {
  o->cbCopyProgress_TransferredCurr.QuadPart = 0;
}

void TotalFileSystemStats_cbCopyProgress_Aft(TotalFileSystemStats o) {
  LONGLONG tmp = o->cbCopyProgress_TransferredCurr.QuadPart;
  o->cbCopyProgress_TransferredCurr.QuadPart = 0;  
  o->cbCopyProgress_Transferred.QuadPart += tmp;
}

void PathStorage_Free(PathStorage o) {
  //if(o->asci) free(o->asci);
  if(o->wide) free(o->wide);  
  memset(o, 0, sizeof(*o));
}

int PathStorage_Init(PathStorage o) {
  memset(o, 0, sizeof(*o));
  return 0;
exit_with_failure:;
  PathStorage_Free(o);
  return 1;    
}

int PathStorage_SafeAppendStringToPath(PathStorage o, size_t strlen_path, wchar_t *w, size_t * strlen_s) {
  *strlen_s = wcslen(w);
  //GenericGrowMacro1(char    , &o->asci, &o->asci_elems, strlen_path + *strlen_s + 2, 32);
  GenericGrowMacro1(wchar_t , &o->wide, &o->wide_elems, strlen_path + *strlen_s + 2, 32);  
  memcpy(&o->wide[strlen_path], w, sizeof(*w) * (*strlen_s + 1));
  /*
  size_t l = wcstombs(&o->asci[strlen_path], w, o->asci_elems - strlen_path);
  if(l != *strlen_s) {
    if(l != (size_t) -1) {
#ifdef _DEBUG    
      printf("PathStorage_SafeAppendStringToPath:'%ls' %zd %zd %zd\n", w, l, *strlen_s, o->asci_elems - strlen_path);
      THROWX("l != *strlen_s", 1)
#endif      
    } else {      
      // It seems some characters were Unicode ... create our own version of the ascii string
      wchar_t *ww; 
      char *ss;
      int i;
      for(i = 0, ww = w, ss = &o->asci[strlen_path]; i < *strlen_s; i++, ww++, ss++) {
        if(*((char *) ww + 1) != '\0') {
          *ss = '?';
        } else {
          *ss = *(char *) ww;
        }
      }
      *ss = '\0';    
    }
  }
  */
  return 0;
exit_with_failure:;
  return 1;
}

int PathStorage_SafeAppendPathSep(PathStorage o, size_t *strlen_path) {
  if(wcsicmp(&o->wide[*strlen_path - 1], L"\\") != 0) {
    // 2020_10_20_09_47
    size_t strlen_s;
    if(PathStorage_SafeAppendStringToPath(o, *strlen_path, L"\\", &strlen_s)) {
      goto_exit_with_failure(NULL);
    }
    (*strlen_path)++;
  }
  return 0;
exit_with_failure:;
  return 1;
}



void HeartbeatThread_Free(HeartbeatThread o) {
  if(o->hStopEvent) CloseHandle(o->hStopEvent);
  if(o->hThread) CloseHandle(o->hThread);
  memset(o, 0, sizeof(*o));
}

int HeartbeatThread_Init(HeartbeatThread o) {
  memset(o, 0, sizeof(*o));
  o->hStopEvent = CreateEventA(NULL /*lpEventAttributes*/, 0 /* bManualReset */, 0 /* bInitialState */, NULL /* lpName */);
  if(!o->hStopEvent) {
    goto_exit_with_failure(NULL);
  }    
  return 0;
exit_with_failure:;
  HeartbeatThread_Free(o);
  return 1;    
}

DWORD WINAPI HeartbeatThread_ThreadFunction(LPVOID lpParam) {
  HeartbeatThread ctx = lpParam;
  unsigned int call_count = 0;
  int ret, continue_looping = 1;
  while(continue_looping) {
    if(ctx->cbCallback) {     
        call_count += 1;
        ctx->cbCallback(ctx->vCallbackCtx, ctx, call_count);
    }
    ret = WaitForSingleObject(ctx->hStopEvent, (ctx->callbackIntervalMilliseconds < 0 ? 0 : ctx->callbackIntervalMilliseconds));
    switch(ret) {
    case(WAIT_ABANDONED): continue_looping = 0; break;
    case(WAIT_OBJECT_0):  continue_looping = 0; break;
    case(WAIT_TIMEOUT):   continue_looping = 1; break;
    case(WAIT_FAILED):    continue_looping = 0; break;
    default:              continue_looping = 0; break;
    }    
  }
  if(ctx->hThread) {
    HANDLE tmp = ctx->hThread;
    ctx->hThread = NULL;
    CloseHandle(tmp);
  }
  return 0;
}

int HeartbeatThread_Run(HeartbeatThread o) {
  int ret = ResetEvent(o->hStopEvent);
  if(!ret) {
    ret = GetLastError();
  } else {
    ret = 0;
    o->hThread = CreateThread( 
              NULL,               // default security attributes
              0,                  // use default stack size  
              HeartbeatThread_ThreadFunction,       // thread function name
              o,                  // argument to thread function 
              0,                  // use default creation flags 
              &o->dwThreadId);    // returns the thread identifier 
    if(!o->hThread) {
      ret = GetLastError();
    }
  }
  return ret;
}

int HeartbeatThread_Stop(HeartbeatThread o) {
  int ret;
  if(!o->hStopEvent) {
    ret = 0;
  } else {
    ret = SetEvent(o->hStopEvent);
    if (!ret) {
      ret = GetLastError();
    } else {
      ret = 0;
    }
  }
  return ret;
}

int HeartbeatThread_Join(HeartbeatThread o, int dwMilliseconds, int * is_alive) {
  int ret;
  if(!o->hThread) {
    ret = 0;
    *is_alive = 0;    
  } else {
    ret = WaitForSingleObject(o->hThread, (dwMilliseconds < 0 ? INFINITE : dwMilliseconds));
    switch(ret) {
    case(WAIT_ABANDONED): ret = 1; *is_alive = 0; break;
    case(WAIT_OBJECT_0):  ret = 0; *is_alive = 0; break;
    case(WAIT_TIMEOUT):   ret = 0; *is_alive = 1; break;
    case(WAIT_FAILED):    ret = 1; *is_alive = 0; break;
    default:              ret = 1; *is_alive = 0; break;
    }
    if(ret) {
      ret = GetLastError();
      if(ret == ERROR_INVALID_HANDLE && !o->hThread) {
        ret = 0;
        *is_alive = 0;
      }
    }
  }
  return ret;  
}

int cbHeartbeatThread_Callback_Test(void * cbCtx, struct HeartbeatThread_s * o, unsigned int call_count) {
  printf("cb %d\n", call_count);
  return 0;
}

int cbHeartbeatThread_Test() { // cbHeartbeatThread_Test
  struct HeartbeatThread_s hb_s, * const hb = &hb_s;   
  int is_alive, loop;
  
  if(HeartbeatThread_Init(hb)) {
    goto_exit_with_failure(NULL);    
  }
  hb->cbCallback = cbHeartbeatThread_Callback_Test;
  hb->callbackIntervalMilliseconds = 1000;
  if(HeartbeatThread_Run(hb)) {
    goto_exit_with_failure(NULL);    
  }
  is_alive = 1;
  loop = 0;
  while(is_alive) {
    loop++;
    printf("asasx %d\n", loop);
    if(HeartbeatThread_Join(hb, 1000, &is_alive)) {
      goto_exit_with_failure(NULL);    
    }
    if(loop >= 5) {
      printf("asasx %d break\n", loop);
      if(HeartbeatThread_Stop(hb)) {
        goto_exit_with_failure(NULL);            
      }
    }
  }
  printf("Finished\n");  
  
  HeartbeatThread_Free(hb);
  
  return 0;
exit_with_failure:;
  printf("exit_with_failure");
  return 1;     
}

char * format_bytes(double bytes, int decimal_places, char *buff) {
  int power = pow(2, 10);
  int n = 0;
  const char *power_labels[] = {"bytes", "kb", "Mb", "Gb", "Tb", "Pb"};
  while(bytes > power) {
    bytes /= power;
    n++;
  }
  if(bytes == 0.0 || n == 0) {
    sprintf(buff, "%dbytes", (int) bytes);
  } else {
    sprintf(buff, "%.*f%s", min(3 * n, decimal_places), bytes, power_labels[n]);
  }   
  return buff;
}


void WinAPICallWrapperStats_Free(WinAPICallWrapperStats o) {
  memset(o, 0, sizeof(*o));
}

int WinAPICallWrapperStats_Init(WinAPICallWrapperStats o) {
  memset(o, 0, sizeof(*o));
  return 0;
exit_with_failure:;
  WinAPICallWrapperStats_Free(o);
  return 1;     
}

int WinAPICallWrapperStats_Reset(WinAPICallWrapperStats o) {
  #define z2020_11_05_15_21(gg1, type, name, width_fmt_1, fmt_1) o->gg1.name = 0;
  #define z2020_11_05_13_48(ff, ret_type, failure_val, num_common_getlasterrors, common_getlasterrors) \
  WinAPICallWrapperStats_Elem_fields(z2020_11_05_15_21, ff)
  WinAPICallWrapper_funcs(z2020_11_05_13_48)  
  return 0;
exit_with_failure:;
  return 1;     
}

int WinAPICallWrapperStats_Add(WinAPICallWrapperStats o, WinAPICallWrapperStats to_add_from) {
  #define z2020_11_05_15_22(gg1, type, name, width_fmt_1, fmt_1) o->gg1.name += to_add_from->gg1.name;
  #define z2020_11_05_13_25(ff, ret_type, failure_val, num_common_getlasterrors, common_getlasterrors) \
  WinAPICallWrapperStats_Elem_fields(z2020_11_05_15_22, ff)
  WinAPICallWrapper_funcs(z2020_11_05_13_25)  
  return 0;
exit_with_failure:;
  return 1;     
}
  
int WinAPICallWrapperStats_Sum(WinAPICallWrapperStats o, struct WinAPICallWrapperStats_Elem_s *e, int skip_initialize_2_zero, char *match_name) {  
  int found = 0;
  #define z2020_11_05_15_23b(gg1, type, name, width_fmt_1, fmt_1) e->name = 0;
  #define z2020_11_05_15_23c(gg1, type, name, width_fmt_1, fmt_1) e->name += o->gg1.name;
  if(!skip_initialize_2_zero) {WinAPICallWrapperStats_Elem_fields(z2020_11_05_15_23b, gg1)}
  #define z2020_11_05_13_45(ff, ret_type, failure_val, num_common_getlasterrors, common_getlasterrors) \
  if(!match_name || strcmp(match_name, #ff) == 0) { \
    found = 1; \
    WinAPICallWrapperStats_Elem_fields(z2020_11_05_15_23c, ff) \
  }
  WinAPICallWrapper_funcs(z2020_11_05_13_45)  
#ifdef _DEBUG
  if(!found) {
    THROWX("!found", 1)
  }
#endif
  return 0;
exit_with_failure:;
  return 1;     
}

int WinAPICallWrapperStats_Print(WinAPICallWrapperStats o, FILE *f, double *sum_perc_denominator) {
  #define z2020_11_05_15_24a(gg1, type, name, width_fmt_1, fmt_1) type name##_total = 0;
  #define z2020_11_05_15_24b(gg1, type, name, width_fmt_1, fmt_1) name##_total += o->gg1.name;
  #define z2020_11_05_15_24d(gg1, type, name, width_fmt_1, fmt_1) , name##_total  
  static const int max_strlen = 40;
  WinAPICallWrapperStats_Elem_fields(z2020_11_05_15_24a, gg1)    
  double perc;
  double perc_total = 0.0; 
  
  #define z2020_11_05_13_5x_FMT "%-*s" WinAPICallWrapperStats_Elem_fields(WinAPICallWrapperStats_Elem_expand_format, gg1)
  
  #define z2020_11_05_13_5x(ff) \
  WinAPICallWrapperStats_Elem_fields(z2020_11_05_15_24b, ff)  
  
  #define z2020_11_05_13_58(ff, ret_type, failure_val, num_common_getlasterrors, common_getlasterrors) \
  z2020_11_05_13_5x(ff) \
  perc = o->ff.sum_secs / (*sum_perc_denominator <= 0.0 ? 1.0 : *sum_perc_denominator); \
  perc_total += perc; \
  fprintf(f, z2020_11_05_13_5x_FMT " %5.1f%%\n", max_strlen, #ff WinAPICallWrapperStats_Elem_fields(WinAPICallWrapperStats_Elem_expand_commas, &o->ff), 100.0 * perc);
  
  #define z2020_11_05_13_57(ff, ret_type, failure_val, num_common_getlasterrors, common_getlasterrors) \
  z2020_11_05_13_5x(ff) \
  fprintf(f, z2020_11_05_13_5x_FMT "\n", max_strlen, #ff WinAPICallWrapperStats_Elem_fields(WinAPICallWrapperStats_Elem_expand_commas, &o->ff));
  
  if(sum_perc_denominator) {
    WinAPICallWrapper_funcs(z2020_11_05_13_58)  
    fprintf(f, z2020_11_05_13_5x_FMT " %5.1f%%\n", max_strlen, "Total" WinAPICallWrapperStats_Elem_fields(z2020_11_05_15_24d, ff), 100.0 * perc_total);
  } else {
    WinAPICallWrapper_funcs(z2020_11_05_13_57)  
    fprintf(f, z2020_11_05_13_5x_FMT "\n", max_strlen, "Total" WinAPICallWrapperStats_Elem_fields(z2020_11_05_15_24d, ff));
  }
  
  return 0;
exit_with_failure:;
  return 1;     
}
       
         

void BytesStack_Free(BytesStack o) {
  if(o->stck) free(o->stck);
  if(o->begs) free(o->begs);
  memset(o, 0, sizeof(*o));
}

int BytesStack_Init(BytesStack o) {
  memset(o, 0, sizeof(*o));
  return 0;
exit_with_failure:;
  BytesStack_Free(o);
  return 1;
}

void BytesStack_Destroy(BytesStack * o) {
  if(*o) {
    BytesStack_Free(*o);
    free(*o);
    *o = NULL;
  }
}

int BytesStack_Create(BytesStack *o) {
  *o = malloc(sizeof(**o));
  if(!*o) goto_exit_with_failure(NULL);
  if(BytesStack_Init(*o)) {
      goto_exit_with_failure(NULL);
  }
  return 0;
exit_with_failure:;
  BytesStack_Destroy(o);
  return 1;
}

int BytesStack_PushChunk(BytesStack o, size_t chunk_bytes, void **chunk_beg) {
  GenericGrowMacro1(char  , &o->stck, &o->stck_max_size, o->stck_count + chunk_bytes, 32)
  GenericGrowMacro1(size_t, &o->begs, &o->begs_max_size, o->begs_count + 1, 32)
  *chunk_beg = &o->stck[o->stck_count];  
  o->begs[o->begs_count] = (char *) *chunk_beg - o->stck;
  o->begs_count++;
  o->stck_count += chunk_bytes;
  return 0;
exit_with_failure:;
  *chunk_beg = NULL;
  return 1;
}

int BytesStack_PushWChar(BytesStack o, wchar_t * s) {
  size_t strlen_s = wcslen(s);
  char *chunk_beg;
  if(BytesStack_PushChunk(o, sizeof(*s) * (strlen_s + 1), &chunk_beg)) {
    goto_exit_with_failure(NULL);
  }
  memcpy(chunk_beg, s, sizeof(*s) * (strlen_s + 1));
  return 0;
exit_with_failure:;
  return 1;
}

int BytesStack_Pop(BytesStack o, void ** ss) {
  if(o->begs_count <= 0) {
    *ss = NULL;
    goto exit_with_empty;
  } else {
    o->begs_count--;
    *ss = o->stck + o->begs[o->begs_count];
    o->stck_count -= strlen(*ss) + 1;
  }
  return 1;
exit_with_empty:;
  return 0;  
}

void RecursiveMirrorScan2_Free(RecursiveMirrorScan2 o) {
  BytesStack_Destroy(&o->common_subdirs_stack);
  BytesStack_Destroy(&o->possibly_missing_subdirs_stack);
  memset(o, 0, sizeof(*o));
}

int RecursiveMirrorScan2_Init(RecursiveMirrorScan2 o) {
  memset(o, 0, sizeof(*o));
  if(BytesStack_Create(&o->common_subdirs_stack)) {
    goto_exit_with_failure(NULL);
  }
  if(BytesStack_Create(&o->possibly_missing_subdirs_stack)) {
    goto_exit_with_failure(NULL);
  }
  return 0;
exit_with_failure:;
  RecursiveMirrorScan2_Free(o);
  return 1;
}

void RecursiveScan2RecursionLevel_Free(RecursiveScan2RecursionLevel o, WinAPICallWrapperStats win_api_stats) {
  int ret;
  if(o->findHandle != INVALID_HANDLE_VALUE) WinAPICallWrapperMacro(FindClose, (o->findHandle), &ret, win_api_stats, NULL, NULL);
  memset(o, 0, sizeof(*o));
  o->findHandle = INVALID_HANDLE_VALUE;
}

int RecursiveScan2RecursionLevel_Init(RecursiveScan2RecursionLevel o, WinAPICallWrapperStats win_api_stats) {
  memset(o, 0, sizeof(*o));
  o->findHandle = INVALID_HANDLE_VALUE;
  return 0;
exit_with_failure:;
  RecursiveScan2RecursionLevel_Free(o, win_api_stats);
  return 1;
}

int RecursiveScan2RecursionLevel_ObserveDirectory(RecursiveScan2RecursionLevel o, PathStorage path, WinAPICallWrapperStats win_api_stats) {
  int getlasterror;
  WinAPICallWrapperMacro(FindFirstFileW, (path->wide, &o->findData), &o->findHandle, win_api_stats, &getlasterror, 
    if(getlasterror == ERROR_INVALID_NAME) {
      o->findHandle = INVALID_HANDLE_VALUE;
      printf("ERROR_INVALID_NAME:'%ls'\n", path->wide);
      goto exit_with_warning;
    }
    if(getlasterror == ERROR_CANT_ACCESS_FILE) {
      o->findHandle = INVALID_HANDLE_VALUE;
      printf("ERROR_CANT_ACCESS_FILE:'%ls'\n", path->wide);
      goto exit_with_warning;
    }           
    if(getlasterror == ERROR_PATH_NOT_FOUND) {
      printf("ERROR_PATH_NOT_FOUND:'%ls'\n", path->wide);
    }    
    goto_exit_with_failure(&getlasterror);
  )
  return 0;
exit_with_warning:;
  return 0;
exit_with_failure:;
  return 1;
}

int RecursiveScan2RecursionLevel_Next(RecursiveScan2RecursionLevel o, WinAPICallWrapperStats win_api_stats) 
{
  if(o->iter_ok) {
    /*
    https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-findnextfilew
    NOTE: The order in which the search returns the files, such as alphabetical order, is not guaranteed, and is dependent on the file system.
    BUT : For the most the files **are** sorted!!
    */
    WinAPICallWrapperMacro(FindNextFileW, (o->findHandle, &o->findData), &o->iter_ok, win_api_stats, NULL, NULL);
  }
  return 0;
exit_with_failure:;
  o->iter_ok = 0;
  return 1;
}

int RecursiveScan2RecursionLevel_First(RecursiveScan2RecursionLevel o, WinAPICallWrapperStats win_api_stats) 
{
  if (o->findHandle == INVALID_HANDLE_VALUE) { 
    o->iter_ok = 0;
  } else {
    o->iter_ok = 1;    
    do {
      if (wcscmp(o->findData.cFileName, L".") != 0 && wcscmp(o->findData.cFileName, L"..") != 0) {
        break;
      }
      if(RecursiveScan2RecursionLevel_Next(o, win_api_stats)) {
        goto_exit_with_failure(NULL);
      }      
    } while(o->iter_ok);
  }  
  return 0;
exit_with_failure:;
  return 1;
}

void RecursiveScan2_Free(RecursiveScan2 o) {
  PathStorage_Free(&o->path);
  WinAPICallWrapperStats_Free(&o->win_api_stats);
  memset(o, 0, sizeof(*o));
}

int RecursiveScan2_Init(RecursiveScan2 o, wchar_t * root_dir) {
  size_t strlen_s;
  memset(o, 0, sizeof(*o));
  if(PathStorage_SafeAppendStringToPath(&o->path, 0, root_dir, &strlen_s)) {
    goto_exit_with_failure(NULL);
  }
  if(WinAPICallWrapperStats_Init(&o->win_api_stats)) {
    goto_exit_with_failure(NULL);
  }
  return 0;
exit_with_failure:;
  RecursiveScan2_Free(o);
  return 1;  
}

int RecursiveScan2_SetupFolder(RecursiveScan2 o, size_t * strlen_path, RecursiveScan2RecursionLevel rl) {
  size_t strlen_s;
  // Assume the user has already called RecursiveScan2RecursionLevel_Init on rl
  if(PathStorage_SafeAppendPathSep(&o->path, strlen_path)) {
    goto_exit_with_failure(NULL);
  }
  if(PathStorage_SafeAppendStringToPath(&o->path, *strlen_path, L"*", &strlen_s)) {
    goto_exit_with_failure(NULL);
  }  
  if(RecursiveScan2RecursionLevel_ObserveDirectory(rl, &o->path, &o->win_api_stats)) {
    goto_exit_with_failure(NULL);    
  }
  // Knock off the '*' we added above 
  PathStorage_SetNull(&o->path, *strlen_path);
  return 0;
exit_with_failure:;
  return 1;   
}

int RecursiveScan2_Recurse(RecursiveScan2 o, size_t strlen_path) {
  struct RecursiveScan2RecursionLevel_s rl_s, * const rl = &rl_s;
  RecursiveScan2RecursionLevel_Init(rl, &o->win_api_stats);
  if(RecursiveScan2_SetupFolder(o, &strlen_path, rl)) {
    goto_exit_with_failure(NULL);
  }
  if(RecursiveScan2RecursionLevel_First(rl, &o->win_api_stats)) {
    goto_exit_with_failure(NULL);
  } 
  o->cbRet = 0;  
  if(rl->iter_ok) {    
    do {
      size_t strlen_s;
      wchar_t * name = RecursiveScan2RecursionLevel_CurrentName(rl);
      int is_dir = RecursiveScan2RecursionLevel_CurrentIsDir(rl);
      __int64 size = RecursiveScan2RecursionLevel_CurrentFileSize(rl);
      __int64 modt = RecursiveScan2RecursionLevel_CurrentModifiedTime(rl);
      if (!is_dir) {
        if(o->cbVisitFile) {
          if(o->cbRet = o->cbVisitFile(o->cbCtx, o, rl, &o->path, strlen_path, name, size, modt)) goto exit_early;
        }
      } else {
        if (PathStorage_SafeAppendStringToPath(&o->path, strlen_path, name, &strlen_s)) {
          goto_exit_with_failure(NULL);
        }
#ifdef _DEBUG    
        if(o->path.wide[strlen_path - 1] != '\\') {
          // See 2020_10_20_09_47
          THROWX("o->path.wide[strlen_path - 1] != '\\'", 1)
        }
        if(wcscmp(&o->path.wide[strlen_path], name) != 0) {
          THROWX("strcmp(&o->path.wide[strlen_path], name) != 0", 1)
        }
#endif                  
        if(o->cbBeforeRecursion) {
          int user_signal;
          o->cbRet = o->cbBeforeRecursion(o->cbCtx, o, rl, &o->path, strlen_path + strlen_s, strlen_s, &user_signal);
          if(o->cbRet) goto exit_early;
        }          
        if (RecursiveScan2_Recurse(o, strlen_path + strlen_s)) {
          goto_exit_with_failure(NULL);
        }
        if(o->cbRet) goto exit_early;        
#ifdef _DEBUG    
        if(o->path.wide[strlen_path - 1] != '\\') {
          // See 2020_10_20_09_47
          THROWX("o->path.wide[strlen_path - 1] != '\\'", 1)
        }
        if(memcmp(&o->path.wide[strlen_path], name, sizeof(*o->path.wide) * (strlen_s - 1)) != 0) {
          THROWX("memcmp(&o->path.wide[strlen_path], name, sizeof(*o->path.wide) * (strlen_s - 1)) != 0", 1)
        }        
        if(o->path.wide[strlen_path + strlen_s] != '\\') {
          // See 2020_10_20_09_47
          THROWX("o->path.wide[strlen_path + strlen_s] != '\\'", 1)
        }
#endif         
        if(o->cbAfterRecursion) {
          PathStorage_SetNull(&o->path, strlen_path + strlen_s); // Knock off the trailing path delimiter for the user
          if(o->cbRet = o->cbAfterRecursion(o->cbCtx, o, rl, &o->path, strlen_path + strlen_s, strlen_s)) goto exit_early;
        }         
        PathStorage_SetNull(&o->path, strlen_path); // Restore the null-terminator for the current directory name
      }
      if(RecursiveScan2RecursionLevel_Next(rl, &o->win_api_stats)) {
        goto_exit_with_failure(NULL);
      }
    } while (rl->iter_ok);     
  }
exit_early:;
  if(o->cbRet < 0) o->cbRet = 0; // If the callbacks return < 0 then we just skip the current recursion
  RecursiveScan2RecursionLevel_Free(rl, &o->win_api_stats);
  return 0;
exit_with_failure:;
  RecursiveScan2RecursionLevel_Free(rl, &o->win_api_stats);
  return 1;  
}

int CopyDirRecursion_cbRecursiveScan2_VisitFile(void *cbCtx, RecursiveScan2 src, RecursiveScan2RecursionLevel rl, PathStorage path, size_t strlen_path, wchar_t *name, __int64 size, __int64 modt) {
  CopyDirRecursion ctx = cbCtx;
  int ret;
  size_t strlen_s;
#ifdef _DEBUG   
  if (&src->path != path) {
    THROWX("&src->path != path", 1)
  }
  if(src->path.wide[strlen_path - 1] != '\\') {
    THROWX("src->path.wide[strlen_path - 1] != '\\'", 1)
  }  
  if(ctx->dst_ref->path.wide[ctx->strlen_path_dst - 1] != '\\') {
    THROWX("dst->path.wide[strlen_path_dst - 1] != '\\'", 1)
  }  
#endif   
  if(PathStorage_SafeAppendStringToPath(&src->path, strlen_path, name, &strlen_s)) {
    goto_exit_with_failure(NULL);
  }
  if(PathStorage_SafeAppendStringToPath(&ctx->dst_ref->path, ctx->strlen_path_dst, name, &strlen_s)) {
    goto_exit_with_failure(NULL);
  }
  ctx->tfss.SourceBytes.QuadPart += size; 
  ctx->tfss.Files.QuadPart++;  
  if(ctx->skip_copy) {
    ctx->tfss.CopyBytes.QuadPart += size;      
  } else {
    TotalFileSystemStats_cbCopyProgress_Bef(&ctx->tfss);
    if(WinAPICallWrapperStats_CopyFile(&ctx->win_api_stats, TotalFileSystemStats_cbCopyProgress, &ctx->tfss, src->path.wide, ctx->dst_ref->path.wide, size, &ctx->tfss.CopyBytes.QuadPart)) {
      TotalFileSystemStats_cbCopyProgress_Aft(&ctx->tfss);
      goto_exit_with_failure(NULL);
    }
    TotalFileSystemStats_cbCopyProgress_Aft(&ctx->tfss);
  }
exit_with_warning:;           
  PathStorage_SetNull(&src->path, strlen_path); 
  PathStorage_SetNull(&ctx->dst_ref->path, ctx->strlen_path_dst); 
  return 0;  
exit_with_failure:;
  return 1;    
}

int CopyDirRecursion_cbRecursiveScan2_BeforeRecursion(void *cbCtx, RecursiveScan2 src, RecursiveScan2RecursionLevel rl, PathStorage path, size_t strlen_path, size_t strlen_last_folder_s, int *folder_already_exists) {
  CopyDirRecursion ctx = cbCtx;
  int res, getlasterror;
  size_t strlen_s;
  wchar_t *src_dir_name_wide = path->wide + strlen_path - strlen_last_folder_s;
#ifdef _DEBUG   
  if(path->wide[strlen_path - strlen_last_folder_s - 1] != '\\') {
    THROWX("dst->path.wide[strlen_path_dst - 1] != '\\'", 1)
  }  
  if(ctx->dst_ref->path.wide[ctx->strlen_path_dst - 1] != '\\') {
    THROWX("dst->path.wide[strlen_path_dst - 1] != '\\'", 1)
  }  
#endif  
  *folder_already_exists = 0;
  if(PathStorage_SafeAppendStringToPath(&ctx->dst_ref->path, ctx->strlen_path_dst, src_dir_name_wide, &strlen_s)) {
    goto_exit_with_failure(NULL);
  }       
  
  if(!ctx->skip_copy) {
    WinAPICallWrapperMacro(CreateDirectoryExW, (src->path.wide, ctx->dst_ref->path.wide, NULL), &res, &ctx->win_api_stats, &getlasterror, 
      if(getlasterror == ERROR_INVALID_NAME) {
        printf("ERROR_INVALID_NAME:'%ls'\n", src->path.wide);
        goto exit_with_skip_recursion;
      }
      if(getlasterror == ERROR_ACCESS_DENIED) {
        printf("ERROR_ACCESS_DENIED:'%ls'\n", src->path.wide);
        goto exit_with_skip_recursion;
      }      
      if(getlasterror == ERROR_ALREADY_EXISTS) {
        *folder_already_exists = 1;
        printf("ERROR_ALREADY_EXISTS:'%ls'\n", src->path.wide);
        goto exit_with_skip_recursion;
      }
      printf("getlasterror=%d:'%ls':'%ls'\n", getlasterror, src->path.wide, ctx->dst_ref->path.wide);
      goto_exit_with_failure(&getlasterror);
    )
  }

  ctx->strlen_path_dst += strlen_s;
  if(PathStorage_SafeAppendPathSep(&ctx->dst_ref->path, &ctx->strlen_path_dst)) {
    goto_exit_with_failure(NULL);
  }
  return 0;  
exit_with_failure:;
  return 1;  
exit_with_skip_recursion:;
  return -1;  
}

int CopyDirRecursion_cbRecursiveScan2_AfterRecursion(void *cbCtx, RecursiveScan2 src, RecursiveScan2RecursionLevel rl, PathStorage path, size_t strlen_path, size_t strlen_last_folder_s) {
  CopyDirRecursion ctx = cbCtx;
#ifdef _DEBUG   
  if(ctx->dst_ref->path.wide[ctx->strlen_path_dst - 1] != '\\') {
    THROWX("dst->path.wide[strlen_path_dst - 1] != '\\'", 1)
  }  
#endif   
  ctx->strlen_path_dst--;
  while(ctx->dst_ref->path.wide[ctx->strlen_path_dst - 1] != '\\') ctx->strlen_path_dst--;
  PathStorage_SetNull(&ctx->dst_ref->path, ctx->strlen_path_dst); 
  //ctx->dst_ref->path.wide[ctx->strlen_path_dst] = '\0';
  return 0;  
exit_with_failure:;
  return 1;      
}

void CopyDirRecursion_Free(CopyDirRecursion o) {
  WinAPICallWrapperStats_Free(&o->win_api_stats);
  memset(o, 0, sizeof(*o));
}

int CopyDirRecursion_Init(CopyDirRecursion o) {
  memset(o, 0, sizeof(*o));
  if(WinAPICallWrapperStats_Init(&o->win_api_stats)) {
    goto_exit_with_failure(NULL);
  }  
  return 0;  
exit_with_failure:;
  CopyDirRecursion_Free(o);
  return 1;     
}

int CopyDirRecursion_ResetStats(CopyDirRecursion o) {
  if(WinAPICallWrapperStats_Reset(&o->win_api_stats)) {
      goto_exit_with_failure(NULL);
  }
  if(TotalFileSystemStats_Reset(&o->tfss)) {
    goto_exit_with_failure(NULL);    
  }  
  return 0;
exit_with_failure:;
  return 1;  
}

int CopyDirRecursion_Run(CopyDirRecursion o, RecursiveScan2 src, size_t strlen_path_src, RecursiveScan2 dst, size_t strlen_path_dst, wchar_t * src_dir_name, int * folder_already_exists) {
  // src_dir_name is in src but assumed to not be in dst
  size_t strlen_s, strlen_x_s; 
  size_t strlen_path_src_o = strlen_path_src;
  size_t strlen_path_dst_o = strlen_path_dst;
  void *cbCtx_bak = src->cbCtx;
  void *cbVisitFile_bak = src->cbVisitFile;
  void *cbBeforeRecursion_bak = src->cbBeforeRecursion;
  void *cbAfterRecursion_bak = src->cbAfterRecursion;  
  int ret;
  
  if(PathStorage_SafeAppendPathSep(&src->path, &strlen_path_src)) {
    goto_exit_with_failure(NULL);
  }
  if(PathStorage_SafeAppendPathSep(&dst->path, &strlen_path_dst)) {
    goto_exit_with_failure(NULL);
  }  
  if(PathStorage_SafeAppendStringToPath(&src->path, strlen_path_src, src_dir_name, &strlen_s)) {
    goto_exit_with_failure(NULL);
  }
  
  o->dst_ref = dst;
  o->strlen_path_dst = strlen_path_dst;
  if(ret = CopyDirRecursion_cbRecursiveScan2_BeforeRecursion(o, src, NULL, &src->path, strlen_path_src + strlen_s, strlen_s, folder_already_exists)) {
    if(ret < 0) {
      goto exit_with_skip_recursion;
    } else {
      goto_exit_with_failure(NULL);
    }
  }
  
  // Run the recursion on src
  src->cbCtx = o;
  src->cbVisitFile = CopyDirRecursion_cbRecursiveScan2_VisitFile;
  src->cbBeforeRecursion = CopyDirRecursion_cbRecursiveScan2_BeforeRecursion;
  src->cbAfterRecursion = CopyDirRecursion_cbRecursiveScan2_AfterRecursion;  
  if(RecursiveScan2_Recurse(src, strlen_path_src + strlen_s)) {
    goto_exit_with_failure(NULL);
  }
  
  if(CopyDirRecursion_cbRecursiveScan2_AfterRecursion(o, src, NULL, &src->path, strlen_path_src + strlen_s, strlen_s)) {
    goto_exit_with_failure(NULL);
  }  

exit_with_skip_recursion:;
  
  PathStorage_SetNull(&src->path, strlen_path_src_o); 
  PathStorage_SetNull(&dst->path, strlen_path_dst_o); 
  
  src->cbCtx = cbCtx_bak;
  src->cbVisitFile = cbVisitFile_bak;
  src->cbBeforeRecursion = cbBeforeRecursion_bak;
  src->cbAfterRecursion = cbAfterRecursion_bak;    
  return 0;
exit_with_failure:;
  src->cbCtx = cbCtx_bak;
  src->cbVisitFile = cbVisitFile_bak;
  src->cbBeforeRecursion = cbBeforeRecursion_bak;
  src->cbAfterRecursion = cbAfterRecursion_bak;  
  return 1;   
}

int RecursiveMirrorScan2_Recurse(RecursiveMirrorScan2 o, RecursiveScan2 src, size_t strlen_path_src, RecursiveScan2 dst, size_t strlen_path_dst) {
  struct RecursiveScan2RecursionLevel_s rl_src_s, * const rl_src = &rl_src_s;
  struct RecursiveScan2RecursionLevel_s rl_dst_s, * const rl_dst = &rl_dst_s;
  size_t strlen_s, common_subdirs_count = 0, possibly_missing_subdirs_count = 0;
  int ret;
  
  RecursiveScan2RecursionLevel_Init(rl_src, &src->win_api_stats);
  RecursiveScan2RecursionLevel_Init(rl_dst, &dst->win_api_stats);
  
  if(RecursiveScan2_SetupFolder(src, &strlen_path_src, rl_src)) {
    goto_exit_with_failure(NULL);
  }
  if(RecursiveScan2_SetupFolder(dst, &strlen_path_dst, rl_dst)) {
    goto_exit_with_failure(NULL);
  }  
  
  if(RecursiveScan2RecursionLevel_First(rl_src, &src->win_api_stats)) {
    goto_exit_with_failure(NULL);
  } 
  if(RecursiveScan2RecursionLevel_First(rl_dst, &dst->win_api_stats)) {
    goto_exit_with_failure(NULL);
  }   
  
  /* Parallel scan the two directories and build lists of the missing/common items in destination */  
  if(rl_src->iter_ok) {
    do {
      wchar_t * name_dst, * name_src = RecursiveScan2RecursionLevel_CurrentName(rl_src);
      int is_dir_dst, is_dir_src = RecursiveScan2RecursionLevel_CurrentIsDir(rl_src);
      __int64 size_src = RecursiveScan2RecursionLevel_CurrentFileSize(rl_src);
      int cmp, possibly_missing_in_dst;
      
      possibly_missing_in_dst = 1; // We say "possibly" because RecursiveScan2RecursionLevel_Next may not be **entirely** alphabetically ordered
      if(rl_dst->iter_ok) {
        do {
          name_dst = RecursiveScan2RecursionLevel_CurrentName(rl_dst);
          cmp = wcsicmp(name_dst, name_src);
          if(cmp >= 0) {
            // name_dst is alphabetically the same or after name_src  ... if it is after then name_dst is missing in src
            if(cmp == 0) {
              possibly_missing_in_dst = 0;
              is_dir_dst = RecursiveScan2RecursionLevel_CurrentIsDir(rl_dst);
            }
            break;
          }
          if(RecursiveScan2RecursionLevel_Next(rl_dst, &dst->win_api_stats)) {
            goto_exit_with_failure(NULL);
          }
        } while (rl_dst->iter_ok);
      }
      
      #define RecursiveMirrorScan2_Common(HANDLER_CODE) \
      { \
        if(PathStorage_SafeAppendStringToPath(&src->path, strlen_path_src, name_src, &strlen_s)) { \
          goto_exit_with_failure(NULL); \
        } \
        if(PathStorage_SafeAppendStringToPath(&dst->path, strlen_path_dst, name_src, &strlen_s)) { \
          goto_exit_with_failure(NULL); \
        } \
        HANDLER_CODE; \
        PathStorage_SetNull(&src->path, strlen_path_src); \
        PathStorage_SetNull(&dst->path, strlen_path_dst); \
      }
    
      if(possibly_missing_in_dst) {
        if(is_dir_src) {
          // Missing directory (possibly)
          if(BytesStack_PushWChar(o->possibly_missing_subdirs_stack, name_src)) {
            goto_exit_with_failure(NULL);
          }          
          possibly_missing_subdirs_count++;
        } else {
          // Missing file (possibly)
          if(o->cbPossiblyMissingFile) {
            RecursiveMirrorScan2_Common(
              // CALLBACK  
              ret = o->cbPossiblyMissingFile(o->cbCtx, o, src, strlen_path_src, dst, strlen_path_dst, size_src);
              if(ret) {
                goto_exit_with_failure(NULL);
              }                    
            )
          }
        }
      } else {
        __int64 size_dst = RecursiveScan2RecursionLevel_CurrentFileSize(rl_dst);
        if(is_dir_dst != is_dir_src) {
          // The item in the source is a file but it is a directory in the destination, or vice versa.
          if(o->cbMismatchedFileNameAndDirName) {
            // CALLBACK  
            ret = o->cbMismatchedFileNameAndDirName(o->cbCtx, o, src, strlen_path_src, dst, strlen_path_dst, is_dir_src, is_dir_dst, size_src, size_dst);
            if(ret) {
              goto_exit_with_failure(NULL);
            }                    
          }
        } else {
          if(is_dir_dst) {
            // Common directory
            if(BytesStack_PushWChar(o->common_subdirs_stack, name_src)) {
              goto_exit_with_failure(NULL);
            }
            common_subdirs_count++;
          } else {
            // Common file ... check for modified
            __int64 modt_src = RecursiveScan2RecursionLevel_CurrentModifiedTime(rl_src);
            __int64 modt_dst = RecursiveScan2RecursionLevel_CurrentModifiedTime(rl_dst);
            if(o->cbCommonFile) {
              RecursiveMirrorScan2_Common(
                // CALLBACK  
                ret = o->cbCommonFile(o->cbCtx, o, src, strlen_path_src, dst, strlen_path_dst, size_src, size_dst, modt_src, modt_dst);
                //printf("c %s %s %3lld %3lld\n", name_src, name_dst, size_src, size_dst, modt_src, modt_dst);
                if(ret) {
                  goto_exit_with_failure(NULL);
                }                    
              )            
            }
          }     
          if(RecursiveScan2RecursionLevel_Next(rl_dst, &dst->win_api_stats)) {
            goto_exit_with_failure(NULL);
          }
        }
      }
      
      if(RecursiveScan2RecursionLevel_Next(rl_src, &src->win_api_stats)) {
        goto_exit_with_failure(NULL);
      }
    } while (rl_src->iter_ok);
  }
  
  // Free resources before we recurse
  RecursiveScan2RecursionLevel_Free(rl_src, &src->win_api_stats);
  RecursiveScan2RecursionLevel_Free(rl_dst, &dst->win_api_stats);  
  
  // Iterate the directories that are missing in the destination
  while(possibly_missing_subdirs_count > 0) {
    wchar_t * name_mis;
    possibly_missing_subdirs_count--;
    if(!BytesStack_Pop(o->possibly_missing_subdirs_stack, &name_mis)) {
      goto_exit_with_failure(NULL);
    }    
    if(o->cbPossiblyMissingDir) {
      // CALLBACK 
      int folder_already_exists;
      ret = o->cbPossiblyMissingDir(o->cbCtx, o, src, strlen_path_src, dst, strlen_path_dst, name_mis, &folder_already_exists);
      if(ret) {
        goto_exit_with_failure(NULL);
      } 
      if(folder_already_exists) {
        if(BytesStack_PushWChar(o->common_subdirs_stack, name_mis)) {
          goto_exit_with_failure(NULL);
        }
        common_subdirs_count++;      
      }
    }
  }
  
  // Recurse into the common directories
  while(common_subdirs_count > 0) {
    wchar_t * name_com;
    common_subdirs_count--;
    if(!BytesStack_Pop(o->common_subdirs_stack, &name_com)) {
      goto_exit_with_failure(NULL);
    }
    
    if (PathStorage_SafeAppendStringToPath(&src->path, strlen_path_src, name_com, &strlen_s)) {
      goto_exit_with_failure(NULL);
    }
    if (PathStorage_SafeAppendStringToPath(&dst->path, strlen_path_dst, name_com, &strlen_s)) {
      goto_exit_with_failure(NULL);
    }      
    
    if (RecursiveMirrorScan2_Recurse(o, src, strlen_path_src + strlen_s, dst, strlen_path_dst + strlen_s)) {
      goto_exit_with_failure(NULL);
    }
    
#ifdef _DEBUG    
    if(src->path.wide[strlen_path_src + strlen_s] != '\\') {
      // See 2020_10_20_09_47
      THROWX("src->path.wide[strlen_path_src + strlen_s] != '\\'", 1)
    }
#endif    
    
    PathStorage_SetNull(&src->path, strlen_path_src); // Restore the null-terminator for the current directory name
    PathStorage_SetNull(&dst->path, strlen_path_dst); // Restore the null-terminator for the current directory name
  }
  
  return 0;
exit_with_failure:;
  RecursiveScan2RecursionLevel_Free(rl_src, &src->win_api_stats);
  RecursiveScan2RecursionLevel_Free(rl_dst, &dst->win_api_stats);
  return 1;  
}

int xCompareFilesAreIndentical(wchar_t *f1, wchar_t *f2, char **comp_buff1, char **comp_buff2, size_t *comp_buff1_max_size, size_t *comp_buff2_max_size, int *files_are_identical, LONGLONG * CompareReadBytes) {
  FILE *ff1 = NULL;
  FILE *ff2 = NULL;
  size_t n1, n2;
  *files_are_identical = 1;
  #define CompareFilesAreIndentical_BUFF_BYTES (10 * 1024 * 1024)
  GenericGrowMacro1(char, comp_buff1, comp_buff1_max_size, CompareFilesAreIndentical_BUFF_BYTES, CompareFilesAreIndentical_BUFF_BYTES)
  GenericGrowMacro1(char, comp_buff2, comp_buff2_max_size, CompareFilesAreIndentical_BUFF_BYTES, CompareFilesAreIndentical_BUFF_BYTES)
  if(!(ff1 = _wfopen(f1, L"rb"))) {
    goto_exit_with_failure(NULL);
  }
  if(!(ff2 = _wfopen(f2, L"rb"))) {
    goto_exit_with_failure(NULL);
  }
  while(!feof(ff1) && !feof(ff2)) {
    n1 = fread(*comp_buff1, 1, *comp_buff1_max_size, ff1);
    n2 = fread(*comp_buff2, 1, *comp_buff2_max_size, ff2);
    *CompareReadBytes += n1 + n2;    
    if(n1 != n2) {
      *files_are_identical = 0;
      break;
    }
    if(n1 == 0) break;    
    if(memcmp(*comp_buff1, *comp_buff2, n1) != 0) {
      *files_are_identical = 0;
      break;      
    }
  }
  if(ff1) fclose(ff1); ff1 = NULL;
  if(ff2) fclose(ff2); ff2 = NULL;
  return 0;
exit_with_failure:;
  if(ff1) fclose(ff1); ff1 = NULL;
  if(ff2) fclose(ff2); ff2 = NULL;
  return 1;    
}

int SyncSrc2Dst_CopyFile(SyncSrc2Dst o, wchar_t *src_file, wchar_t *dst_file, __int64 size_src, __int64 size_dst) {
  TotalFileSystemStats_cbCopyProgress_Bef(&o->tfss);
  if(WinAPICallWrapperStats_CopyFile(&o->win_api_stats, TotalFileSystemStats_cbCopyProgress, &o->tfss, src_file, dst_file, size_src, &o->tfss.CopyBytes.QuadPart)) {
    TotalFileSystemStats_cbCopyProgress_Aft(&o->tfss);
    goto_exit_with_failure(NULL);
  }
  TotalFileSystemStats_cbCopyProgress_Aft(&o->tfss);
  return 0;
exit_with_failure:;
  return 1;    
}

int SyncSrc2Dst_cbRecursiveMirrorScan2_PossiblyMissingDir(void * cbCtx, struct RecursiveMirrorScan2_s * o, struct RecursiveScan2_s * src, size_t strlen_path_src, struct RecursiveScan2_s * dst, size_t strlen_path_dst, wchar_t * src_dir_name, int * folder_already_exists) {
  SyncSrc2Dst ctx = cbCtx;
  if(CopyDirRecursion_Run(&ctx->cdr, src, strlen_path_src, dst, strlen_path_dst, src_dir_name, folder_already_exists)) {
    goto_exit_with_failure(NULL);
  }
  return 0;
exit_with_failure:;
  return 1;   
}

int SyncSrc2Dst_cbRecursiveMirrorScan2_CommonFile(void * cbCtx, struct RecursiveMirrorScan2_s * o, struct RecursiveScan2_s * src, size_t strlen_path_src, struct RecursiveScan2_s * dst, size_t strlen_path_dst, __int64 size_src, __int64 size_dst, __int64 modt_src, __int64 modt_dst) {
  SyncSrc2Dst ctx = cbCtx;    
  int ret = 0, getlasterror, skip_copy_local = 0;
  char buffer_src[1 + SYSTEMTIME_fields(SYSTEMTIME_fields_bytes)];
  char buffer_dst[1 + SYSTEMTIME_fields(SYSTEMTIME_fields_bytes)];

    /*
  double CopyRate_MebiPerSec = 30 * 1024 * 1024;
  double estimated_copy_secs = size_src / CopyRate_MebiPerSec;
  */
  
  double modt_diff_secs = (double) (modt_dst - (modt_src)) / 10000000;
  
  ctx->tfss.SourceBytes.QuadPart += size_src;
  ctx->tfss.Files.QuadPart++;
  
  if(size_src != size_dst) {
    if(modt_diff_secs >= 0.0) {
      /* The destination has been modified AFTER the source ... don't overwrite the destination */
      if(modt_diff_secs > 0.0) {
        /* Warn the user that this file isn't being backed up */
        printf("A:modt_src < modt_dst:TortoiseGitMerge.exe %4.2g %s %s \"%ls\" \"%ls\"\n", modt_diff_secs, FILETIME_format((FILETIME *) &modt_src, buffer_src), FILETIME_format((FILETIME *) &modt_dst, buffer_dst), src->path.wide, dst->path.wide);
      }
      skip_copy_local = 1;
    }    
  } else {
    if(ctx->use_modified_times_otherwise_compare_files) {  
      if(modt_diff_secs >= 0.0) {
        /* The destination has been modified AFTER the source ... don't overwrite the destination */
        if(modt_diff_secs > 0.0) {
          /* Warn the user that this file isn't being backed up */
          printf("B:modt_src < modt_dst:TortoiseGitMerge.exe %8.2g %s %s \"%ls\" \"%ls\"\n", modt_diff_secs, FILETIME_format((FILETIME *) &modt_src, buffer_src), FILETIME_format((FILETIME *) &modt_dst, buffer_dst), src->path.wide, dst->path.wide);
        }
        skip_copy_local = 1;
      }
    } else {
      if(!ctx->pfc) {
        WinAPICallWrapperMacro(xCompareFilesAreIndentical, (src->path.wide, dst->path.wide, &ctx->comp_buff1, &ctx->comp_buff2, &ctx->comp_buff1_max_size, &ctx->comp_buff2_max_size, &skip_copy_local, &ctx->tfss.CompareReadBytes.QuadPart), &ret, &ctx->win_api_stats, NULL, 
          goto_exit_with_failure(NULL);  
        )
      } else {
        struct ParallelFileComparerJob_s job = {0};
        skip_copy_local = 1; // We're going to be the doing the copy in parallel fron within the ParallelFileComparer framework
        job.path1 = src->path.wide;
        job.path2 = dst->path.wide;
        job.size1 = size_src;
        job.size2 = size_dst;
        if(ParallelFileComparer_QueueJob(ctx->pfc, &job)) {
          goto_exit_with_failure(NULL);  
        }
      }
    }
  }
 
  if(!skip_copy_local) {
    if(ctx->skip_copy) {
      ctx->tfss.CopyBytes.QuadPart += size_src;  
    } else {
      if(SyncSrc2Dst_CopyFile(ctx, src->path.wide, dst->path.wide, size_src, size_dst)) {
        goto_exit_with_failure(NULL);  
      }
    }
  }
  
exit_with_warning:;
  return 0;
exit_with_failure:;
  return 1;   
}

int SyncSrc2Dst_cbRecursiveMirrorScan2_PossiblyMissingFile(void * cbCtx, struct RecursiveMirrorScan2_s * o, struct RecursiveScan2_s * src, size_t strlen_path_src, struct RecursiveScan2_s * dst, size_t strlen_path_dst, __int64 size_src) {
  SyncSrc2Dst ctx = cbCtx;    
  ctx->tfss.SourceBytes.QuadPart += size_src;
  ctx->tfss.Files.QuadPart++;
  if(ctx->skip_copy) {
    ctx->tfss.CopyBytes.QuadPart += size_src;
  } else {
    if(SyncSrc2Dst_CopyFile(ctx, src->path.wide, dst->path.wide, size_src, 0)) {
      goto_exit_with_failure(NULL);  
    }    
  }
  return 0;
exit_with_failure:;
  return 1;   
}

int SyncSrc2Dst_cbRecursiveMirrorScan2_MismatchedFileNameAndDirName(void * cbCtx, struct RecursiveMirrorScan2_s * o, struct RecursiveScan2_s * src, size_t strlen_path_src, struct RecursiveScan2_s * dst, size_t strlen_path_dst, int is_dir_src, int is_dir_dst, __int64 size_src, __int64 size_dst) {
  SyncSrc2Dst ctx = cbCtx;  
  return 0;
exit_with_failure:;
  return 1;   
}

void SyncSrc2Dst_Free(SyncSrc2Dst o) {
  CopyDirRecursion_Free(&o->cdr);  
  WinAPICallWrapperStats_Free(&o->win_api_stats);
  TotalFileSystemStats_Free(&o->tfss);
  if(o->comp_buff1) free(o->comp_buff1);
  if(o->comp_buff2) free(o->comp_buff2);
  RateSmoother_Free(&o->rs_1);
  RateSmoother_Free(&o->rs_2);
  RateSmoother_Free(&o->rs_3);
  memset(o, 0, sizeof(*o));
}

int SyncSrc2Dst_Init(SyncSrc2Dst o) {
  const int window_size = 3;
  memset(o, 0, sizeof(*o));
  if(CopyDirRecursion_Init(&o->cdr)) {
    goto_exit_with_failure(NULL);
  }
  if(WinAPICallWrapperStats_Init(&o->win_api_stats)) {
    goto_exit_with_failure(NULL);
  }  
  if(TotalFileSystemStats_Init(&o->tfss)) {
    goto_exit_with_failure(NULL);    
  }  
  if(RateSmoother_Init(&o->rs_1, window_size)) {
    goto_exit_with_failure(NULL);
  }
  if(RateSmoother_Init(&o->rs_2, window_size)) {
    goto_exit_with_failure(NULL);
  }
  if(RateSmoother_Init(&o->rs_3, window_size)) {
    goto_exit_with_failure(NULL);
  }
  return 0;  
exit_with_failure:;
  return 1;     
}

int SyncSrc2Dst_Reset(SyncSrc2Dst o) {
  if(CopyDirRecursion_ResetStats(&o->cdr)) {
    goto_exit_with_failure(NULL);    
  }  
  if(WinAPICallWrapperStats_Reset(&o->win_api_stats)) {
    goto_exit_with_failure(NULL);    
  }
  if(TotalFileSystemStats_Reset(&o->tfss)) {
    goto_exit_with_failure(NULL);    
  }
  if(RateSmoother_Reset(&o->rs_1)) {
    goto_exit_with_failure(NULL);
  }
  if(RateSmoother_Reset(&o->rs_2)) {
    goto_exit_with_failure(NULL);
  }
  if(RateSmoother_Reset(&o->rs_3)) {
    goto_exit_with_failure(NULL);
  }
  return 0;  
exit_with_failure:;
  return 1;    
}

int SyncSrc2Dst_cbHeartbeatThread_Callback(void * cbCtx, struct HeartbeatThread_s * o, unsigned int call_count) {
  SyncSrc2Dst ctx = cbCtx;  
  struct WinAPICallWrapperStats_Elem_s e_dirs;
  struct WinAPICallWrapperStats_Elem_s e_file;
  struct WinAPICallWrapperStats_Elem_s e_copy;
  struct WinAPICallWrapperStats_Elem_s e_comp;
  double secs;
  const int width1 = 4 + 1 + 5 + 5;
  const int width2 = 8;
  const int width3 = 4 + 1 + 5 + 1;
  double rate_1, rate_2, rate_3;
  int rate_undefined_1, rate_undefined_2, rate_undefined_3;
  char buff1[128];
  char buff2[128];
  char buff3[128];
  char buff4[128];
  char buff5[128];
  char buff6[128];
  char buff7[128];
  char buff8[128];
  struct TotalFileSystemStats_s tfss_accum = {0};
  
  QueryPerformanceCounter_End(ctx->qpfc_beg, &secs, NULL, NULL, NULL);    
  
  #define z2020_11_06_16_40(e_, match_name) \
  { \
    int skip_initialize_2_zero = 0; \
    WinAPICallWrapperStats_Sum(&ctx->win_api_stats          , &(e_), skip_initialize_2_zero, match_name); skip_initialize_2_zero = 1; \
    if(ctx->src_ref) \
    WinAPICallWrapperStats_Sum(&ctx->src_ref->win_api_stats , &(e_), skip_initialize_2_zero, match_name); skip_initialize_2_zero = 1; \
    if(0 && ctx->dst_ref) \
    WinAPICallWrapperStats_Sum(&ctx->dst_ref->win_api_stats , &(e_), skip_initialize_2_zero, match_name); skip_initialize_2_zero = 1; \
    WinAPICallWrapperStats_Sum(&ctx->cdr.win_api_stats      , &(e_), skip_initialize_2_zero, match_name); skip_initialize_2_zero = 1; \
    if(ctx->pfc) { \
      ParallelFileComparer_IterateStats(ctx->pfc,  \
        WinAPICallWrapperStats_Sum(win_api_stats_iter       , &(e_), skip_initialize_2_zero, match_name); skip_initialize_2_zero = 1; \
      ) \
    } \
  }
  
  if(TotalFileSystemStats_Init(&tfss_accum)) {
    goto_exit_with_failure(NULL);
  }  
  if(TotalFileSystemStats_Add(&tfss_accum, &ctx->tfss)) {
    goto_exit_with_failure(NULL);
  }
  if(TotalFileSystemStats_Add(&tfss_accum, &ctx->cdr.tfss)) {
    goto_exit_with_failure(NULL);
  }  
  if(ctx->pfc) {
    ParallelFileComparer_IterateStats(ctx->pfc,
      if(TotalFileSystemStats_Add(&tfss_accum, tfss_iter)) {
        goto_exit_with_failure(NULL);
      } 
    )
  }  
  
  z2020_11_06_16_40(e_dirs, "FindFirstFileW")
  if(e_dirs.call_count > 0) e_dirs.call_count--; // Ignore the FindFirstFileW call on the root directory
  z2020_11_06_16_40(e_copy, "CopyFileExW")
  z2020_11_06_16_40(e_comp, "xCompareFilesAreIndentical")
  
  RateSmoother_Add(&ctx->rs_1, secs, tfss_accum.SourceBytes.QuadPart);
  RateSmoother_Add(&ctx->rs_2, secs, tfss_accum.cbCopyProgress_Transferred.QuadPart + tfss_accum.cbCopyProgress_TransferredCurr.QuadPart);
  RateSmoother_Add(&ctx->rs_3, secs, tfss_accum.CompareReadBytes.QuadPart);
  
  RateSmoother_GetValue(&ctx->rs_1, &rate_1, &rate_undefined_1);
  RateSmoother_GetValue(&ctx->rs_2, &rate_2, &rate_undefined_2);
  RateSmoother_GetValue(&ctx->rs_3, &rate_3, &rate_undefined_3);
  
  if((call_count - 1) % 20 == 0)
  printf("%6s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s"  "\n"
    , "Secs"
    , width1
    , "Scanned"
    , width3 + 2
    , "ScanRate"    
    , width1
    , "CopySuccFull"
    , width1
    , "CopySuccChnk"    
    , width2
    , "Dirs"
    , width2
    , "Files"
    , width2
    , "Copied"
    , width3 + 2
    , "CopyRate"
    , width2
    , "Compared"
    , width1
    , "CompareReadByte"    
    , width3 + 2
    , "CompReadRate"
    );
    
  
  printf("%6d %*s %*s/s %*s %*s %*lld %*lld %*lld %*s/s %*lld %*s %*s/s"  "\n"
    , (int) floor(secs + 0.5)
    , width1
    , format_bytes((double) tfss_accum.SourceBytes.QuadPart, 5, buff1)
    , width3
    , rate_undefined_1 ? "NA" : format_bytes(rate_1, 1, buff8)       
    , width1
    , format_bytes((double) tfss_accum.CopyBytes.QuadPart, 5, buff2)     
    , width1
    , format_bytes((double) (tfss_accum.cbCopyProgress_Transferred.QuadPart + tfss_accum.cbCopyProgress_TransferredCurr.QuadPart), 5, buff4)           
    , width2
    , e_dirs.call_count
    , width2
    , tfss_accum.Files.QuadPart
    , width2
    , e_copy.call_count - e_copy.fail_count
    , width3
    , rate_undefined_2 ? "NA" : format_bytes(rate_2, 1, buff6)    
    , width2
    , (ctx->use_modified_times_otherwise_compare_files) ? -1 : e_comp.call_count
    , width1
    , (ctx->use_modified_times_otherwise_compare_files) ? "NA" : format_bytes((double) tfss_accum.CompareReadBytes.QuadPart, 5, buff3)     
    , width3
    , (rate_undefined_3 || ctx->use_modified_times_otherwise_compare_files) ? "NA" : format_bytes(rate_3, 1, buff7)    
    );
  TotalFileSystemStats_Free(&tfss_accum);
  return 0;  
exit_with_failure:;
  TotalFileSystemStats_Free(&tfss_accum);
  return 1; 
}

int SyncSrc2Dst_Print(SyncSrc2Dst o, FILE *f, double *sum_perc_denominator) {
  struct WinAPICallWrapperStats_s win_api_stats;
  WinAPICallWrapperStats_Init(&win_api_stats);
  WinAPICallWrapperStats_Add(&win_api_stats, &o->win_api_stats);
  WinAPICallWrapperStats_Add(&win_api_stats, &o->cdr.win_api_stats);  
  WinAPICallWrapperStats_Print(&win_api_stats, f, sum_perc_denominator);
  WinAPICallWrapperStats_Free(&win_api_stats);
  return 0;  
}

int SyncSrc2Dst_Run(SyncSrc2Dst o, wchar_t * src_dir, wchar_t * dst_dir, int skip_copy, ParallelFileComparer pfc) {
  struct RecursiveScan2_s src_s = {0}, * const src = &src_s;
  struct RecursiveScan2_s dst_s = {0}, * const dst = &dst_s;
  struct RecursiveMirrorScan2_s rms_s = {0}, * const rms = &rms_s;   
  struct HeartbeatThread_s hb_s = {0}, * const hb = &hb_s;  
  int is_alive;
  
  if(SyncSrc2Dst_Reset(o)) {
    goto_exit_with_failure(NULL);    
  }
  
  o->src_ref = src;
  o->dst_ref = dst;
  o->dst_ref = dst;
  o->pfc = pfc;
  if(pfc) {
    pfc->non_identical_auto_copy_mode = skip_copy ? 0 : 1;
    if(ParallelFileComparer_ResetStats(pfc)) {
      goto_exit_with_failure(NULL);    
    }
  }
  o->skip_copy = skip_copy;
  QueryPerformanceCounter_Beg(&o->qpfc_beg);
  
  if(HeartbeatThread_Init(hb)) {
    goto_exit_with_failure(NULL);    
  }    
  
  hb->cbCallback = SyncSrc2Dst_cbHeartbeatThread_Callback;
  hb->vCallbackCtx = o;
  hb->callbackIntervalMilliseconds = 1000;  
  if(HeartbeatThread_Run(hb)) {
    goto_exit_with_failure(NULL);    
  }  
  
  o->cdr.skip_copy = skip_copy;
  
  if(RecursiveScan2_Init(src, src_dir)) {
    goto_exit_with_failure(NULL);
  }  
  if(RecursiveScan2_Init(dst, dst_dir)) {
    goto_exit_with_failure(NULL);
  }     
  if(RecursiveMirrorScan2_Init(rms)) {
    goto_exit_with_failure(NULL);
  }  
  rms->cbCtx = o;
  rms->cbPossiblyMissingDir = SyncSrc2Dst_cbRecursiveMirrorScan2_PossiblyMissingDir;
  rms->cbCommonFile = SyncSrc2Dst_cbRecursiveMirrorScan2_CommonFile;
  rms->cbMismatchedFileNameAndDirName = SyncSrc2Dst_cbRecursiveMirrorScan2_MismatchedFileNameAndDirName;
  rms->cbPossiblyMissingFile = SyncSrc2Dst_cbRecursiveMirrorScan2_PossiblyMissingFile;
  if(RecursiveMirrorScan2_Recurse(rms, src, PathStorage_strlen(&src->path), dst, PathStorage_strlen(&dst->path))) {
    goto_exit_with_failure(NULL);
  }  
  // We're going to lose the reference to src ... copy its stats across
  WinAPICallWrapperStats_Add(&o->win_api_stats, &src->win_api_stats);
  //WinAPICallWrapperStats_Add(&o->win_api_stats, &dst->win_api_stats);
  o->src_ref = NULL;
  o->dst_ref = NULL;
  
  // We're going to lose the reference to pfc ... copy its stats across
  if(o->pfc) {
    ParallelFileComparer_IterateStats(o->pfc,
      if(TotalFileSystemStats_Add(&o->tfss, tfss_iter)) {
        goto_exit_with_failure(NULL);
      } 
      if(WinAPICallWrapperStats_Add(&o->win_api_stats, win_api_stats_iter)) {
        goto_exit_with_failure(NULL);
      }
    )  
  }
  o->pfc = NULL;  
  
  HeartbeatThread_Stop(hb); 
  HeartbeatThread_Join(hb, -1, &is_alive);
  SyncSrc2Dst_cbHeartbeatThread_Callback(o, hb, -1);
  RecursiveMirrorScan2_Free(rms);
  RecursiveScan2_Free(dst);  
  RecursiveScan2_Free(src);  
  HeartbeatThread_Free(hb);
  return 0;
exit_with_failure:;
  HeartbeatThread_Stop(hb); 
  HeartbeatThread_Join(hb, -1, &is_alive);
  SyncSrc2Dst_cbHeartbeatThread_Callback(o, hb, -1);
  RecursiveMirrorScan2_Free(rms);
  RecursiveScan2_Free(dst);
  RecursiveScan2_Free(src);  
  HeartbeatThread_Free(hb);
  return 1;    
}

#define SemaphoreDriver_WorkerIndices_Del(driver, worker, wait_or_work) \
{ \
  int *wwi_p, wwi; \
  wwi = (worker)->wait_or_work##_workers_indices_index; \
  wwi_p = &(driver)->wait_or_work##_workers_indices[wwi]; \
  /*printf("%5d:Del:" #wait_or_work ":%d\n", __LINE__, *wwi_p);*/ \
  FOLDER_MIRROR_DEBUG_WRAPPER( \
    if(*wwi_p != (worker)->index_Zb) { \
      printf("%d %d %d\n", wwi, *wwi_p, (worker)->index_Zb); \
      THROWX("SemaphoreDriver_WorkerIndices__Del:" #wait_or_work ":*wwi_p != (worker)->index_Zb", 1) \
    } \
    if(wwi < 0) { \
      THROWX("SemaphoreDriver_WorkerIndices__Del:" #wait_or_work ":wwi < 0", 1) \
    }   \
    if(wwi >= (driver)->wait_or_work##_workers_count) { \
      THROWX("SemaphoreDriver_WorkerIndices__Del:"#wait_or_work ":wwi >= (driver)->xxx_workers_count", 1) \
    } \
  ) \
  (worker)->wait_or_work##_workers_indices_index = -1; \
  (driver)->wait_or_work##_workers_count--; \
  if(wwi != (driver)->wait_or_work##_workers_count) { \
    *wwi_p = (driver)->wait_or_work##_workers_indices[(driver)->wait_or_work##_workers_count]; \
    (driver)->workers[*wwi_p].wait_or_work##_workers_indices_index = wwi; \
  } \
}

#define SemaphoreDriver_WorkerIndices_Add(driver, worker, wait_or_work) \
{ \
  /*printf("%5d:Add:" #wait_or_work ":%d\n", __LINE__, (worker)->index_Zb);*/ \
  FOLDER_MIRROR_DEBUG_WRAPPER( \
    if((worker)->wait_or_work##_workers_indices_index != -1) { \
      THROWX("SemaphoreDriver_WorkerIndices__Add:" #wait_or_work ":(worker)->xxx_workers_indices_index != -1", 1) \
    }   \
    if((driver)->wait_or_work##_workers_count >= (driver)->workers_count) { \
      THROWX("SemaphoreDriver_WorkerIndices__Add:" #wait_or_work ":(driver)->xxx_workers_count >= (driver)->workers_count", 1) \
    } \
  ) \
  (worker)->wait_or_work##_workers_indices_index = (driver)->wait_or_work##_workers_count; \
  (driver)->wait_or_work##_workers_indices[(driver)->wait_or_work##_workers_count] = (worker)->index_Zb; \
  (driver)->wait_or_work##_workers_count++; \
}

void SemaphoredWorkerThread_Free(SemaphoredWorkerThread o) {
  if(o->hSignalWorkerEvent) CloseHandle(o->hSignalWorkerEvent);
  if(o->hThread) CloseHandle(o->hThread);
  if(o->bAutoFreeWorkCtxOnCompletion && o->workCtx) {
    free(o->workCtx);
  }  
  memset(o, 0, sizeof(*o));
}

int SemaphoredWorkerThread_Init(SemaphoredWorkerThread o, struct SemaphoreDriver_s * driver) {
  memset(o, 0, sizeof(*o));
  o->driver = driver;
  o->index_Zb = -1;
  o->waiting_workers_indices_index = -1;
  o->working_workers_indices_index = -1;
  o->hSignalWorkerEvent = CreateEventA(NULL /*lpEventAttributes*/, 0 /* bManualReset */, 0 /* bInitialState */, NULL /* lpName */);
  if(!o->hSignalWorkerEvent) {
    goto_exit_with_failure(NULL);
  }    
  return 0;
exit_with_failure:;
  SemaphoredWorkerThread_Free(o);
  return 1;    
}

DWORD WINAPI SemaphoredWorkerThread_ThreadFunction(LPVOID lpParam) {
  SemaphoredWorkerThread ctx = lpParam;
  unsigned int call_count = 0;
  int ret, work_available = 0, run_work = 0;
  ctx->continue_looping = 1;
  while(ctx->continue_looping) {
    /* Wait for work to become available */
    ret = WaitForSingleObject(ctx->hSignalWorkerEvent, (ctx->driver->sleepIntervalMilliseconds < 0 ? 0 : ctx->driver->sleepIntervalMilliseconds));
    switch(ret) {
    case(WAIT_ABANDONED): work_available = 0; ctx->continue_looping = 0; break;
    case(WAIT_OBJECT_0):  work_available = 1; break;
    case(WAIT_TIMEOUT):   work_available = 0; break;
    case(WAIT_FAILED):    work_available = 0; ctx->continue_looping = 0; break;
    default:              work_available = 0; ctx->continue_looping = 0; break;
    }    
    if(work_available) {
      /* We've got some work */
      while(ctx->continue_looping) {
        /* Wait for a semaphore slot to become available */
        ret = WaitForSingleObject(ctx->driver->hSemaphore, (ctx->driver->sleepIntervalMilliseconds < 0 ? 0 : ctx->driver->sleepIntervalMilliseconds));
        switch(ret) {
        case(WAIT_ABANDONED): run_work = 0; ctx->continue_looping = 0; break;
        case(WAIT_OBJECT_0):  run_work = 1; break;
        case(WAIT_TIMEOUT):   run_work = 0; break;
        case(WAIT_FAILED):    run_work = 0; ctx->continue_looping = 0; break;
        default:              run_work = 0; ctx->continue_looping = 0; break;
        }        
        if(run_work) {     
          /* We can run our work */
          call_count += 1;
          if(ctx->driver->cbRunWork) {
            /* Call the user to do the work */
            ctx->driver->cbRunWork(ctx->driver->cbCtx, ctx->driver, ctx, ctx->workCtx, ctx->index_Zb, call_count);
          }
          if(ctx->bAutoFreeWorkCtxOnCompletion && ctx->workCtx) {
            /* The pointer assigned to workCtx needs to be freed */
            free(ctx->workCtx);
            ctx->bAutoFreeWorkCtxOnCompletion = 0;
          }
          ctx->workCtx = NULL;        
          
          /* Signal to driver that we're waiting again */
          EnterCriticalSection(&ctx->driver->state_lock);
          SemaphoreDriver_WorkerIndices_Add(ctx->driver, ctx, waiting);
          SemaphoreDriver_WorkerIndices_Del(ctx->driver, ctx, working);
          ret = SetEvent(ctx->driver->hAvailableWorkerEvent);
          if(!ret) {
            ret = GetLastError();
            ctx->continue_looping = 0;
          } else {
            ret = 0;
          }
          LeaveCriticalSection(&ctx->driver->state_lock); 
          
          /* Release our semaphore slot */
          if(ret) {
            //
          } else {
            ret = ReleaseSemaphore(ctx->driver->hSemaphore, 1, NULL);
            if(!ret) {
              ret = GetLastError();
              ctx->continue_looping = 0;
            } else {
              ret = 0;
            }             
          }
          
          /* Go back to the outer loop to wait for new work */
          break;            
        }    
      }
    }
  }
  if(ctx->hThread) {
    HANDLE tmp = ctx->hThread;
    ctx->hThread = NULL;
    CloseHandle(tmp);
  }
  return 0;
}

int SemaphoredWorkerThread_Run(SemaphoredWorkerThread o) {
  int ret = ResetEvent(o->hSignalWorkerEvent);
  if(!ret) {
    ret = GetLastError();
  } else {
    ret = 0;
    o->hThread = CreateThread( 
              NULL,               // default security attributes
              0,                  // use default stack size  
              SemaphoredWorkerThread_ThreadFunction,       // thread function name
              o,                  // argument to thread function 
              0,                  // use default creation flags 
              &o->dwThreadId);    // returns the thread identifier 
    if(!o->hThread) {
      ret = GetLastError();
    }
  }
  return ret;
}

int SemaphoredWorkerThread_Signal(SemaphoredWorkerThread o) {
  int ret;
  if(!o->hSignalWorkerEvent) {
    ret = 0;
  } else {
    ret = SetEvent(o->hSignalWorkerEvent);
    if (!ret) {
      ret = GetLastError();
    } else {
      ret = 0;
    }
  }
  return ret;
}

int SemaphoredWorkerThread_Join(SemaphoredWorkerThread o, int dwMilliseconds, int * is_alive) {
  int ret;
  if(!o->hThread) {
    ret = 0;
    *is_alive = 0;    
  } else {
    ret = WaitForSingleObject(o->hThread, (dwMilliseconds < 0 ? INFINITE : dwMilliseconds));
    switch(ret) {
    case(WAIT_ABANDONED): ret = 1; *is_alive = 0; break;
    case(WAIT_OBJECT_0):  ret = 0; *is_alive = 0; break;
    case(WAIT_TIMEOUT):   ret = 0; *is_alive = 1; break;
    case(WAIT_FAILED):    ret = 1; *is_alive = 0; break;
    default:              ret = 1; *is_alive = 0; break;
    }
    if(ret) {
      ret = GetLastError();
      if(ret == ERROR_INVALID_HANDLE && !o->hThread) {
        ret = 0;
        *is_alive = 0;
      }
    }
  }
  return ret;  
}

void SemaphoreDriver_Free(SemaphoreDriver o) {
  int i;
  DeleteCriticalSection(&o->state_lock);
  if(o->hAvailableWorkerEvent) CloseHandle(o->hAvailableWorkerEvent);
  if(o->hSemaphore) CloseHandle(o->hSemaphore);
  if(o->waiting_workers_indices) free(o->waiting_workers_indices);
  if(o->working_workers_indices) free(o->working_workers_indices);
  if(o->workers) {
    for(i = 0; i < o->workers_count; i++) {
      SemaphoredWorkerThread_Free(&o->workers[i]);
    }    
    free(o->workers);
  }
  memset(o, 0, sizeof(*o));
}

int SemaphoreDriver_Init(SemaphoreDriver o, int semaphore_maximum_count) {
  int zero_count, i;
  memset(o, 0, sizeof(*o));
  // https://docs.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-createsemaphorea
  if(!(o->hSemaphore = CreateSemaphoreA(NULL, semaphore_maximum_count, semaphore_maximum_count, NULL))) {
    goto_exit_with_failure(NULL);
  }
  o->hAvailableWorkerEvent = CreateEventA(NULL /*lpEventAttributes*/, 0 /* bManualReset */, 0 /* bInitialState */, NULL /* lpName */);
  if(!o->hAvailableWorkerEvent) {
    goto_exit_with_failure(NULL);
  }   
  InitializeCriticalSection(&o->state_lock);
  zero_count = 0; GenericGrowMacro1(struct SemaphoredWorkerThread_s , &o->workers                 , &zero_count, semaphore_maximum_count, semaphore_maximum_count);
  zero_count = 0; GenericGrowMacro1(int                             , &o->waiting_workers_indices , &zero_count, semaphore_maximum_count, semaphore_maximum_count);
  zero_count = 0; GenericGrowMacro1(int                             , &o->working_workers_indices , &zero_count, semaphore_maximum_count, semaphore_maximum_count);
  
  /* All workers are available, initially */
  o->waiting_workers_count = o->workers_count = semaphore_maximum_count;  
  memset(o->workers, 0, sizeof(*o->workers) * o->workers_count);
  for(i = 0; i < o->workers_count; i++) {
    if(SemaphoredWorkerThread_Init(&o->workers[i], o)) {
      goto_exit_with_failure(NULL);
    }
    o->waiting_workers_indices[i] = o->workers[i].index_Zb = i;
    o->workers[i].waiting_workers_indices_index = i;
  }     
  
  return 0;
exit_with_failure:;
  SemaphoreDriver_Free(o);
  return 1;    
}

int SemaphoreDriver_Run(SemaphoreDriver o) 
{
  int i;
  for(i = 0; i < o->workers_count; i++) {
    if(SemaphoredWorkerThread_Run(&o->workers[i])) {
      goto_exit_with_failure(NULL);
    }
  }    
  return 0;
exit_with_failure:;
  return 1;  
}

int SemaphoreDriver_Stop(SemaphoreDriver o) {
  int i;
  for(i = 0; i < o->workers_count; i++) {
    o->workers[i].continue_looping = 0;
    if(SemaphoredWorkerThread_Signal(&o->workers[i])) {
      goto_exit_with_failure(NULL);
    }        
  }
  return 0;
exit_with_failure:;
  return 1;    
}

int SemaphoreDriver_Join(SemaphoreDriver o, int dwMilliseconds, int * is_alive) 
{
  int i, is_alive_tmp;
  LARGE_INTEGER beg;
  double secs;
  DWORD dwMilliseconds_;
  *is_alive = 0;
  if(dwMilliseconds >= 0) {
    QueryPerformanceCounter_Beg(&beg);
  } else {
    dwMilliseconds_ = INFINITE;
  }
  for(i = 0; i < o->workers_count; i++) {  
    if(dwMilliseconds >= 0) {
      QueryPerformanceCounter_End(beg, &secs, NULL, NULL, NULL); 
      dwMilliseconds_ = max(0, dwMilliseconds - secs / 1000.0);
    }
    if(SemaphoredWorkerThread_Join(&o->workers[i], dwMilliseconds_, &is_alive_tmp)) {
      goto_exit_with_failure(NULL);
    }        
    if(is_alive_tmp) *is_alive = 1;
  }
  return 0;
exit_with_failure:;
  return 1;    
}

int SemaphoreDriver_SubmitWork(SemaphoreDriver o, void *workCtx, int bAutoFreeWorkCtxOnCompletion) {
  int ret, getlasterror = 0, waiting_for_worker, continue_looping;
  
  #define z2020_11_09_15_10(ret__p, getlasterror_p) \
  { \
    SemaphoredWorkerThread worker = &o->workers[o->waiting_workers_indices[0]]; \
    SemaphoreDriver_WorkerIndices_Del(o, worker, waiting); \
    SemaphoreDriver_WorkerIndices_Add(o, worker, working); \
    *(ret__p) = 0; \
    *(getlasterror_p) = 0; \
    worker->workCtx = workCtx; \
    worker->bAutoFreeWorkCtxOnCompletion = bAutoFreeWorkCtxOnCompletion; \
    if(o->cbObserveWorkAllocation) { \
      /* Call the user while we're protected by o->state_lock ... they can user worker->index_Zb to organise storage of the data pointed to by workCtx */ \
      *(ret__p) = o->cbObserveWorkAllocation(o->cbCtx, o, worker, workCtx, worker->index_Zb, &worker->workCtx, &worker->bAutoFreeWorkCtxOnCompletion); \
    } \
    if(!*(ret__p)) { \
      *(getlasterror_p) = SetEvent(worker->hSignalWorkerEvent); \
      if (!*(getlasterror_p)) { \
        *(getlasterror_p) = GetLastError(); \
      } else { \
        *(getlasterror_p) = 0; \
      } \
    } \
  }
  
  /* We trigger a worker if there are any available to work */
  EnterCriticalSection(&o->state_lock);
  if(o->waiting_workers_count > 0) {
    waiting_for_worker = 0;
    z2020_11_09_15_10(&ret, &getlasterror)
  } else {
    /* There's no available workers ... reset the event hAvailableWorkerEvent .. we'll wait for it below */
    waiting_for_worker = 1;
    ret = ResetEvent(o->hAvailableWorkerEvent);
    if(!ret) {
      ret = GetLastError();
    } else {
      ret = 0;
    }
  }
  LeaveCriticalSection(&o->state_lock);
  if(ret || getlasterror) {
    goto_exit_with_failure(getlasterror != 0 ? &getlasterror : NULL);
  }
  
  if(waiting_for_worker) {
    /* We need to wait for workers to become available to work */
    continue_looping = 1;
    while(continue_looping) {
      ret = WaitForSingleObject(o->hAvailableWorkerEvent, (o->sleepIntervalMilliseconds < 0 ? 0 : o->sleepIntervalMilliseconds));
      switch(ret) {
      case(WAIT_ABANDONED): continue_looping = 0; break;
      case(WAIT_OBJECT_0):  continue_looping = 0; ret = 0; break;
      case(WAIT_TIMEOUT):   continue_looping = 1; break;
      case(WAIT_FAILED):    continue_looping = 0; break;
      default:              continue_looping = 0; break;
      }     
    }
    if(ret) {
      goto_exit_with_failure(NULL);
    }    
    /* Trigger a worker to do the work in workCtx */
    EnterCriticalSection(&o->state_lock);
    if(o->waiting_workers_count > 0) {
      z2020_11_09_15_10(&ret, &getlasterror)
    } else {
      ret = -1; /* ERROR: We should have at least one available worker */
    }
    LeaveCriticalSection(&o->state_lock);
    if(ret || getlasterror) {
      goto_exit_with_failure(getlasterror != 0 ? &getlasterror : NULL);
    }
  }

  return 0;
exit_with_failure:;
  return 1;      
}

void ParallelFileComparisonWorkspace_Free(ParallelFileComparisonWorkspace o) {
  PathStorage_Free(&o->path1);
  PathStorage_Free(&o->path2);
  if(o->comp_buff1) free(o->comp_buff1);
  if(o->comp_buff2) free(o->comp_buff2);
  WinAPICallWrapperStats_Free(&o->win_api_stats);
  TotalFileSystemStats_Free(&o->tfss);
  memset(o, 0, sizeof(*o));
}

int ParallelFileComparisonWorkspace_Init(ParallelFileComparisonWorkspace o) {
  memset(o, 0, sizeof(*o));
  if(PathStorage_Init(&o->path1)) {
    goto_exit_with_failure(NULL);
  }
  if(PathStorage_Init(&o->path2)) {
    goto_exit_with_failure(NULL);
  }  
  if(WinAPICallWrapperStats_Init(&o->win_api_stats)) {
    goto_exit_with_failure(NULL);    
  }
  if(TotalFileSystemStats_Init(&o->tfss)) {
    goto_exit_with_failure(NULL);    
  }    
  return 0;
exit_with_failure:;
  ParallelFileComparisonWorkspace_Free(o);
  return 1;    
}

int ParallelFileComparisonWorkspace_ResetStats(ParallelFileComparisonWorkspace o) {
  if(WinAPICallWrapperStats_Reset(&o->win_api_stats)) {
    goto_exit_with_failure(NULL);    
  }
  if(TotalFileSystemStats_Reset(&o->tfss)) {
    goto_exit_with_failure(NULL);    
  }    
  return 0;
exit_with_failure:;
  return 1;    
}

int ParallelFileComparer_cbSemaphoredWorkerThread_RunWork(void * cbCtx, struct SemaphoreDriver_s * driver, struct SemaphoredWorkerThread_s * o, void *workCtx, unsigned int index_Zb, unsigned int call_count) {
  ParallelFileComparer ctx = cbCtx;
  ParallelFileComparisonWorkspace ws = &ctx->workspaces[index_Zb];
  int files_are_identical, ret;
  size_t src_size = ctx->non_identical_auto_copy_mode <= 1 ? ws->size1 : ws->size2;
#ifdef _DEBUG
  if(ctx->non_identical_auto_copy_mode < 0 || ctx->non_identical_auto_copy_mode > 2) {
    THROWX("ctx->non_identical_auto_copy_mode < 0 || ctx->non_identical_auto_copy_mode > 2", 1)
  }
#endif         
  //ws->tfss.SourceBytes.QuadPart += src_size;
  //ws->tfss.Files.QuadPart++;    
  WinAPICallWrapperMacro(xCompareFilesAreIndentical, (ws->path1.wide, ws->path2.wide, &ws->comp_buff1, &ws->comp_buff2, &ws->comp_buff1_max_size, &ws->comp_buff2_max_size, &files_are_identical, &ws->tfss.CompareReadBytes.QuadPart), &ret, &ws->win_api_stats, NULL, 
    goto_exit_with_failure(NULL);  
  )
  if(!files_are_identical) {
    if(!ctx->non_identical_auto_copy_mode) {
      ws->tfss.CopyBytes.QuadPart += src_size;
    } else {
      wchar_t * src_file = ctx->non_identical_auto_copy_mode == 1 ? ws->path1.wide : ws->path2.wide;
      wchar_t * dst_file = ctx->non_identical_auto_copy_mode == 1 ? ws->path2.wide : ws->path1.wide;
      TotalFileSystemStats_cbCopyProgress_Bef(&ws->tfss);
      if(WinAPICallWrapperStats_CopyFile(&ws->win_api_stats, TotalFileSystemStats_cbCopyProgress, &ws->tfss, src_file, dst_file, src_size, &ws->tfss.CopyBytes.QuadPart)) {
        TotalFileSystemStats_cbCopyProgress_Aft(&ws->tfss);
        goto_exit_with_failure(NULL);
      }
      TotalFileSystemStats_cbCopyProgress_Aft(&ws->tfss);
    }
  }
  return 0;
exit_with_failure:;  
  return 0;
}

int ParallelFileComparer_cbSemaphoredWorkerThread_ObserveWorkAllocation(void * cbCtx, struct SemaphoreDriver_s * driver, struct SemaphoredWorkerThread_s * o, void *initial_workCtx, unsigned int index_Zb, void **actual_workCtx, int *bAutoFreeWorkCtxOnCompletion) {
  size_t strlen_path, strlen_s;
  ParallelFileComparer ctx = cbCtx;
  ParallelFileComparerJob job = initial_workCtx;
  ParallelFileComparisonWorkspace ws = &ctx->workspaces[index_Zb];
  
  
  #define z2020_11_09_21_30(_1_or_2) \
  { \
    ws->size##_1_or_2 = job->size##_1_or_2; \
    strlen_path = 0; \
    if(PathStorage_SafeAppendStringToPath(&ws->path##_1_or_2, strlen_path, job->path##_1_or_2, &strlen_s)) { \
      goto_exit_with_failure(NULL); \
    } \
    strlen_path += strlen_s; \
    if(job->name##_1_or_2) { \
      if(PathStorage_SafeAppendPathSep(&ws->path##_1_or_2, &strlen_path)) { \
        goto_exit_with_failure(NULL); \
      } \
      if(PathStorage_SafeAppendStringToPath(&ws->path##_1_or_2, strlen_path, job->name##_1_or_2, &strlen_s)) { \
        goto_exit_with_failure(NULL); \
      }   \
      strlen_path += strlen_s; \
    } \
  }
  
  z2020_11_09_21_30(1)
  z2020_11_09_21_30(2)
  
  return 0;
exit_with_failure:;
  return 1;  
}

int ParallelFileComparer_QueueJob(ParallelFileComparer o, ParallelFileComparerJob job) {
  if(SemaphoreDriver_SubmitWork(&o->smd, job, 0)) {
    goto_exit_with_failure(NULL);
  }
  return 0;
exit_with_failure:;
  return 1;    
}

void ParallelFileComparer_Free(ParallelFileComparer o) {
  int is_alive, i;
  if(SemaphoreDriver_Stop(&o->smd)) {
    //goto_exit_with_failure(NULL);
  }  
  if(SemaphoreDriver_Join(&o->smd, -1, &is_alive)) {
    //goto_exit_with_failure(NULL);
  }
  SemaphoreDriver_Free(&o->smd);  
  for(i = 0; i < o->max_parallel_comparisons; i++) {
    ParallelFileComparisonWorkspace_Free(&o->workspaces[i]);
  }  
  if(o->workspaces) free(o->workspaces);
  memset(o, 0, sizeof(*o));
}

int ParallelFileComparer_Init(ParallelFileComparer o, int max_parallel_comparisons) {
  int zero_count, i;
  memset(o, 0, sizeof(*o));
  o->max_parallel_comparisons = max_parallel_comparisons;
  zero_count = 0; GenericGrowMacro1(struct ParallelFileComparisonWorkspace_s, &o->workspaces, &zero_count, max_parallel_comparisons, max_parallel_comparisons);
  memset(o->workspaces, 0, sizeof(*o->workspaces) * max_parallel_comparisons);
  for(i = 0; i < o->max_parallel_comparisons; i++) {
    if(ParallelFileComparisonWorkspace_Init(&o->workspaces[i])) {
        goto_exit_with_failure(NULL);
    }
  }
  if(SemaphoreDriver_Init(&o->smd, max_parallel_comparisons)) {
    goto_exit_with_failure(NULL);
  }         
  o->smd.cbCtx = o;
  o->smd.cbRunWork = ParallelFileComparer_cbSemaphoredWorkerThread_RunWork;
  o->smd.cbObserveWorkAllocation = ParallelFileComparer_cbSemaphoredWorkerThread_ObserveWorkAllocation;    
  if(SemaphoreDriver_Run(&o->smd)) {
    goto_exit_with_failure(NULL);
  }      
  return 0;
exit_with_failure:;
  ParallelFileComparer_Free(o);
  return 1;    
}

int ParallelFileComparer_ResetStats(ParallelFileComparer o) {
  int i;
  for(i = 0; i < o->max_parallel_comparisons; i++) {
    if(ParallelFileComparisonWorkspace_ResetStats(&o->workspaces[i])) {
      goto_exit_with_failure(NULL);
    }
  }  
  return 0;
exit_with_failure:;
  return 1;   
}


int SemaphoreDriver_Test_VisitFile(void *cbCtx, RecursiveScan2 src, RecursiveScan2RecursionLevel rl, PathStorage path, size_t strlen_path, wchar_t *name, __int64 size, __int64 modt) {
  ParallelFileComparer ctx = cbCtx;
  struct ParallelFileComparerJob_s job = {0};
  ctx->total_queued_jobs++;
  if(ctx->total_queued_jobs % 50 == 0) printf("%6d %ls%ls\n", ctx->total_queued_jobs, path->wide, name);
  job.path1 = path->wide;
  job.name1 = name;
  if(ParallelFileComparer_QueueJob(ctx, &job)) {
    THROWX("ParallelFileComparer_QueueJob(ctx, &job)", 1)
  }
  return ctx->total_queued_jobs > 50000 ? 1 : 0;
}

int wmain(int argc, wchar_t**argv) { // test_main
  struct ParallelFileComparer_s pfc_s, * const pfc = &pfc_s;
  struct RecursiveScan2_s src_s, * const src = &src_s;
  struct RecursiveScan2_s dst_s, * const dst = &dst_s;
  struct RecursiveScan2_s rss_s, * const rss = &rss_s;
  struct RecursiveMirrorScan2_s rms_s, * const rms = &rms_s; 
  struct CopyDirRecursion_s cdr_s, * const cdr = &cdr_s; 
  struct SyncSrc2Dst_s ss2d_s, * const ss2d = &ss2d_s; 
  int is_alive, loop;
  LARGE_INTEGER tmp;
  double secs;  
  int skip_copy, i, folder_already_exists;
  
  #define PYTHON_EXE "C:\\analytics\\projects\\git\\junk\\myenv37pyspark\\Scripts\\python.exe"
  #define PYTHON_TOOL "random_dir_tree_pair.py"  

  wchar_t *l_dir = L"C:\\analytics\\projects\\c\\root_l";
  wchar_t *r_dir = L"C:\\analytics\\projects\\c\\root_r";
  
  if(RecursiveMirrorScan2_Init(rms)) {
    goto_exit_with_failure(NULL);
  }
  if(RecursiveScan2_Init(src, l_dir)) {
    goto_exit_with_failure(NULL);
  }  
  if(RecursiveScan2_Init(dst, r_dir)) {
    goto_exit_with_failure(NULL);
  }    
  if(CopyDirRecursion_Init(cdr)) {
    goto_exit_with_failure(NULL);
  }
  if(SyncSrc2Dst_Init(ss2d)) {
    goto_exit_with_failure(NULL);
  }
  
  if(0) {
    if(RecursiveScan2_Init(rss, L"C:\\analytics")) {
      goto_exit_with_failure(NULL);
    }     
    
    if(ParallelFileComparer_Init(pfc, 1)) {
      goto_exit_with_failure(NULL);
    }         
   
    rss->cbCtx = pfc;
    rss->cbVisitFile = SemaphoreDriver_Test_VisitFile;
    
    if(RecursiveScan2_Recurse(rss, PathStorage_strlen(&rss->path))) {
      goto_exit_with_failure(NULL);
    }
    
    ParallelFileComparer_Free(pfc);
    RecursiveScan2_Free(rss);
  }  
  
  if(argc > 1) {
    #define z2020_09_11_11_57(_ff_) \
    _ff_(L"/?") \
    _ff_(L"-h") \
    _ff_(L"--help") \
    
    #define z2020_09_11_11_56_exp1(flag, help, handler) "  -%-11ls %s\n"
    #define z2020_09_11_11_56_exp2(flag, help, handler) , flag, help
    #define z2020_09_11_11_56_exp3(flag, help, handler_code) else if(wcsicmp(argv[i] + 1, flag) == 0) { handler_code; }
    #define z2020_09_11_11_56(_ff_) \
    _ff_(L"m", "Use file size and modified times to test for file changes; otherwise, compare file contents.", ss2d->use_modified_times_otherwise_compare_files = 1;)
    
    char * help = 
    "Backup directory incrementally. Check for changes using either file size and modified times or using file comparison (default).\n"
    "\n"
    "backup <source-dir> <backup-dir> [-m]\n"
    "\n"
    "  <source-dir> Specifies the folder containing the source.\n"
    "  <backup-dir> Specifies the folder storing the backup.\n"
    z2020_09_11_11_56(z2020_09_11_11_56_exp1)
    ;
    l_dir = NULL;
    r_dir = NULL;
    for(i = 1; i < argc; i++) {
      #define z2020_09_11_11_57_exp(hh) || (wcsicmp(argv[i], hh) == 0)
      if(0 z2020_09_11_11_57(z2020_09_11_11_57_exp)) {
        goto print_help;  
      } else if(*argv[i] == '-') {
        if(0) {} 
        z2020_09_11_11_56(z2020_09_11_11_56_exp3) 
        else {
          printf("ERROR:Unrecognized option '%ls'. See help below\n", argv[i]);
          goto print_help;           
        }
      } else if(!l_dir) {
        l_dir = argv[i];
      } else if(!r_dir) {
        r_dir = argv[i];
      } else {
        printf("ERROR:More than one argument for <backup-dir>. See help below\n");
        goto print_help; 
      }
    }    
    if(!l_dir) {
      printf("ERROR:Missing argument <source-dir>. See help below\n");
      goto print_help;   
    }
    if(!r_dir) {
      printf("ERROR:Missing argument <backup-dir>. See help below\n");
      goto print_help;   
    }    
    if(0) {
      QueryPerformanceCounter_Beg(&tmp);
      if(SyncSrc2Dst_Run(ss2d, l_dir, r_dir, skip_copy, NULL)) {
        SyncSrc2Dst_Print(ss2d, stdout, NULL);
        goto_exit_with_failure(NULL);
      }
      QueryPerformanceCounter_End(tmp, &secs, NULL, NULL, NULL); 
      SyncSrc2Dst_Print(ss2d, stdout, &secs);         
    }
    goto exit_early;
print_help:;    
    printf(help z2020_09_11_11_56(z2020_09_11_11_56_exp2));
    goto exit_early;
  }
  
  if(0) {
    system("rm -r C:\\analytics\\projects\\c\\root_r\\2");
    if(CopyDirRecursion_Run(cdr, src, PathStorage_strlen(&src->path), dst, PathStorage_strlen(&dst->path), L"2", &folder_already_exists)) {
      goto_exit_with_failure(NULL);
    }
  }
  
  if(1) {
    skip_copy = 0;
    printf("beg\n");
    if(1) {
      l_dir = L"C:\\analytics";
      r_dir = L"D:\\analytics";
      //l_dir = L"C:\\analytics\\projects\\git\\xopt\\maui\\R\\swig";
      //r_dir = L"D:\\analytics\\projects\\git\\xopt\\maui\\R\\swig";
      
    } else {
      printf("gen\n");
      QueryPerformanceCounter_Beg(&tmp);
      system(PYTHON_EXE " " PYTHON_TOOL);    
      QueryPerformanceCounter_End(tmp, &secs, NULL, NULL, NULL);    
      printf("end:%f\n", secs);      
    }
    QueryPerformanceCounter_Beg(&tmp);
    ss2d->use_modified_times_otherwise_compare_files = 1;
    if(ParallelFileComparer_Init(pfc, 2)) {
      goto_exit_with_failure(NULL);
    }      
    if(SyncSrc2Dst_Run(ss2d, l_dir, r_dir, skip_copy, pfc)) {
      SyncSrc2Dst_Print(ss2d, stdout, NULL);
      goto_exit_with_failure(NULL);
    }
    ParallelFileComparer_Free(pfc);    
    QueryPerformanceCounter_End(tmp, &secs, NULL, NULL, NULL); 
    printf("end:%f\n", secs);   
    SyncSrc2Dst_Print(ss2d, stdout, &secs);   
    
  }
  
  if(0) {
    skip_copy = 0;
    printf("gen\n");
    QueryPerformanceCounter_Beg(&tmp);
    system(PYTHON_EXE " " PYTHON_TOOL);    
    QueryPerformanceCounter_End(tmp, &secs, NULL, NULL, NULL);    
    printf("end:%f\n", secs);
    printf("beg\n");
    QueryPerformanceCounter_Beg(&tmp);
    if(ParallelFileComparer_Init(pfc, 2)) {
      goto_exit_with_failure(NULL);
    }      
    if(SyncSrc2Dst_Run(ss2d, l_dir, r_dir, skip_copy, pfc)) {
      SyncSrc2Dst_Print(ss2d, stdout, NULL);
      goto_exit_with_failure(NULL);
    }
    ParallelFileComparer_Free(pfc);    
    QueryPerformanceCounter_End(tmp, &secs, NULL, NULL, NULL);   
    printf("end:%f\n", secs);    
    SyncSrc2Dst_Print(ss2d, stdout, &secs);
    if(!skip_copy) {
      printf("beg\n");
      QueryPerformanceCounter_Beg(&tmp);
      if(ParallelFileComparer_Init(pfc, 2)) {
        goto_exit_with_failure(NULL);
      }            
      if(SyncSrc2Dst_Run(ss2d, l_dir, r_dir, skip_copy, NULL)) {
        SyncSrc2Dst_Print(ss2d, stdout, NULL);
        goto_exit_with_failure(NULL);
      }
      ParallelFileComparer_Free(pfc); 
      QueryPerformanceCounter_End(tmp, &secs, NULL, NULL, NULL);    
      printf("end:%f\n", secs);    
      SyncSrc2Dst_Print(ss2d, stdout, &secs);
    }
  }
  
exit_early:;  
  
  SyncSrc2Dst_Free(ss2d);
  CopyDirRecursion_Free(cdr);
  RecursiveScan2_Free(dst);  
  RecursiveScan2_Free(src);  
  RecursiveMirrorScan2_Init(rms);
  return 0;
exit_with_failure:;
  printf("exit_with_failure");
  return 1;   
}
