#if defined(_WIN32) || defined(_WIN64)
#include "port/win32.h"
#undef PGDLLIMPORT
#undef PGDLLEXPORT
#ifdef __clang__
#undef __MINGW64__
#endif /* __clang__ */
#endif
