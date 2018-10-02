#pragma once

#include <string>
#include <vector>
#include <stdarg.h>

/**
 * Formst string with va_list.
 */
inline std::string formatStringV(const char *format, va_list ap)
{
    char *p = nullptr;
    int ret = ::vasprintf(&p, format, ap);
    if (ret < 0) throw std::runtime_error("vasprintf failed.");
    try {
        std::string s(p, ret);
        ::free(p);
        return s;
    } catch (...) {
        ::free(p);
        throw;
    }
}

/**
 * Create a std::string using printf() like formatting.
 */
inline std::string formatString(const char * format, ...)
{
    std::string s;
    std::exception_ptr ep;
    va_list args;
    va_start(args, format);
    try {
        s = formatStringV(format, args);
    } catch (...) {
        ep = std::current_exception();
    }
    va_end(args);
    if (ep) std::rethrow_exception(ep);
    return s;
}


inline std::vector<std::string> splitString(const std::string& s, const char c)
{
    std::vector<std::string> ret;
    size_t cur = 0;
    for (;;) {
        size_t pos = s.find(c, cur);
        if (pos == std::string::npos) {
            ret.push_back(s.substr(cur, s.size() - cur));
            break;
        }
        ret.push_back(s.substr(cur, pos - cur));
        cur = pos + 1;
    }
    return ret;
}
